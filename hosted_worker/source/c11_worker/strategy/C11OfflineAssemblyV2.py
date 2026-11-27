"""C11 source registration and research diagnostics. This class cannot start trading."""
from freqtrade.exceptions import OperationalException
from freqtrade.strategy.interface import IStrategy
from pandas import DataFrame


class C11OfflineAssemblyV2(IStrategy):
    """Versioned Freqtrade resolver target; NO dry-run or live release."""
    INTERFACE_VERSION = 3
    timeframe = "15m"       # C11 primary decision clock; not a trading configuration
    can_short = False
    minimal_roi = {}        # resolver scaffolding, not a policy release
    stoploss = -0.30        # research-only map for the known C11 5x / 6% price case
    position_adjustment_enable = False
    use_exit_signal = False

    def bot_start(self, **kwargs):
        raise OperationalException("C11_OFFLINE_CANDIDATE_NOT_RELEASED")

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        return dataframe.copy()

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        out = dataframe.copy()
        out["enter_long"] = out["enter_short"] = 0
        out["enter_tag"] = None
        return out

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        out = dataframe.copy()
        out["exit_long"] = out["exit_short"] = 0
        out["exit_tag"] = None
        return out

    def confirm_trade_entry(self, *args, **kwargs) -> bool:
        return False

    def adjust_trade_position(self, *args, **kwargs):
        return None

    @staticmethod
    def preview_primary(scope, *, as_of_ms, valid_until_ms, candle_symbol, candles15,
                        minutes, bound_oi, rpd_segment, completed_bars5, rpd_checkpoint=None):
        """Use verified research ports; never read the collector or set a trading field."""
        from c11_primary_decision_seam_v1.primary_seam import Decision, decide
        from c11_primary_decision_seam_v1.rpd5_producer import SegmentStart, produce
        from c11_primary_decision_seam_v1.exact_rpd5_join import ClosedRpdState, join

        proof = None
        if type(rpd_checkpoint) is ClosedRpdState and rpd_segment is None and not completed_bars5:
            proof = rpd_checkpoint
        elif type(rpd_segment) is SegmentStart:
            try:
                proof = produce(scope, as_of_ms, rpd_segment, completed_bars5)
            except (AssertionError, AttributeError, ArithmeticError, IndexError, TypeError, ValueError):
                return Decision("UNKNOWN", "RPD5_PRODUCER_INPUT_INVALID")
        _, rpd = join(scope, as_of_ms, [proof] if proof is not None else [])
        return decide(scope, as_of_ms=as_of_ms, valid_until_ms=valid_until_ms,
                      candle_symbol=candle_symbol, candles15=candles15, minutes=minutes,
                      oi=bound_oi, rpd=rpd)


    @staticmethod
    def preview_pending_stop(**bound_fixture_inputs):
        """A private fixture handoff, not an authorization or a Freqtrade hook."""
        from c11_pending_bridge_v2 import pre_reconcile
        return pre_reconcile(**bound_fixture_inputs)
