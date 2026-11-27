"""Offline trial: exercise the real DP refresh without touching protected paths."""
from contextlib import contextmanager
import time
from local_candle_refresh import refresh_with_local_basis


@contextmanager
def flat_native_refresh(bot):
    due_terminal = set()
    ex = bot.exchange
    refresh = ex.refresh_latest_ohlcv
    previous = vars(ex).get('refresh_latest_ohlcv')
    had_override = 'refresh_latest_ohlcv' in vars(ex)

    def market_id(pair):
        market = ex.markets.get(pair)
        return market.get('id') if isinstance(market, dict) else None

    def scoped(pairs, since_ms=None, cache=True, drop_incomplete=None):
        opts = dict(since_ms=since_ms, cache=cache, drop_incomplete=drop_incomplete)
        if not (bot.config.get('dry_run') is True
                and bot._owned_revisit_no_positions is True
                and bot._owned_revisit_reconciled is True):
            return refresh(pairs, **opts)
        if not hasattr(bot,'_c11_trial_15m_fallback_cache'):
            bot._c11_trial_15m_fallback_cache={}
        bot._c11_trial_native_frame_sources={}
        return refresh_with_local_basis(
            native_refresh=refresh, cache_view=ex._klines, store=bot.policy.bars,
            market_id=market_id, now_ms=lambda: time.time_ns() // 1000000,
            pair_list=pairs, allow_native_15m_fallback=True,
            provenance=bot._c11_trial_native_frame_sources,
            fallback_cache=bot._c11_trial_15m_fallback_cache, due_terminal=due_terminal, **opts,
        )

    ex.refresh_latest_ohlcv = scoped
    try:
        yield due_terminal
    finally:
        if had_override:
            ex.refresh_latest_ohlcv = previous
        else:
            del ex.refresh_latest_ohlcv
