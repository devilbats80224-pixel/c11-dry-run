"""Public OI20-to-Freqtrade boundary inputs; no broker, secrets, or live I/O."""
from decimal import Decimal

from .bridge_protocol import Refused
from .exact_symbol import quote_contract_shape
from .oi20_rank_slots import bridge_slots


def make_publication(now):
    # Public CI injects a provider response, not a production credential.
    raw = [
        (f"DUMMY{i}USDT", "0.1999", True)
        for i in range(1, 61)
    ]
    raw[:4] = [
        ("STOCKUSDT", "0.9", False),
        ("INVALID\u200dUSDT", "0.7", True),
        ("龙虾USDT", "0.20", True),
        ("BELOWUSDT", "0.1999", True),
    ]
    kept = [(rank, {"symbol": symbol, "native_oi_change_24h_raw": value,
                             "verified_crypto": crypto})
            for rank, (symbol, value, crypto) in enumerate(raw, 1)
            if quote_contract_shape(symbol)]
    return {"status": "FRESH_PARTIAL", "expires_ms": now + 60_000,
            "rows": [row for _, row in kept],
            "provider_ranks_v1": [rank for rank, _ in kept]}


def test_scope(publication, now):
    if now >= publication["expires_ms"]:
        return []
    try:
        indices = bridge_slots(publication)
    except Refused:
        return []
    return [
        (rank, f'{row["symbol"][:-4]}/USDT:USDT')
        for rank, row in zip(indices, publication["rows"])
        if row["verified_crypto"]
        and quote_contract_shape(row["symbol"])
        and Decimal(row["native_oi_change_24h_raw"]) >= Decimal("0.20")
    ]
