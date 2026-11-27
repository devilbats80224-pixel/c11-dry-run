"""Native minute tail sizing, not an indicator warm-up rule."""
MINUTE=60_000
FIVE=300_000
QUARTER=900_000
DEFAULT=30
MAX_MINUTES=495  # largest whole 5m span within existing 499-row transport class

def valid_minute_span(n):
    return type(n) is int and 15<=n<=MAX_MINUTES and n%5==0

def select_minute_span(end,items):
    if type(end) is not int or end%QUARTER:return None
    needed=DEFAULT
    for at,known in items:
        if known is None:
            if type(at) is not int or not end-MAX_MINUTES*MINUTE<=at<end-QUARTER or at%FIVE:
                return None
            needed=max(needed,(end-at)//MINUTE)
    return needed if valid_minute_span(needed) else None
