"""One unchanged timing contract shared by 5m-only triad sender and proof owner."""
CLOSE_AGE_MS=10_000
RECEIPT_GAP_MS=5_000
def first_send_floor(end_ms,initial_received_ms):
 return max(end_ms+CLOSE_AGE_MS,max(initial_received_ms)+RECEIPT_GAP_MS)
def second_send_floor(first_received_ms):
 return max(first_received_ms)+RECEIPT_GAP_MS
