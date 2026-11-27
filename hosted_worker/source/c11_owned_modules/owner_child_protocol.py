"""Bound bounded frames, no authority bytes or DB descriptors."""
import json
from dataclasses import dataclass

class PeerHold(ValueError):pass
CAP=512*1024
def frame(value,max_bytes=CAP):
    b=(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode('ascii')
    if len(b)>max_bytes:raise PeerHold('HISTORY_PEER_FRAME_LIMIT')
    return b

def strict_line(data,max_bytes=CAP):
    if len(data)>max_bytes or not data.endswith(b'\n') or data.count(b'\n')!=1:
        raise PeerHold('HISTORY_PEER_FRAME_LIMIT')
    try:x=json.loads(data)
    except (ValueError,UnicodeError) as e:raise PeerHold('HISTORY_PEER_PARSE') from e
    if type(x) is not dict:raise PeerHold('HISTORY_PEER_PARSE')
    return x
