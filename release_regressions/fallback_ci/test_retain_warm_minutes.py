"""Exercise the actual C11 projection; all admitted raw timestamps must survive."""
from causal_bars import ClosedBar
from native_minute_projection import MIN, QUARTER, project

anchor=500*QUARTER
for count in (15,30,45,495):
    receipt=anchor+900
    expected=list(range(anchor-count*MIN,anchor,MIN))
    raw=tuple((t,'4','5','3','4','1',t+MIN-1,'4',1,'0','0',0) for t in expected)
    hist=tuple(ClosedBar(t,t+QUARTER+1,4,5,3,4,15,60)
        for t in range(anchor-260*QUARTER,anchor-QUARTER,QUARTER))
    batches=project(anchor,raw,receipt,receipt+1,(),hist)
    assert batches is not None, f'MISSING_BATCH_{count}'
    actual=[b.open_ms for b in batches[0][1]]
    assert actual==expected,f'RAW_1M_TAIL_TRUNCATED {count=} kept={len(actual)}'
    assert len(batches[-1][1])==260
    assert project(anchor,raw[:1]+raw[2:],receipt,receipt+1,(),hist) is None
print('NATIVE_VALIDATED_1M_PROJECTION_RETAINS_15_30_45_495')
