"""MPL-2.0. Signal-only translation of AlgoAlpha NW v1, copyright AlgoAlpha.
Original Pine SHA bf8fcb560555f607fe3671e58f1be69133502ee4a409399cec3dd8f4cb62cf1a.
No order, position-sizing or exit decisions. Input rows must be completed bars.
"""
import numpy as np
import pandas as pd

def first_rebound(osc, rebound):
    consumed = False
    out = np.zeros(len(osc), dtype=bool)
    for i, value in enumerate(osc):
        if not np.isfinite(value) or value <= 0:
            consumed = False
        elif rebound[i] and not consumed:
            out[i] = True
            consumed = True
    return out

def calculate(frame):
    c = frame['close'].to_numpy(dtype=float)
    if not np.isfinite(c).all():
        raise ValueError('Nonfinite closes: split and rewarm contiguous source segments')
    n = len(c)
    out = frame.copy()
    if not n:
        raise ValueError('Empty source')
    w = np.exp(-np.arange(140, dtype=float)**2 / (2 * 30.0**2))
    nw = np.convolve(c, w, 'full')[:n] / w.sum()
    nw[:139] = np.nan
    slope = np.r_[np.nan, np.diff(nw)]
    # Pine stdev default biased=true; population variance, not sample ddof=1.
    std = pd.Series(slope).rolling(100, min_periods=100).std(ddof=0).to_numpy()
    ratio = np.divide(slope, std, out=np.full(n, np.nan), where=std > 0)
    osc = np.convolve(ratio, np.arange(21, 0, -1, dtype=float), 'full')[:n] / 231
    previous = np.r_[np.nan, osc[:-1]]
    known = np.isfinite(osc)
    rebound = known & (osc > 0) & (np.r_[np.nan, c[:-1]] < nw) & (c > nw)
    out['nw'] = nw
    out['nw_osc'] = osc
    out['nw_known'] = known
    out['nw_green'] = known & (osc > 0)
    out['nw_red'] = known & (osc <= 0)
    out['nw_bullish_zero_cross'] = known & np.isfinite(previous) & (osc > 0) & (previous <= 0)
    out['nw_bearish_zero_cross'] = known & np.isfinite(previous) & (osc < 0) & (previous >= 0)
    out['nw_bullish_rebound'] = rebound
    # Consume first triangle before OI20, wallet or slot gating.
    out['nw_first_rebound'] = first_rebound(osc, rebound)
    if 'high' in out:
        for strength in (2, 3):
            out[f'nw_swing{strength}_breakout'], out[f'nw_swing{strength}_anchor'] = structural_breakouts(out, strength)
        out['nw_swing2_samebar'], _ = structural_breakouts(out, 2, True)
    return out

def entry_mask(frame, mode):
    cols = {'triangle':'nw_bullish_rebound', 'flip':'nw_bullish_zero_cross', 'first_rebound':'nw_first_rebound', 'swing2':'nw_swing2_breakout', 'swing3':'nw_swing3_breakout', 'swing2_samebar':'nw_swing2_samebar'}
    if mode not in cols:
        raise ValueError(mode)
    if 'oi20_eligible' not in frame:
        return pd.Series(False, index=frame.index)
    oi = frame['oi20_eligible'].eq(True).fillna(False)
    return frame[cols[mode]].fillna(False) & frame['nw_green'] & oi & frame['volume'].gt(0)

def structural_breakouts(frame, strength=2, same_triangle_only=False):
    """Experimental confirmed-pivot proxy, not a claim to automate discretionary video.
    Anchor is latest strict swing high confirmed by a PREVIOUS completed bar.
    Current triangle's own high and any future right bars cannot select its anchor.
    New triangle replaces pending anchor; red/unknown cancels. One signal per setup.
    """
    h = frame['high'].to_numpy(float); c = frame['close'].to_numpy(float)
    green = frame['nw_green'].to_numpy(bool)
    tri = frame['nw_bullish_rebound'].to_numpy(bool)
    n = len(frame); last = np.nan; anchor = np.nan
    out = np.zeros(n, bool); anchors = np.full(n,np.nan)
    for i in range(n):
        if not green[i]: anchor = np.nan
        elif tri[i]: anchor = last  # last was confirmed no later than i-1
        elif same_triangle_only: anchor = np.nan
        anchors[i] = anchor
        if green[i] and np.isfinite(anchor) and c[i] > anchor:
            out[i] = True; anchor = np.nan
        # Confirmation available only AFTER this row's anchor selection.
        j = i-strength
        if j >= strength:
            neighbors = np.r_[h[j-strength:j], h[j+1:i+1]]
            if np.isfinite(h[j]) and np.isfinite(neighbors).all() and (h[j]>neighbors).all():
                last = h[j]
    return out, anchors
