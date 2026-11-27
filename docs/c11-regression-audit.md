# C11 Dry Run regression inventory (2026-11-27, Asia/Taipei)

This is the coverage inventory for PR #11, not a release receipt. Compare full
native Worker execution, component checks, and Mac handoff separately. A known
bug remains open if the expected outcome was not observed on the relevant image.

| Requirement | Current evidence | Missing proof to keep open |
| --- | --- | --- |
| Dynamic recent backfill without listing-day default | Previous SQD/EVAA targeted history, MMT 45 one-minute refill, BAND bounded-page planner | Enforce zero unnecessary pages and exact raw minutes on packaged Worker for blank, stale, partially filled and already-complete candidates. Verify a missing analysis frame and an existing wide-row cache do not block real refill. |
| Blank, stale, partial and returning candidates | Older original archive replay did reconstruct blank and stale JCT from raw input and delivered the original 5m veto; current GitHub suite has independent variants and 45-minute warm callback | Restore automatic trusted-source selection without reopening an unbounded listing scan. Run true first qualifying window, as well as drop and return, with no preloaded analytical state and a separate positive original-strategy case. Add BAND 2020, ME segment-only and the real MOVE positive-primary miss; do not certify by JCT alone. |
| Same-window delivery without a fixed 65s barrier | Previous staged E+5 preparation, E+20 delivery, E+19 original veto and E+7 warm original callback | Exact native multi-candidate run: individual decisions, no double delivery or replay order, one slow candidate cannot delay others, no OOM or lost completion under contention. |
| Original OI intake and contract identity | OI20 fixture routes exact ranks to Freqtrade; component cases use Unicode symbols | Closed replay from publisher protocol to Worker: Chinese genuine IDs (including 牛来USDT), restart/continuation, invalid aliases, lease expiry and an isolated bad row while another candidate completes. Do not modify the live OI20 collector. |
| 1m-first routing and narrow same-close 15m fallback | Hosted 45-minute warm callback + independent readback; component tests cover positive priority and invalid fallback | Verify historical and live paths on final Worker: no direct 5m, 15m backup never replaces base history or feeds an old close, preserve origination, not blocked on harmless cross-timeframe difference. Preserve original strategy's separate 5m entry fallback and filters. |
| Original protection and historical corrections | Isolated old images exercised the approved OPG correction and held-position/pending-intent controls | Re-run on the actual candidate, keep corrections append-only, verify data backfill is separate from holding protection and preserves position lifecycle behavior. |
| Actual artifact and Mac handoff | GitHub requires hosted check; disabled startup and digest pin checks; portable checks reject a wrong launch/mount | Publish **one** verified ARM64 artifact, use that exact artifact for all independent cases and compare its immutable deployment identity. Validate same ledger read-only, one writer, no unresolved positions/intents, usable rollback and independent snapshot before any cutover. Confirm real post-cutover per-candidate decisions. |

Sources to reconcile before closing:
[PR #11](https://github.com/devilbats80224-pixel/c11-dry-run/pull/11),
[hosted run 45917481900](https://github.com/devilbats80224-pixel/c11-dry-run/actions/runs/45917481900),
[independently replayed warm callback 45918026508](https://github.com/devilbats80224-pixel/c11-dry-run/actions/runs/45918026508),
and [separate ARM64 case jobs](https://github.com/devilbats80224-pixel/c11-dry-run/actions/runs/45918803587).
Do not count a prebuilt seed, a bare minimum warmup length, a manual green
status, or replay of a different artifact as resolution of an open item.

Delivery procedure: capture a failing fixture before the fix, keep the
requirement executable in the repository, use independent GitHub status checks,
then verify and deploy after merge. Factor the candle ingest/repair and this
procedure for reuse by other strategies without copying C11 signal logic or
binding another strategy's deployment to C11.

### 已驗證清單、沒有原始快取的重建路徑

清單可明確指定 `pinned_official_download`。這個設定和原有純本機快取不同：只封存已審核的合約、截止時間、每個官方檔案的 URL、長度及 SHA-256；需要時才由原有背景程式讀取官方原始檔，再用原策略重算。官方回傳的校驗檔也必須符合封存指紋，不能代替原封存指紋。不延長候選有效期間，不掃描上市歷史作為預設，不以近期行情冒充狀態。

GitHub 保留「原始快取可用」、「只有驗過清單」、「沒有快取也沒有可信清單」三類情境；前兩者可在相同來源重建，最後一類不是前兩者的成功證據。草稿中的測試封存只覆蓋該測試合約；不能當成現役全市場已有清單或已完成部署。

### 不預載清單的自動尋源候選版

GitHub 的停用映像顯式開啟 `BOUNDED_OFFICIAL_ARCHIVE_V1`，測試沒有本機快取、沒有事先審核清單時，能否由已驗證的合約起點自動列出官方閉市封存、校驗和讀取逐份原始檔，再透過原策略狀態重建。現有清單或可續接狀態優先；這不是一般行情 API 從上市日逐根回抓。無已知狀態時，計畫超出既有 60 份／26 MiB 預算就不把它當可完成的方案（已知超出檔案數時不發任何取數請求；實際位元組在每一片累計）。不允許把未讀完或過期的狀態提交給原策略。

這是隔離候選版，沒有改動既有 Docker 設定。若要部署須另外核對實際最終映像、同一模擬帳本和切換授權。未取得可信起點、超過背景預算或官方封存有缺口時，不能宣稱所有老幣均可當場重建。
