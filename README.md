# Independent B0–B3 Spot alert comparison

Owner-authorized public-data research running alongside the unchanged [A0–A3 study](https://github.com/fengoftian/spot-alert-comparison-pilot).

Five paper models: a contemporaneous A2 control, earlier coin-relative acceleration (B0), BTC-relative strength (B1), buying-pressure confirmation (B2), and breakout/retest timing (B3). Exact definitions and common risk/cost controls are in [gen/DESIGN.md](gen/DESIGN.md). These are hypotheses, not validated trading strategies.

GitHub prepares lagged public warmup, freezes a future 30-day window, then runs minute collection in bounded single-writer leases. Immutable raw receipts, seed parts and state journals are stored on `observations`. `gen/FREEZE.json` pins dates and scientific identities; `CLOUD_STATUS.json` identifies the actual producer and checkpoint. Warmup is not prospective evidence. The maximum settlement tail is four hours; the workflow disables itself afterward.

Standard Ubuntu hosted runners in a public repository; no paid runner, Actions artifact/cache storage, Git LFS or paid external API. Scheduling is best effort. Collection requires no laptop, exchange account, API key or real capital. No live orders, derivatives, leverage or borrowing. All 45 simulated accounts are alternatives rather than one aggregate allocation.

The 30-day screen reports whole-account period returns with drawdown, deployment and costs at 2000 USDT primary plus 250/2500 scale checks. It cannot establish long-run CAGR, full-cycle qualification or a best strategy. Trading remains unauthorized.
