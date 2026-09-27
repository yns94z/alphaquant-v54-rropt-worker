# Integration next-run
Copy `telemetry_rropt/` and `publisher_rropt/` side-by-side.
Open `TelemetryWriter(RUN_DIR, RUN_ID, WORKER_ID, source=...)` once at startup.
Call `record_bar(...)` only after a public-feed candle is actually closed.
Call `record_event(...)` only for a setup/pivot the worker actually confirms.
Wrap telemetry calls so telemetry errors cannot stop the PAPER loop.
Use a NEW RUN_ID and WORKER_ID; never reuse AQ-V54-3-OKX-RUN02.
Run the publisher as a separate process with the same RUN_ID/WORKER_ID/RUN_DIR and `RROPT_INTERVAL_S=60`.
Use the app DEV base URL and reference the bridge token from Railway secrets.
