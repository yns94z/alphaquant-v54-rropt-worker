NEW PAPER RUN — RR-OPT

Worker service:
- Dockerfile -> V54.3 next worker
- requirements.txt -> numpy, pandas, websockets
- RUN_ID must be new; never AQ-V54-3-OKX-RUN02
- WORKER_ID must be new
- RROPT_TELEMETRY_DIR=/app/telemetry_rropt
- /data is the dedicated RR-OPT volume
- worker command writes to /data/${RUN_ID}

Publisher service:
- use Dockerfile.publisher
- same RUN_ID, WORKER_ID and RUN_DIR=/data/${RUN_ID}
- ALPHAQUANT_BASE_URL must point to the app DEV endpoint
- WORKER_BRIDGE_TOKEN must be supplied as a Railway secret/reference
- RROPT_INTERVAL_S=60

Safety:
- Do not redeploy or restart alphaquant-v54-3-okx.
- Do not reuse RUN-02 identifiers or its volume.
- No API key and no order routing; LIVE remains LOCKED.
- The worker records only data it already observes. No reversal point is invented.
