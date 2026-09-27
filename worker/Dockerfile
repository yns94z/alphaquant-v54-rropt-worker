FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY run_v54_next_rropt.py /app/run_v54_next_rropt.py
COPY telemetry_rropt /app/telemetry_rropt
COPY publisher_rropt /app/publisher_rropt

ENV RROPT_TELEMETRY_DIR=/app/telemetry_rropt

CMD ["sh", "-c", "exec python -u /app/run_v54_next_rropt.py --out /data/${RUN_ID}"]
