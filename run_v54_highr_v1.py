VERSION="V54.3_OKX_EEA_PAPER_B7D"

ASSETS=["AVAX","BNB","BTC","DOGE","ETH","SOL","XRP"]
SYMBOL={a:f"{a}-USDT-SWAP" for a in ASSETS}

C={
    "id":"B_2024_FIXED",

    # Configuration B validee historiquement
    "signal_floor":2.0385,
    "w_signal":.70,
    "w_mom":.75,
    "w_flow":.30,
    "w_range_penalty":.25,

    # V54 quality / selection
    "quality_min":2.4,
    "pool":6,
    "cap":12,
    "cool":24,
    "max_asset":6,

    # Risk / daily controls
    "soft":-2.0,
    "hard":-4.0,
    "keep":.25,

    # B thresholds
    "breakout_z":0.2725,
    "mtf_min":.55,
    "flow_min":.18,
    "range_expand_z":.75,"range_expand_min":1.1003,

    # B cooldown = 120 minutes
    "entry_gap_sec":7200
}

STOP_BPS=15.
TARGET_R=8.
HORIZON_SEC=1800
TRIG=1.00
GAP=.50
HIST_COST_R=.0833333333333333

# PAPER TEST = 7 JOURS
RUN_DAYS=7

# Warmup
WARMUP_HOURS=6

# Public market data only.
# No API key / no order routing / LIVE LOCKED.
WS_PUBLIC="wss://wseea.okx.com:8443/ws/v5/public"
WS_BUSINESS="wss://wseea.okx.com:8443/ws/v5/business"

REST_INSTRUMENTS=(
    "https://eea.okx.com/api/v5/public/"
    "instruments?instType=SWAP"
)

EXPECTED_CTVAL={
    "AVAX":1.0,
    "BNB":0.01,
    "BTC":0.01,
    "DOGE":1000.0,
    "ETH":0.1,
    "SOL":1.0,
    "XRP":100.0
}


