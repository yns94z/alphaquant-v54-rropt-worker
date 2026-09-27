#!/usr/bin/env python3
from __future__ import annotations
import csv,json,math,os,random,sys,time,urllib.error,urllib.request
from datetime import datetime,timezone
PUBLISHER_VERSION="rropt-publisher-0.1.0"
ASSETS={"AVAX","BNB","BTC","DOGE","ETH","SOL","XRP"}; TIMEFRAMES={"1m","3m","5m","15m","1h"}; KINDS={"REVERSAL_HIGH","REVERSAL_LOW","SETUP"}; SIDES={"LONG","SHORT"}; MAX_BARS,MAX_EVENTS=1000,500
def iso_utc(v):
    s=(v or "").strip()
    if not s:return None
    try:d=datetime.fromisoformat(s.replace("Z","+00:00"))
    except ValueError:return None
    if d.tzinfo is None:return None
    return d.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
def _pos(v):
    try:x=float(v)
    except (TypeError,ValueError):return None
    return x if math.isfinite(x) and x>0 else None
def _opt(v):
    s=(v or "").strip() if isinstance(v,str) else v
    return s or None
def parse_bar(row):
    asset,tf=_opt(row.get("asset")),_opt(row.get("timeframe")); ts=iso_utc(row.get("ts",""))
    if asset not in ASSETS:return None,"BAD_ASSET"
    if tf not in TIMEFRAMES:return None,"BAD_TIMEFRAME"
    if ts is None:return None,"BAD_TS"
    o,h,l,c=(_pos(row.get(k,"")) for k in ("open","high","low","close"))
    if None in (o,h,l,c):return None,"MISSING_OHLC"
    if not(h>=max(o,c,l) and l<=min(o,c)):return None,"INCOHERENT_OHLC"
    bar={"asset":asset,"ts":ts,"open":o,"high":h,"low":l,"close":c}; vol=_opt(row.get("volume"))
    if vol is not None:
        try:fv=float(vol)
        except ValueError:return None,"BAD_VOLUME"
        if not math.isfinite(fv) or fv<0:return None,"BAD_VOLUME"
        bar["volume"]=fv
    return {"timeframe":tf,"bar":bar},None
def parse_event(row):
    eid,kind,asset,tf=(_opt(row.get(k)) for k in ("event_id","kind","asset","timeframe")); ts=iso_utc(row.get("ts",""))
    if not eid or len(eid)>128:return None,"BAD_EVENT_ID"
    if kind not in KINDS:return None,"BAD_KIND"
    if asset not in ASSETS:return None,"BAD_ASSET"
    if tf not in TIMEFRAMES:return None,"BAD_TIMEFRAME"
    if ts is None:return None,"BAD_TS"
    ev={"eventId":eid,"kind":kind,"asset":asset,"timeframe":tf,"ts":ts}
    price=_opt(row.get("price"))
    if price is not None:
        p=_pos(price)
        if p is None:return None,"BAD_PRICE"
        ev["price"]=p
    side=_opt(row.get("side"))
    if side is not None:
        if side not in SIDES:return None,"BAD_SIDE"
        ev["side"]=side
    for src,dst in (("signal_id","signalId"),("trade_id","tradeId")):
        v=_opt(row.get(src))
        if v is not None:ev[dst]=v
    return ev,None
def _run_mismatch(r,run_id): rid=_opt(r.get("run_id")); return rid is not None and rid!=run_id
def build_bar_payloads(rows,run_id,worker_id,source):
    by_tf={}; rejected={}
    for r in rows:
        p,err=(None,"RUN_ID_MISMATCH") if _run_mismatch(r,run_id) else parse_bar(r)
        if err:rejected[err]=rejected.get(err,0)+1; continue
        b=p["bar"]; by_tf.setdefault(p["timeframe"],{})[(b["asset"],b["ts"])]=b
    payloads=[]
    for tf in sorted(by_tf):
        bars=[by_tf[tf][k] for k in sorted(by_tf[tf])]
        for i in range(0,len(bars),MAX_BARS):payloads.append({"runId":run_id,"workerId":worker_id,"source":source,"timeframe":tf,"bars":bars[i:i+MAX_BARS]})
    return payloads,rejected
def build_event_payloads(rows,run_id,worker_id,source):
    seen={};rejected={}
    for r in rows:
        e,err=(None,"RUN_ID_MISMATCH") if _run_mismatch(r,run_id) else parse_event(r)
        if err:rejected[err]=rejected.get(err,0)+1;continue
        seen.setdefault(e["eventId"],e)
    evs=[seen[k] for k in sorted(seen)]
    return [{"runId":run_id,"workerId":worker_id,"source":source,"events":evs[i:i+MAX_EVENTS]} for i in range(0,len(evs),MAX_EVENTS)],rejected
def read_csv(path):
    if not os.path.exists(path):return None
    with open(path,newline="",encoding="utf-8") as f:return list(csv.DictReader(f))
class StopPublisher(Exception):pass
def post(base,action,token,body,max_tries=8):
    url=f"{base.rstrip('/')}/api/public/worker/{action}"; data=json.dumps(body).encode(); delay=2.0
    for _ in range(max_tries):
        req=urllib.request.Request(url,data=data,method="POST",headers={"content-type":"application/json","x-worker-token":token})
        try:
            with urllib.request.urlopen(req,timeout=30) as r:return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            txt=e.read().decode("utf-8","replace")[:300]
            if e.code==401:raise StopPublisher("401 — mauvaise URL ou jeton ; arrêt")
            if e.code==409:return {"ok":False,"status":409,"detail":txt}
            if e.code in (400,404):return {"ok":False,"status":e.code,"detail":txt}
        except (urllib.error.URLError,TimeoutError):pass
        time.sleep(delay+random.uniform(0,delay/2)); delay=min(delay*2,60.0)
    return {"ok":False,"status":"RETRY_EXHAUSTED"}
def run_once(env):
    base,token=env["ALPHAQUANT_BASE_URL"],env["WORKER_BRIDGE_TOKEN"]; run_id,worker_id,run_dir=env["RUN_ID"],env["WORKER_ID"],env["RUN_DIR"]; source=env.get("RROPT_SOURCE",f"worker:{PUBLISHER_VERSION}"); report={"version":PUBLISHER_VERSION,"blocked":[]}
    for fname,builder,action,key in (("paper_bars.csv",build_bar_payloads,"paper-bars","bars"),("setup_events.csv",build_event_payloads,"paper-setup-events","events")):
        rows=read_csv(os.path.join(run_dir,fname))
        if rows is None:report["blocked"].append(f"{fname} absent — rien envoyé");continue
        payloads,rejected=builder(rows,run_id,worker_id,source); results=[post(base,action,token,p) for p in payloads]; report[key]={"rows":len(rows),"rejected":rejected,"batches":len(payloads),"results":results}
    return report
def main():
    need=["ALPHAQUANT_BASE_URL","WORKER_BRIDGE_TOKEN","RUN_ID","WORKER_ID","RUN_DIR"]; missing=[k for k in need if not os.environ.get(k)]
    if missing:print(json.dumps({"error":"MISSING_ENV","vars":missing}));return 2
    if "id-preview--" in os.environ["ALPHAQUANT_BASE_URL"]:print(json.dumps({"error":"mauvaise adresse (id-preview--)"}));return 2
    interval=int(os.environ.get("RROPT_INTERVAL_S","60"))
    while True:
        try:print(json.dumps(run_once(dict(os.environ)),default=str),flush=True)
        except StopPublisher as e:print(json.dumps({"error":str(e)}),flush=True);return 1
        if os.environ.get("RROPT_ONCE")=="1":return 0
        time.sleep(interval)
if __name__=="__main__":sys.exit(main())
