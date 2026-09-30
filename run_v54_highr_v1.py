#!/usr/bin/env python3
import asyncio, argparse, csv, json, math, os, signal, time, urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

# Next-run RR-OPT telemetry is observational only; it must be copied beside this worker.
_TELEMETRY_ROOT = Path(__file__).resolve().parent
if str(_TELEMETRY_ROOT) not in sys.path: sys.path.insert(0, str(_TELEMETRY_ROOT))
from telemetry_rropt.telemetry_writer import TelemetryWriter

import numpy as np, pandas as pd
import websockets

VERSION="V54.3_OKX_EEA_PAPER_HIGHR_V1_BREAKOUT_MTF_FLOW"
ASSETS=["AVAX","BNB","BTC","DOGE","ETH","SOL","XRP"]
SYMBOL={a:f"{a}-USDT-SWAP" for a in ASSETS}
C={"id":"V514_C12_Q2.4_M+","signal_floor":1.25,"w_signal":.70,"w_mom":.75,"w_flow":.30,
   "w_range_penalty":.25,"quality_min":2.4,"pool":6,"cap":12,"cool":24,"max_asset":6,
   "soft":-2.0,"hard":-4.0,"keep":.25,"breakout_z":1.00,"mtf_min":.55,"flow_min":.18,"range_expand_z":.75,"entry_gap_sec":5400}
STOP_BPS=15.; TARGET_R=8.; HORIZON_SEC=1800; TRIG=1.00; GAP=.50; HIST_COST_R=.0833333333333333
RUN_DAYS=1
# Public market-data only. There is deliberately no REST trading endpoint, API key, secret, or order method.
WS_PUBLIC="wss://wseea.okx.com:8443/ws/v5/public"
WS_BUSINESS="wss://wseea.okx.com:8443/ws/v5/business"
REST_INSTRUMENTS="https://eea.okx.com/api/v5/public/instruments?instType=SWAP"
EXPECTED_CTVAL={"AVAX":1.0,"BNB":0.01,"BTC":0.01,"DOGE":1000.0,"ETH":0.1,"SOL":1.0,"XRP":100.0}
WARMUP_HOURS=6

def utcnow(): return datetime.now(timezone.utc)
def iso(t=None): return (t or utcnow()).isoformat()
def atomic_json(path,obj):
    tmp=path.with_suffix(path.suffix+".tmp"); tmp.write_text(json.dumps(obj,indent=2,default=str),encoding="utf-8"); tmp.replace(path)
def append_csv(path,row,fields):
    new=not path.exists()
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        if new:w.writeheader()
        w.writerow(row);f.flush()
def zlast(s,w,m):
    if len(s)<m:return np.nan
    x=pd.Series(s,dtype=float); sd=x.rolling(w,min_periods=m).std().iloc[-1]
    mu=x.rolling(w,min_periods=m).mean().iloc[-1]
    return float((x.iloc[-1]-mu)/(sd+1e-12))
def resample_mom(prices,mins,periods):
    if len(prices)<2:return np.nan
    x=pd.Series({pd.Timestamp(t):p for t,p in prices}).sort_index()
    y=x.resample(f"{mins}min").last().ffill()
    if len(y)<=periods:return np.nan
    return float(y.pct_change(periods).iloc[-1])

class Engine:
    def __init__(self,out,run_id,worker_id,source):
        self.out=out;
        self.run_id=run_id; self.worker_id=worker_id; self.source=source
        self.telemetry=TelemetryWriter(str(out), run_id, worker_id, source); self.process_start=utcnow(); self.start=None; self.stop_at=None
        self.bars={a:deque(maxlen=9000) for a in ASSETS}; self.bucket={}; self.book={}
        self.positions=[]; self.pending_entries={}; self.last_entry={}; self.last_global_entry=None; self.day=None; self.day_trades=0; self.day_net=0.; self.per_asset=defaultdict(int)
        self.total_closed=0; self.total_net=0.; self.closed_wins=0; self.ws_reconnects=0; self.messages=0; self.last_filter_log=None
        self.filter_counts={
            "no_feature":0,
            "signal":0,
            "mtf":0,
            "flow":0,
            "breakout":0,
            "range_expand":0,
            "quality":0,
            "accepted":0,
            "cooldown_global":0,
            "cooldown_asset":0,
            "asset_reject":0,
            "position_or_pending":0,
        }
        self.event_counts={a:{"trades":0,"bbo-tbt":0,"candle5m":0} for a in ASSETS}
        self.last_event_utc={a:{"trades":None,"bbo-tbt":None} for a in ASSETS}
        self.ready=False; self.last_bar_eval=None; self.window_closed=False; self.contracts={}
        self.trade_fields=["trade_id","asset","side","signal_time","entry_time","entry_ref_price","paper_entry_price","entry_spread_bps","entry_latency_ms","quality","signal","exit_time","paper_exit_price","exit_reason","gross_r","cost_r","net_r","duration_sec"]
        atomic_json(out/"V54_PRECOMMIT.json",{
          "version":VERSION,"candidate":C,"assets":ASSETS,"duration_days":RUN_DAYS,"warmup_hours":WARMUP_HOURS,
          "execution":{"stop_bps":STOP_BPS,"target_r":TARGET_R,"horizon_sec":HORIZON_SEC,"trail_trigger":TRIG,"trail_gap":GAP,"historical_cost_r_for_comparison":HIST_COST_R},
          "data":"OKX EEA public WebSocket: trades + bbo-tbt on USDT linear perpetual swaps; independent sockets; adapter candidate, not Binance-equivalent",
          "paper_fill":"first executable book quote observed after signal; BUY uses ask, SELL uses bid; exits use opposite executable side","window_end_policy":"7-day signal/entry window from feed_ready_utc; no new signals or fills at/after planned_stop_utc; pending entries are cancelled; positions already open continue under frozen STOP/TARGET/TRAIL/HORIZON rules until naturally closed; no forced WINDOW_END liquidation",
          "feature_note":"same frozen signal/quality formulas applied causally to finalized 5-second OKX trades buckets; source microstructure differs from Binance aggTrade, so this is a separate validation and not historical replication",
          "trading_access":False,"api_key_used":False,"order_routing":False,"real_trading_enabled":False,"live":"LOCKED",
          "process_started_utc":iso(self.process_start),"started_utc":None,"feed_ready_utc":None,"planned_stop_utc":None})
    def reset_day(self,dt):
        d=dt.date().isoformat()
        if d!=self.day:
            self.day=d; self.day_trades=0; self.day_net=0.; self.per_asset=defaultdict(int); self.last_entry={}
    def load_and_validate_contracts(self):
        req=urllib.request.Request(REST_INSTRUMENTS,headers={"User-Agent":"AlphaQuant-PAPER/1.0"})
        with urllib.request.urlopen(req,timeout=15) as r:
            payload=json.load(r)
        if str(payload.get("code"))!="0":raise RuntimeError(f"OKX instrument metadata error: {payload.get('code')} {payload.get('msg')}")
        by_id={x.get("instId"):x for x in payload.get("data",[])}
        contracts={}
        for a in ASSETS:
            inst=SYMBOL[a]; x=by_id.get(inst)
            if not x:raise RuntimeError(f"DATA LOCK: missing OKX instrument {inst}")
            if x.get("state")!="live" or x.get("ctType")!="linear" or x.get("settleCcy")!="USDT" or x.get("ctValCcy")!=a:
                raise RuntimeError(f"DATA LOCK: incompatible OKX contract metadata for {inst}: {x}")
            ctval=float(x["ctVal"]); ctmult=float(x.get("ctMult") or 1)
            if abs(ctval-EXPECTED_CTVAL[a])>1e-12 or abs(ctmult-1.0)>1e-12:
                raise RuntimeError(f"DATA LOCK: contract value changed for {inst}: ctVal={ctval} ctMult={ctmult}")
            contracts[a]={"instId":inst,"ctVal":ctval,"ctMult":ctmult,"ctValCcy":x.get("ctValCcy"),"ctType":x.get("ctType"),"settleCcy":x.get("settleCcy"),"tickSz":x.get("tickSz"),"lotSz":x.get("lotSz"),"minSz":x.get("minSz"),"state":x.get("state")}
        self.contracts=contracts
        atomic_json(self.out/"V54_OKX_CONTRACTS.json",{"version":VERSION,"retrieved_utc":iso(),"source":REST_INSTRUMENTS,"contracts":contracts})

    def on_book(self,a,d):
        received=utcnow()
        bids=d.get("bids") or []; asks=d.get("asks") or []
        if not bids or not asks:return
        bid=float(bids[0][0]); ask=float(asks[0][0]); evt=int(d.get("ts",0))
        self.book[a]=(bid,ask,evt)
        self.update_positions(a,received,bid,ask)
        pending=self.pending_entries.get(a)
        if pending is None:return
        if received<=pending["signal_received_utc"]:return
        if self.stop_at is not None and received>=self.stop_at: del self.pending_entries[a]; return
        if not self.data_feed_healthy(): del self.pending_entries[a]; return
        self.reset_day(received)
        if self.day_net<=C["hard"] or self.day_trades>=C["cap"] or self.per_asset[a]>=C["max_asset"]:
            del self.pending_entries[a]; return
        direction=pending["direction"]
        fill=ask if direction==1 else bid
        spread=(ask/bid-1)*10000 if bid>0 else np.nan
        tid=f"{self.day}-{self.day_trades+1:02d}-{a}-{int(time.time()*1000)}"
        pos={"trade_id":tid,"asset":a,"direction":direction,"side":pending["side"],"signal_time":pending["signal_time"],"entry_time":iso(received),"entry_ref_price":pending["entry_ref_price"],"paper_entry_price":fill,"entry_spread_bps":spread,"entry_latency_ms":max(0,int((received-pending["signal_received_utc"]).total_seconds()*1000)),"quality":pending["quality"],"signal":pending["signal"],"best":0.,"trail":None,"opened":received}
        self.positions.append(pos); self.day_trades+=1; self.per_asset[a]+=1; self.last_entry[a]=received; self.last_global_entry=received
        del self.pending_entries[a]
        # Record the already-confirmed V54.3 setup after the paper entry exists.
        signal_id=f"{self.run_id}:{a}:{pd.Timestamp(pos['signal_time']).isoformat()}"
        try:
            rr_status=self.telemetry.record_event(
                event_id=signal_id, kind="SETUP", asset=a, timeframe="5s",
                ts=pos["signal_time"], price=pos["paper_entry_price"], side=pos["side"],
                signal_id=signal_id, trade_id=tid,
            )
            if rr_status not in ("WRITTEN","DUPLICATE"):
                print(f"RR-OPT SETUP {tid} {rr_status}",flush=True)
        except Exception as e:
            print(f"RR-OPT SETUP ERROR {tid} {type(e).__name__}: {e}",flush=True)
        append_csv(self.out/"signals.csv",{"time":iso(received),"trade_id":tid,"asset":a,"side":pos["side"],"signal":pending["signal"],"quality":pending["quality"],"ref_price":pending["entry_ref_price"],"paper_fill":fill,"spread_bps":spread},["time","trade_id","asset","side","signal","quality","ref_price","paper_fill","spread_bps"])
        print(f"PAPER OPEN {tid} {a} {pos['side']} q={pending['quality']:.3f} fill={fill}",flush=True)

    def on_candle5m(self,a,d):
        # OKX public candle5m: only confirmed (closed) candles are recorded.
        # This is telemetry only; it never feeds the V54.3 decision path.
        if len(d) < 6 or str(d[-1]) != "1":
            return
        try:
            ts=int(d[0])
            o,h,l,c=[float(d[i]) for i in range(1,5)]
            v=float(d[5])
            close_ts=pd.to_datetime(ts,unit="ms",utc=True).isoformat()
            result=self.telemetry.record_bar(
                asset=a, ts=close_ts, timeframe="5m", open=o, high=h, low=l, close=c, volume=v
            )
            if result not in ("WRITTEN","DUPLICATE"):
                print(f"RR-OPT BAR {a} {result}",flush=True)
        except Exception as e:
            print(f"RR-OPT BAR ERROR {a} {type(e).__name__}: {e}",flush=True)

    def on_trade(self,a,d):
        ts=pd.to_datetime(int(d["ts"]),unit="ms",utc=True); p=float(d["px"]); contracts=float(d["sz"])
        spec=self.contracts.get(a)
        if not spec:return
        q=contracts*spec["ctVal"]*spec["ctMult"]
        taker_sell=(d.get("side")=="sell")
        b=ts.floor("5s")
        x=self.bucket.get(a)
        if x is None or x["bucket"]!=b:
            if x is not None:self.finalize(a,x)
            self.bucket[a]={"bucket":b,"last":p,"high":p,"low":p,"quote":p*q,"count":1,"buyq":0. if taker_sell else p*q,"sellq":p*q if taker_sell else 0.}
        else:
            x["last"]=p;x["high"]=max(x["high"],p);x["low"]=min(x["low"],p);x["quote"]+=p*q;x["count"]+=1
            if taker_sell:x["sellq"]+=p*q
            else:x["buyq"]+=p*q
    def finalize(self,a,x):
        imb=(x["buyq"]-x["sellq"])/(x["buyq"]+x["sellq"]+1e-12)
        rng=(x["high"]/x["low"]-1)*10000 if x["low"]>0 else 0
        self.bars[a].append({"timestamp":x["bucket"],"price":x["last"],"quote_volume":x["quote"],"trade_count":x["count"],"taker_imbalance":imb,"range_bps":rng})
        # Evaluate a cross-section only after all assets have progressed beyond the same finalized bucket.
        common=min((self.bars[z][-1]["timestamp"] for z in ASSETS if self.bars[z]),default=None)
        if common is not None and common!=self.last_bar_eval and all(self.bars[z] and self.bars[z][-1]["timestamp"]>=common for z in ASSETS):
            self.last_bar_eval=common; self.evaluate(common)
    def feature(self,a,t):
        arr=[x for x in self.bars[a] if x["timestamp"]<=t]
        if len(arr)<180:return None
        df=pd.DataFrame(arr); p=df.price.astype(float)
        fast=p.rolling(4,min_periods=4).mean(); slow=p.rolling(36,min_periods=36).mean()
        acc=fast/slow-1
        sig=(acc-acc.rolling(120,min_periods=60).mean())/(acc.rolling(120,min_periods=60).std()+1e-12)
        prices=list(zip(pd.to_datetime(df.timestamp,utc=True),p))
        def mz(mins,periods,w,m):
            s=pd.Series({pd.Timestamp(tt):pp for tt,pp in prices}).sort_index().resample(f"{mins}min").last().ffill().pct_change(periods)
            if len(s)<m:return np.nan
            return zlast(s.dropna().tolist(),w,m)
        mom1=mz(1,5,120,30); mom5=mz(5,3,72,20); mom15=mz(15,2,48,16)
        flow=float(df.taker_imbalance.astype(float).rolling(12,min_periods=6).mean().iloc[-1])
        rz=zlast(np.log1p(np.maximum(df["range_bps"].astype(float),0)).tolist(),120,60)
        recent_range=float(df["range_bps"].tail(6).mean())
        base_range=float(df["range_bps"].tail(72).mean())
        range_expand=recent_range/(base_range+1e-12)
        prior_hi=float(p.iloc[-13:-1].max()); prior_lo=float(p.iloc[-13:-1].min()); last=float(p.iloc[-1])
        span=max(prior_hi-prior_lo,last*1e-9)
        breakout_up=(last-prior_hi)/span; breakout_down=(prior_lo-last)/span
        vals=[float(sig.iloc[-1]),mom1,mom5,mom15,flow,rz]
        if any(pd.isna(v) for v in vals):return None
        signal,m1,m5,m15,flow,rangez=vals
        direction=-1 if signal>0 else 1
        mtf=direction*(.20*m1+.45*m5+.35*m15)
        flow_dir=direction*flow
        breakout=breakout_down if direction==1 else breakout_up
        quality=(C["w_signal"]*abs(signal)+C["w_mom"]*mtf+C["w_flow"]*flow_dir
                 +.70*max(0.0,breakout)+.35*max(0.0,rangez)
                 -C["w_range_penalty"]*max(0,rangez-2.5))
        return {"asset":a,"timestamp":t,"price":last,"signal":signal,"quality":quality,
                "direction":direction,"mtf":mtf,"flow_dir":flow_dir,
                "breakout":breakout,"range_expand":range_expand}
    def data_feed_healthy(self,max_age_sec=120):
        now=utcnow()
        for a in ASSETS:
            for e in ("trades","bbo-tbt"):
                stamp=self.last_event_utc[a][e]
                if not stamp:
                    return False
                try:
                    age=(now-datetime.fromisoformat(stamp)).total_seconds()
                except Exception:
                    return False
                if age>max_age_sec:
                    return False
        return True
    def settlement_feed_healthy(self,max_age_sec=120):
        now=utcnow()
        assets={p["asset"] for p in self.positions}
        for a in assets:
            stamp=self.last_event_utc[a]["bbo-tbt"]
            if not stamp:return False
            try: age=(now-datetime.fromisoformat(stamp)).total_seconds()
            except Exception:return False
            if age>max_age_sec:return False
        return True
    def arm_scientific_clock(self):
        if self.start is not None:
            return False
        if not self.data_feed_healthy():
            return False
        ready_times=[datetime.fromisoformat(self.last_event_utc[a][e]) for a in ASSETS for e in ("trades","bbo-tbt")]
        ready_start=max(ready_times)
        ready_stop=ready_start+timedelta(days=RUN_DAYS)
        ready_path=self.out/"V54_FEED_READY.json"
        if ready_path.exists():
            raise RuntimeError("LOCK: V54_FEED_READY.json already exists; refusing to overwrite scientific T0 evidence.")
        atomic_json(ready_path,{"version":VERSION,"candidate":C["id"],"process_started_utc":iso(self.process_start),"feed_ready_utc":iso(ready_start),"planned_stop_utc":iso(ready_stop),"duration_days":RUN_DAYS,"warmup_hours":WARMUP_HOURS,"feed_health_max_age_sec":120,"transports":{"trades":WS_PUBLIC,"bbo-tbt":WS_PUBLIC},"provider":"OKX EEA","instruments":SYMBOL,"contracts":self.contracts,"event_counts":self.event_counts,"last_event_utc":self.last_event_utc,"paper_only":True,"trading_access":False,"live":"LOCKED"})
        self.start=ready_start
        self.stop_at=ready_stop
        return True
    def evaluate(self,t):
        now=utcnow(); self.reset_day(now)
        if not self.data_feed_healthy(): self.status("DATA_FEED_BLOCKED"); return
        if self.start is None: self.status("DATA_FEED_BLOCKED"); return
        if self.stop_at is not None and now>=self.stop_at:return
        if now-self.start<timedelta(hours=WARMUP_HOURS): self.status("WARMUP");return
        cand=[]
        for a in ASSETS:
            f=self.feature(a,t)

            if not f:
                self.filter_counts["no_feature"]+=1
                continue

            if abs(f["signal"])<C["signal_floor"]:
                self.filter_counts["signal"]+=1
                continue

            if f["mtf"]<C["mtf_min"]:
                self.filter_counts["mtf"]+=1
                continue

            if f["flow_dir"]<C["flow_min"]:
                self.filter_counts["flow"]+=1
                continue

            if f["breakout"]<C["breakout_z"]:
                self.filter_counts["breakout"]+=1
                continue

            if f["range_expand"]<1.0375:
                self.filter_counts["range_expand"]+=1
                continue

            if f["quality"]<C["quality_min"]+.35:
                self.filter_counts["quality"]+=1
                continue

            self.filter_counts["accepted"]+=1
            cand.append(f)

        cand=sorted(cand,key=lambda x:x["quality"],reverse=True)
        if self.day_net<=C["hard"] or self.day_trades>=C["cap"]:return

        for f in cand[:1]:
            a=f["asset"]

            if self.per_asset[a]>=C["max_asset"]:
                self.filter_counts["asset_reject"]+=1
                continue

            if self.last_global_entry is not None and (now-self.last_global_entry).total_seconds()<C["entry_gap_sec"]:
                self.filter_counts["cooldown_global"]+=1
                continue

            if a in self.last_entry and (now-self.last_entry[a]).total_seconds()<5*C["cool"]:
                self.filter_counts["cooldown_asset"]+=1
                continue

            if any(p["asset"]==a for p in self.positions) or a in self.pending_entries:
                self.filter_counts["position_or_pending"]+=1
                continue

            direction=f["direction"]; signal_received=utcnow()
            self.pending_entries[a]={"asset":a,"direction":direction,"side":"LONG" if direction==1 else "SHORT",
                "signal_time":iso(pd.Timestamp(t).to_pydatetime()),"signal_received_utc":signal_received,
                "entry_ref_price":f["price"],"quality":f["quality"],"signal":f["signal"]}

        if self.last_filter_log is None or (now - self.last_filter_log).total_seconds() >= 300:
            print(f"FILTERS {self.filter_counts}",flush=True)
            self.last_filter_log=now

        self.status(self.current_state())
    def update_positions(self,a,received,bid,ask):
        if not self.positions:return
        self.reset_day(received)
        for p in list(self.positions):
            if p["asset"]!=a:continue
            if received<=p["opened"]:continue
            exit_px=bid if p["direction"]==1 else ask
            gross=p["direction"]*(exit_px/p["paper_entry_price"]-1)/(STOP_BPS/10000.)
            p["best"]=max(p["best"],gross);reason=None
            if gross<=-1:reason="STOP"
            elif gross>=TARGET_R:reason="TARGET"
            else:
                if p["best"]>=TRIG:
                    p["trail"]=max(TRIG-GAP,p["best"]-GAP) if p["trail"] is None else max(p["trail"],p["best"]-GAP)
                    if gross<=p["trail"]:reason="TRAIL"
                if (received-p["opened"]).total_seconds()>=HORIZON_SEC:reason=reason or "HORIZON"
            if reason:self.close(p,received,exit_px,gross,reason)
    def close(self,p,when,px,gross,reason):
        # Spread is already embodied by executable bid/ask fills. HIST_COST_R is retained as a conservative comparison charge.
        net=gross-HIST_COST_R
        row={**{k:p[k] for k in ["trade_id","asset","side","signal_time","entry_time","entry_ref_price","paper_entry_price","entry_spread_bps","entry_latency_ms","quality","signal"]},
             "exit_time":iso(when),"paper_exit_price":px,"exit_reason":reason,"gross_r":gross,"cost_r":HIST_COST_R,"net_r":net,"duration_sec":(when-p["opened"]).total_seconds()}
        append_csv(self.out/"paper_trades.csv",row,self.trade_fields)
        self.positions.remove(p);self.total_closed+=1;self.total_net+=net;self.day_net+=net;self.closed_wins+=int(net>0)
        print(f"PAPER CLOSE {p['trade_id']} {reason} net={net:+.3f}R",flush=True);self.status(self.current_state())
    def close_window_if_due(self):
        if self.window_closed:return False
        if self.stop_at is None or utcnow()<self.stop_at:return False
        self.window_closed=True
        self.pending_entries.clear()
        return True
    def current_state(self):
        now=utcnow()
        if self.start is None:return "DATA_FEED_BLOCKED"
        if self.stop_at is not None and now>=self.stop_at:
            if self.positions:return "SETTLING" if self.settlement_feed_healthy() else "SETTLEMENT_FEED_BLOCKED"
            return "SETTLING"
        if not self.data_feed_healthy():return "DATA_FEED_BLOCKED"
        if now-self.start<timedelta(hours=WARMUP_HOURS):return "WARMUP"
        return "RUNNING"
    def status(self,state):
        elapsed=(utcnow()-self.start).total_seconds() if self.start is not None else 0.0
        atomic_json(self.out/"status.json",{"version":VERSION,"state":state,"utc":iso(),"process_started_utc":iso(self.process_start),"started_utc":iso(self.start) if self.start else None,"feed_ready_utc":iso(self.start) if self.start else None,"planned_stop_utc":iso(self.stop_at) if self.stop_at else None,
          "elapsed_hours":elapsed/3600,"warmup_complete":elapsed>=WARMUP_HOURS*3600,"messages":self.messages,"ws_reconnects":self.ws_reconnects,
          "event_counts":self.event_counts,"last_event_utc":self.last_event_utc,
          "open_positions":len(self.positions),"pending_entries":len(self.pending_entries),"today":self.day,"today_trades":self.day_trades,"today_net_r":self.day_net,
          "closed_trades":self.total_closed,"wins":self.closed_wins,"wr":self.closed_wins/self.total_closed if self.total_closed else None,
          "total_net_r":self.total_net,"candidate":C["id"],"paper_only":True,"trading_access":False,"live":"LOCKED"})
    async def window_boundary_timer(self):
        while self.stop_at is None:
            await asyncio.sleep(0.1)
        delay=max(0.0,(self.stop_at-utcnow()).total_seconds())
        await asyncio.sleep(delay)
        self.close_window_if_due()
        self.status(self.current_state())
    async def stream_asset_event(self,a,channel):
        backoff=1
        while self.stop_at is None or utcnow()<self.stop_at or (channel=="bbo-tbt" and any(p["asset"]==a for p in self.positions)):
            try:
                ws_url = WS_BUSINESS if channel == "candle5m" else WS_PUBLIC
                async with websockets.connect(ws_url,ping_interval=20,ping_timeout=20,close_timeout=10,open_timeout=20,max_queue=5000) as ws:
                    await ws.send(json.dumps({"op":"subscribe","args":[{"channel":channel,"instId":SYMBOL[a]}]}))
                    print(f"WS {a} {channel} CONNECTED {iso()}",flush=True)
                    backoff=1
                    async for raw in ws:
                        self.messages+=1
                        self.close_window_if_due()
                        j=json.loads(raw)
                        if j.get("event")=="error":raise RuntimeError(f"OKX subscription error: {j}")
                        arg=j.get("arg") or {}
                        if arg.get("channel")==channel and arg.get("instId")==SYMBOL[a] and j.get("data"):
                            for d in j["data"]:
                                self.event_counts[a][channel]+=1
                                self.last_event_utc[a][channel]=iso()
                                if self.start is None:self.arm_scientific_clock()
                                if channel=="trades":self.on_trade(a,d)
                                elif channel=="bbo-tbt":self.on_book(a,d)
                                elif channel=="candle5m":self.on_candle5m(a,d)
                        if self.messages%5000==0:self.status(self.current_state())
                        if self.stop_at is not None and utcnow()>=self.stop_at and not (channel=="bbo-tbt" and any(p["asset"]==a for p in self.positions)):break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.ws_reconnects+=1
                print(f"WS {a} {channel} RECONNECT {type(e).__name__}: {e} | wait {backoff}s",flush=True)
                self.status("RECONNECTING")
                await asyncio.sleep(backoff)
                backoff=min(60,backoff*2)

    async def run(self):
        print("V54.3-OKX PAPER ADAPTER CANDIDATE | PUBLIC MARKET DATA ONLY | NO API KEY | NO ORDER ROUTING | LIVE LOCKED",flush=True)
        print(f"Warmup={WARMUP_HOURS}h | planned paper window={RUN_DAYS}d | assets={','.join(ASSETS)}",flush=True)
        print("Provider: OKX EEA | trades + bbo-tbt | USDT linear SWAP | fail-closed feed health.",flush=True)
        self.load_and_validate_contracts()
        tasks=[asyncio.create_task(self.window_boundary_timer(),name="window-boundary-timer")]
        for a in ASSETS:
            tasks.append(asyncio.create_task(self.stream_asset_event(a,"trades"),name=f"ws-{a}-trades"))
            tasks.append(asyncio.create_task(self.stream_asset_event(a,"bbo-tbt"),name=f"ws-{a}-bbo-tbt"))
            # candle5m disabled: OKX rejects this subscription; telemetry only
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
        self.status("COMPLETE_7D")
        print("V54.3-OKX 7-DAY WINDOW COMPLETE | LIVE LOCKED",flush=True)


def main():
    ap=argparse.ArgumentParser();ap.add_argument("--out",required=True);ap.add_argument("--run-id",required=True);ap.add_argument("--worker-id",required=True);ap.add_argument("--source",default="okx-public-eea")
    a=ap.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    if (out/"V54_PRECOMMIT.json").exists():raise SystemExit("LOCK: output folder already initialized; use a new run directory.")
    asyncio.run(Engine(out,a.run_id,a.worker_id,a.source).run())
if __name__=="__main__":main()




