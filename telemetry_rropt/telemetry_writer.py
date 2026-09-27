#!/usr/bin/env python3
from __future__ import annotations
import csv, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "publisher_rropt"))
from publisher_rropt import parse_bar, parse_event

WRITER_VERSION = "rropt-telemetry-writer-0.1.0"
BAR_COLUMNS = ["run_id","worker_id","source","asset","ts","timeframe","open","high","low","close","volume"]
EVENT_COLUMNS = ["run_id","worker_id","source","event_id","kind","asset","timeframe","ts","price","side","signal_id","trade_id"]

def _fmt(v):
    return "" if v is None else (repr(float(v)) if isinstance(v,(int,float)) else str(v))

class _Csv:
    def __init__(self,path,columns,run_id):
        self.path,self.columns=path,columns; self.index={}; self.foreign_rows=0; self.torn_rows=0
        if os.path.exists(path) and os.path.getsize(path)>0:
            with open(path,"rb") as f:
                f.seek(-1,os.SEEK_END); torn_tail=f.read(1)!=b"\n"
            if torn_tail:
                with open(path,"a",encoding="utf-8") as f: f.write("\n")
            with open(path,newline="",encoding="utf-8") as f:
                rd=csv.DictReader(f)
                if rd.fieldnames!=columns: raise ValueError(f"{os.path.basename(path)} : en-tête inattendu {rd.fieldnames}")
                for r in rd:
                    if None in r or any(r.get(c) is None for c in columns):
                        self.torn_rows+=1; continue
                    if r.get("run_id")!=run_id: self.foreign_rows+=1; continue
                    self.index[self.key(r)]=r
        else:
            with open(path,"w",newline="",encoding="utf-8") as f: csv.writer(f).writerow(columns)
    def key(self,r): raise NotImplementedError
    def put(self,row):
        k=self.key(row); prev=self.index.get(k)
        if prev is not None:
            return "DUPLICATE" if all(prev.get(c,"")==row[c] for c in self.columns) else "CONFLICT"
        with open(self.path,"a",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow([row[c] for c in self.columns]); f.flush(); os.fsync(f.fileno())
        self.index[k]=row; return "WRITTEN"

class _Bars(_Csv):
    def key(self,r): return (r["asset"],r["timeframe"],r["ts"])
class _Events(_Csv):
    def key(self,r): return (r["event_id"],)

class TelemetryWriter:
    def __init__(self,run_dir,run_id,worker_id,source):
        for name,v in (("run_id",run_id),("worker_id",worker_id),("source",source)):
            if not v or not str(v).strip(): raise ValueError(f"{name} obligatoire (provenance)")
        os.makedirs(run_dir,exist_ok=True); self.run_id,self.worker_id,self.source=run_id,worker_id,source
        self.bars=_Bars(os.path.join(run_dir,"paper_bars.csv"),BAR_COLUMNS,run_id)
        self.events=_Events(os.path.join(run_dir,"setup_events.csv"),EVENT_COLUMNS,run_id); self.counts={}
    def _count(self,s): self.counts[s]=self.counts.get(s,0)+1; return s
    def _prov(self): return {"run_id":self.run_id,"worker_id":self.worker_id,"source":self.source}
    def record_bar(self,*,asset,ts,timeframe,open,high,low,close,volume=None):
        raw={"asset":asset,"ts":ts,"timeframe":timeframe,"open":_fmt(open),"high":_fmt(high),"low":_fmt(low),"close":_fmt(close),"volume":_fmt(volume)}
        p,err=parse_bar(raw)
        if err: return self._count(f"REJECTED:{err}")
        b=p["bar"]; row={**self._prov(),"asset":b["asset"],"ts":b["ts"],"timeframe":p["timeframe"],**{k:_fmt(b[k]) for k in ("open","high","low","close")},"volume":_fmt(b.get("volume"))}
        return self._count(self.bars.put(row))
    def record_event(self,*,event_id,kind,asset,timeframe,ts,price=None,side=None,signal_id=None,trade_id=None):
        raw={"event_id":event_id,"kind":kind,"asset":asset,"timeframe":timeframe,"ts":ts,"price":_fmt(price),"side":_fmt(side),"signal_id":_fmt(signal_id),"trade_id":_fmt(trade_id)}
        e,err=parse_event(raw)
        if err: return self._count(f"REJECTED:{err}")
        row={**self._prov(),"event_id":e["eventId"],"kind":e["kind"],"asset":e["asset"],"timeframe":e["timeframe"],"ts":e["ts"],"price":_fmt(e.get("price")),"side":e.get("side",""),"signal_id":e.get("signalId",""),"trade_id":e.get("tradeId","")}
        return self._count(self.events.put(row))
