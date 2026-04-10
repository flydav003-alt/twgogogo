"""
台股GOGOGO 上市專用模型 v2.0 — 正式版
Phase 0: TWSE OpenAPI 免費 pre-filter（1023→~290 檔）
Phase 1: FinMind Token1 → 股價+營收
Phase 2: FinMind Token2 → 籌碼（BATCH=15, delay=4s, retry=2）
輸出: output/Twgogogo_YYYYMMDD.csv
"""
import subprocess,sys,os,time,warnings,json,smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
import requests
from scipy import stats as scipy_stats
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',30)

# ════════════════════════════════════════
# 常數
# ════════════════════════════════════════
FINMIND_TOKEN_1=os.environ.get("FINMIND_TOKEN_1","") or os.environ.get("FINMIND_TOKEN","")
FINMIND_TOKEN_2=os.environ.get("FINMIND_TOKEN_2","") or FINMIND_TOKEN_1
TELEGRAM_TOKEN=os.environ.get("TELEGRAM_TOKEN","")
TELEGRAM_CHAT_ID=os.environ.get("TELEGRAM_CHAT_ID","")
GMAIL_USER=os.environ.get("GMAIL_USER","")
GMAIL_APP_PASS=os.environ.get("GMAIL_APP_PASS","")
EMAIL_TO=os.environ.get("EMAIL_TO","")
GITHUB_PAGES_URL=os.environ.get("REPORT_URL","")
TSE_CSV_PATH=os.environ.get("TSE_CSV_PATH","stock_list.csv")

PREFILTER_TURNOVER=150_000_000
A_VOL_MA5_MIN=800; A_TURNOVER_MIN=250_000_000; A_PRICE_MIN=10
A_LIMIT_DAYS=3; A_LIMIT_THRESHOLD=0.095
B1_VOL_RATIO_MIN=1.3; B1_VOL_RATIO_MAX=4.0; B2_RETURN_MIN=0.01
B2_RETURN_MAX_PCT=9.0; B4_CLOSE_RATIO=0.65; B_PASS_COUNT=2
C_CONSEC_DAYS_MIN=3; C_SINGLE_MIN=200
D_RSI_MAX=78; D_RETURN_MAX=0.10
W_VOL_RATIO=1.6; W_HIGH20=1.4; W_MA28_BIAS=1.0; W_INST_DAYS=3.0; W_RETURN_PCT=0.8
EW_VOL_RATIO_MIN=1.3; EW_VOL_RATIO_MAX=4.0; EW_RETURN_MAX=9.0; EW_RETURN_MIN=-2.0
EW_MA28_BIAS_MAX=25.0; EW_CONSOL_RATIO=1.12; EW_TURNOVER_MIN=250_000_000
EW_ABOVE_MA20_MIN=0; EW_MAX20D_RET_MAX=20.0; EW_INST_MIN=80
EW_PAST60D_MAX=45.0; EW_PAST60D_BIAS=25.0
EW_BONUS_YOY=16.0; EW_BONUS_INST=24.0; EW_BONUS_60D=22.0
COMPOSITE_EARLY_W=0.52; COMPOSITE_TOTAL_W=0.48; INST_CONSEC_WEIGHT=2.2
MIN_DAYS=60; ERROR_LOG="error_log.txt"

BATCH_PRICE=40; DELAY_PRICE=1.5
BATCH_INST=15; DELAY_INST=4.0; RETRY_INST=2; RETRY_WAIT=10
BATCH_REV=40; DELAY_REV=1.5

TODAY=datetime.today()
END_DATE=TODAY.strftime('%Y-%m-%d')
START_DATE=(TODAY-timedelta(days=400)).strftime('%Y-%m-%d')
TODAY_STR=TODAY.strftime('%Y%m%d')
TODAY_DISP=TODAY.strftime('%Y/%m/%d')
print(f"[上市專用模型v2.0] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[系統] {START_DATE} → {END_DATE}")

# ════════════════════════════════════════
# 工具
# ════════════════════════════════════════
def log_error(msg):
    with open(ERROR_LOG,'a',encoding='utf-8') as f:
        f.write(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}\n')

def calc_rsi(s,p=14):
    d=s.diff(); g=d.clip(lower=0); l=(-d).clip(lower=0)
    ag=g.ewm(alpha=1/p,adjust=False).mean()
    al=l.ewm(alpha=1/p,adjust=False).mean()
    rs=ag/al.replace(0,np.nan)
    return 100-(100/(1+rs))

def calc_macd(s,f=12,sl=26,sg=9):
    ef=s.ewm(span=f,adjust=False).mean()
    es=s.ewm(span=sl,adjust=False).mean()
    m=ef-es; return m-m.ewm(span=sg,adjust=False).mean()

def consec_buy_days(series):
    if series is None or len(series)==0: return 0
    vals=series.dropna().values[::-1]; c=0
    for v in vals:
        if v>0: c+=1
        else: break
    return c

def safe_zscore(arr):
    a=np.array(arr,dtype=float)
    if len(a)<2 or np.nanstd(a)==0: return np.zeros_like(a)
    return scipy_stats.zscore(a,nan_policy='omit')

def calc_indicators(df):
    if df is None or df.empty: return None
    df=df.rename(columns={'max':'high','min':'low','Trading_Volume':'volume','Trading_money':'turnover'})
    for c in ['date','open','high','low','close','volume']:
        if c not in df.columns: return None
    df=df.sort_values('date').reset_index(drop=True)
    for c in ['open','high','low','close','volume']:
        df[c]=pd.to_numeric(df[c],errors='coerce')
    df=df.dropna(subset=['close','volume'])
    if len(df)<MIN_DAYS: return None
    if 'turnover' in df.columns:
        df['turnover']=pd.to_numeric(df['turnover'],errors='coerce').fillna(0)
        m=df['turnover']<=0
        df.loc[m,'turnover']=df.loc[m,'close']*df.loc[m,'volume']*1000
    else:
        df['turnover']=df['close']*df['volume']*1000
    df['MA5']=df['close'].rolling(5).mean()
    df['MA20']=df['close'].rolling(20).mean()
    df['MA28']=df['close'].rolling(28).mean()
    df['vol_ma5']=df['volume'].rolling(5).mean()
    df['high20']=df['high'].rolling(20).max()
    df['daily_return']=df['close'].pct_change()
    df['RSI14']=calc_rsi(df['close'],14)
    h=calc_macd(df['close'])
    df['MACD_hist']=h; df['MACD_hist_prev']=h.shift(1)
    df['amplitude']=df['high']-df['low']
    return df

# ════════════════════════════════════════
# Phase 0: TWSE pre-filter
# ════════════════════════════════════════
def load_stock_list():
    df_csv=None
    for enc in ['utf-8-sig','utf-8','cp950','big5','latin1']:
        try:
            df_csv=pd.read_csv(TSE_CSV_PATH,encoding=enc,dtype=str)
            print(f'✅ CSV（{enc}）{len(df_csv)} 筆'); break
        except (UnicodeDecodeError,UnicodeError): continue
        except FileNotFoundError: raise
    if df_csv is None: raise RuntimeError(f'無法讀取 {TSE_CSV_PATH}')
    df_csv.columns=df_csv.columns.str.strip()
    df_csv['stock_id']=df_csv['stock_id'].astype(str).str.strip()
    df_csv['name']=df_csv['name'].astype(str).str.strip()
    df_csv=df_csv[df_csv['stock_id'].str.match(r'^\d{4,5}$')].copy()
    return df_csv['stock_id'].tolist(), dict(zip(df_csv['stock_id'],df_csv['name']))

def twse_prefilter(all_ids, name_map):
    print(f'\n{"="*60}')
    print(f'[Phase 0] TWSE pre-filter（成交額≥{PREFILTER_TURNOVER/1e8:.1f}億 + 股價≥{A_PRICE_MIN}）')
    print(f'{"="*60}')
    hdr={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    twse_df=None
    try:
        print('  TWSE OpenAPI...', end=' ', flush=True)
        r=requests.get('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',headers=hdr,timeout=30)
        if r.status_code==200:
            data=r.json()
            if isinstance(data,list) and len(data)>100:
                twse_df=pd.DataFrame(data)
                print(f'✅ {len(twse_df)} 筆')
    except Exception as e:
        print(f'失敗：{e}')
    if twse_df is None:
        print('  ⚠️ 無資料，使用全部清單'); return all_ids
    def pn(s):
        try: return float(str(s).replace(',','').strip())
        except: return 0
    sc='Code' if 'Code' in twse_df.columns else twse_df.columns[0]
    tc=next((c for c in twse_df.columns if 'TradeValue' in str(c) or '成交金額' in str(c)),None)
    cc=next((c for c in twse_df.columns if 'ClosingPrice' in str(c) or '收盤' in str(c)),None)
    if not tc or not cc:
        print('  ⚠️ 欄位缺失，使用全部清單'); return all_ids
    twse_df['sid']=twse_df[sc].astype(str).str.strip()
    twse_df['tv']=twse_df[tc].apply(pn)
    twse_df['cp']=twse_df[cc].apply(pn)
    my=set(all_ids)
    mt=twse_df[twse_df['sid'].isin(my)]
    ps=mt[(mt['tv']>=PREFILTER_TURNOVER)&(mt['cp']>=A_PRICE_MIN)]
    fids=ps['sid'].tolist()
    print(f'  匹配：{len(mt)} 檔 → 通過：{len(fids)} 檔（淘汰 {len(mt)-len(fids)}）')
    if len(fids)<50:
        print(f'  ⚠️ 太少，使用全部清單'); return all_ids
    return fids

# ════════════════════════════════════════
# FinMind REST
# ════════════════════════════════════════
def fm_rest(dataset,sid,token,start=None,end=None):
    try:
        r=requests.get('https://api.finmindtrade.com/api/v4/data',
            params={'dataset':dataset,'data_id':sid,'token':token,
                    'start_date':start or START_DATE,'end_date':end or END_DATE},timeout=25)
        rj=r.json()
        if rj.get('status')==200 and rj.get('data'): return pd.DataFrame(rj['data'])
        msg=rj.get('msg','')
        if 'request' in str(msg).lower() or 'limit' in str(msg).lower():
            print(f'\n  ⚠️ 限速：{msg}')
        return None
    except Exception as e:
        log_error(f'{sid} {dataset}：{e}'); return None

# ════════════════════════════════════════
# Phase 1: 股價 + 營收
# ════════════════════════════════════════
def fetch_all_prices(ids,token):
    pd_={}; t=len(ids); bs=(t-1)//BATCH_PRICE+1
    print(f'\n[K線] {t} 檔 {bs} 批（...{token[-6:]}）')
    for i in range(0,t,BATCH_PRICE):
        b=ids[i:i+BATCH_PRICE]; bn=i//BATCH_PRICE+1
        print(f'  {bn}/{bs}...',end=' ',flush=True); ok=0
        for s in b:
            raw=fm_rest('TaiwanStockPrice',s,token)
            if raw is not None:
                p=calc_indicators(raw)
                if p is not None: pd_[s]=p; ok+=1
        print(f'✓{ok} ✗{len(b)-ok} 累計{len(pd_)}')
        if i+BATCH_PRICE<t: time.sleep(DELAY_PRICE)
    print(f'✅ K線 {len(pd_)}/{t}'); return pd_

def fetch_all_revenue(ids,token):
    fd={}; t=len(ids); bs=(t-1)//BATCH_REV+1
    rs=(TODAY-timedelta(days=400)).strftime('%Y-%m-%d')
    print(f'\n[營收] {t} 檔（...{token[-6:]}）')
    for i in range(0,t,BATCH_REV):
        b=ids[i:i+BATCH_REV]; bn=i//BATCH_REV+1
        print(f'  {bn}/{bs}...',end=' ',flush=True); ok=0
        for s in b:
            raw=fm_rest('TaiwanStockMonthRevenue',s,token,start=rs); yoy=None
            if raw is not None and not raw.empty:
                raw=raw.sort_values('date').reset_index(drop=True)
                rc=next((c for c in ['revenue','Revenue','monthly_revenue'] if c in raw.columns),None)
                if rc:
                    raw[rc]=pd.to_numeric(raw[rc],errors='coerce'); raw=raw.dropna(subset=[rc])
                    if len(raw)>=13:
                        lt,pv=raw[rc].iloc[-1],raw[rc].iloc[-13]
                        if pv>0 and not np.isnan(pv): yoy=round((lt-pv)/abs(pv)*100,1); ok+=1
            fd[s]=yoy
        print(f'有{ok} 無{len(b)-ok}')
        if i+BATCH_REV<t: time.sleep(DELAY_REV)
    print(f'✅ 營收完成'); return fd

# ════════════════════════════════════════
# Phase 2: 籌碼（★ 小批次 + 重試）
# ════════════════════════════════════════
def _to_int(x):
    try:
        s=str(x).replace(',', '').replace('+', '').replace(' ', '').replace('－', '-').strip()
        return int(s) if s and s != '--' else 0
    except Exception:
        return 0


def _fetch_t86_one_day(date_str):
    """
    抓單日全市場三大法人（T86 ALLBUT0999）。
    T86 欄位（0-based index）：
      [0]  股票代號
      [4]  外資及陸資買賣超（千股 = 張）
      [13] 投信買賣超（千股 = 張）
    回傳 DataFrame(stock_id, foreign_net, trust_net) 或空 DataFrame。
    """
    url = ('https://www.twse.com.tw/rwd/zh/fund/T86'
           f'?date={date_str}&selectType=ALLBUT0999&response=json')
    try:
        r = requests.get(url, timeout=25,
                         headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                                'AppleWebKit/537.36 Chrome/120.0'})
        j = r.json()
        data = next((j[k] for k in ['data', 'data9', 'data0'] if k in j and j[k]), None)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        ncol = len(df.columns)
        if ncol < 13:
            return pd.DataFrame()
        df.columns = list(range(ncol))

        trust_col = 13 if ncol >= 24 else (10 if ncol >= 11 else 7)

        result = pd.DataFrame({
            'stock_id': df[0].astype(str).str.strip().str.zfill(4),
            'foreign_net': df[4].apply(_to_int),
            'trust_net': df[trust_col].apply(_to_int),
        })
        return (result[result['stock_id'].str.match(r'^\d{4}$')]
                .reset_index(drop=True))
    except Exception as e:
        log_error(f'T86 {date_str}：{e}')
        return pd.DataFrame()


def _recent_trade_dates(n_days=30, max_back_days=120):
    ds = []
    d = datetime.today()
    checked = 0
    while len(ds) < n_days and checked < max_back_days:
        s = d.strftime('%Y%m%d')
        if not _fetch_t86_one_day(s).empty:
            ds.append(s)
        d -= timedelta(days=1)
        checked += 1
        time.sleep(0.2)
    return sorted(ds)


def fetch_all_inst(ids, token):
    """
    籌碼抓取：改用 TWSE T86 全市場資料。
    token 參數保留但不使用，維持介面相容。
    """
    EMPTY = {'foreign_consec': 0, 'trust_consec': 0,
             'foreign_today': 0.0, 'trust_today': 0.0,
             'foreign_3d': 0.0, 'trust_3d': 0.0}
    inst = {s: dict(EMPTY) for s in ids}

    trade_dates = _recent_trade_dates(30, 120)
    print(f'\n[籌碼抓取] TWSE T86 模式（{len(trade_dates)} 天 × 全市場，不耗 Token）')
    if not trade_dates:
        print('  ⚠️  T86 無資料，籌碼欄位維持 0')
        return inst

    daily_inst = {}
    for i, dt in enumerate(trade_dates, 1):
        df_d = _fetch_t86_one_day(dt)
        if not df_d.empty:
            daily_inst[dt] = df_d
        if i % 10 == 0 or i == len(trade_dates):
            print(f'  已抓 {i}/{len(trade_dates)} 天  有效 {len(daily_inst)} 天', flush=True)
        time.sleep(0.25)

    if not daily_inst:
        print('  ⚠️  T86 全部無資料，籌碼欄位維持 0')
        return inst

    sorted_dates = sorted(daily_inst.keys())
    print(f'  T86 資料範圍：{sorted_dates[0]} → {sorted_dates[-1]}')

    for sid in ids:
        fvals = []
        tvals = []
        for dt in sorted_dates:
            ddf = daily_inst[dt]
            hit = ddf[ddf['stock_id'] == sid]
            if hit.empty:
                fvals.append(0)
                tvals.append(0)
            else:
                fvals.append(int(hit['foreign_net'].iloc[0]))
                tvals.append(int(hit['trust_net'].iloc[0]))

        inst[sid] = {
            'foreign_consec': consec_buy_days(pd.Series(fvals)),
            'trust_consec': consec_buy_days(pd.Series(tvals)),
            'foreign_today': float(fvals[-1]) if fvals else 0.0,
            'trust_today': float(tvals[-1]) if tvals else 0.0,
            'foreign_3d': float(sum(fvals[-3:])) if fvals else 0.0,
            'trust_3d': float(sum(tvals[-3:])) if tvals else 0.0,
        }

    print(f'  ✅ 籌碼完成：{len(ids)} 檔')
    return inst
def compute_limit_flag(df):
    if len(df)<A_LIMIT_DAYS: return False
    r=df.tail(A_LIMIT_DAYS)['daily_return'].fillna(0)
    return all(v>=A_LIMIT_THRESHOLD for v in r) or all(v<=-A_LIMIT_THRESHOLD for v in r)

def module_a(r):
    return (r.get('vol_ma5',0)>A_VOL_MA5_MIN and r.get('turnover_today',0)>A_TURNOVER_MIN
            and r.get('close',0)>A_PRICE_MIN and not r.get('limit_flag',False))

def module_b(r):
    sigs=[]; cl=r.get('close',0); vr=r.get('vol_ratio',0); dp=r.get('daily_return',0)*100
    if not(B1_VOL_RATIO_MIN<=vr<=B1_VOL_RATIO_MAX): return False,[]
    if not(EW_RETURN_MIN<=dp<=B2_RETURN_MAX_PCT): return False,[]
    if vr>=B1_VOL_RATIO_MIN: sigs.append(f'爆量{vr:.1f}x')
    if cl>=(r.get('high20') or 1e9) and r.get('daily_return',0)>B2_RETURN_MIN: sigs.append('20日新高')
    m5,m28=r.get('MA5',0) or 0,r.get('MA28',0) or 0
    if cl>m28>0 and cl>m5>0: sigs.append('多頭排列')
    h,l,o=r.get('high',0),r.get('low',0),r.get('open',0); hl=h-l
    if cl>o and hl>0 and cl>=l+hl*B4_CLOSE_RATIO: sigs.append('強勢紅K')
    return len(sigs)>=B_PASS_COUNT, sigs

def module_c(sid,inst):
    info=inst.get(sid,{}); sigs=[]
    for tag,ck,tk in [('外資','foreign_consec','foreign_today'),('投信','trust_consec','trust_today')]:
        c,t=info.get(ck,0),info.get(tk,0)
        if c>=C_CONSEC_DAYS_MIN: sigs.append(f'{tag}連買{c}天')
        elif t>=C_SINGLE_MIN: sigs.append(f'{tag}買超{int(t)}張')
    return len(sigs)>=1, sigs

def module_d(r):
    rsi=r.get('RSI14',0); ret=r.get('daily_return',0)
    mc=r.get('MACD_hist',None); mp=r.get('MACD_hist_prev',None)
    if rsi>=D_RSI_MAX and ret>=D_RETURN_MAX: return False
    if mc is not None and mp is not None and not np.isnan(mc) and not np.isnan(mp):
        if mc<-0.5 and mc<mp: return False
    return True

# ════════════════════════════════════════
# 強勢確認
# ════════════════════════════════════════
def run_strong_filter(pd_,inst,fin,nm):
    cands=[]
    for sid,df in pd_.items():
        if df is None or df.empty: continue
        last=df.iloc[-1].to_dict()
        vm5=last.get('vol_ma5',0) or 0
        last['vol_ratio']=(last.get('volume',0)/vm5) if vm5>0 else 0
        last['turnover_today']=last.get('turnover',0) or 0
        last['limit_flag']=compute_limit_flag(df)
        ma28=last.get('MA28',0) or 0
        mb=((last['close']-ma28)/ma28*100) if ma28>0 else 0
        if mb>EW_MA28_BIAS_MAX: continue
        if not module_a(last): continue
        bok,bsig=module_b(last)
        if not bok: continue
        cok,csig=module_c(sid,inst)
        if not cok: continue
        if not module_d(last): continue
        p60=0.0
        if len(df)>=61:
            c60=df['close'].iloc[-61]; p60=((last['close']-c60)/c60*100) if c60>0 else 0
        if p60>EW_PAST60D_MAX and mb>EW_PAST60D_BIAS: continue
        info=inst.get(sid,{})
        ic=info.get('foreign_consec',0)+info.get('trust_consec',0)
        icw=ic*INST_CONSEC_WEIGHT
        dp=last.get('daily_return',0)*100
        h20=1.0 if last['close']>=(last.get('high20') or 1e9) else 0.0
        sc=last['vol_ratio']*W_VOL_RATIO+h20*W_HIGH20+mb*W_MA28_BIAS+icw*W_INST_DAYS+dp*W_RETURN_PCT
        cands.append({'stock_id':sid,'name':nm.get(sid,sid),'score':round(sc,2),
            'close':last['close'],'vol_ratio':round(last['vol_ratio'],2),
            'ma28_bias':round(mb,2),'daily_return_pct':round(dp,2),
            'rsi14':round(last.get('RSI14',0) or 0,1),'inst_consec':ic,
            'foreign_today':info.get('foreign_today',0),'trust_today':info.get('trust_today',0),
            'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0),
            'yoy_revenue_pct':fin.get(sid,None),
            '_vr':last['vol_ratio'],'_mb':mb,'_ic':float(icw),'_dp':dp,'_h20':h20})
    if len(cands)>=2:
        vz=safe_zscore([c['_vr'] for c in cands]); mz=safe_zscore([c['_mb'] for c in cands])
        iz=safe_zscore([c['_ic'] for c in cands]); dz=safe_zscore([c['_dp'] for c in cands])
        hz=safe_zscore([c['_h20'] for c in cands])
        ws=W_VOL_RATIO+W_HIGH20+W_MA28_BIAS+W_INST_DAYS+W_RETURN_PCT
        for i,c in enumerate(cands):
            z=W_VOL_RATIO/ws*vz[i]+W_HIGH20/ws*mz[i]+W_MA28_BIAS/ws*iz[i]+W_INST_DAYS/ws*dz[i]+W_RETURN_PCT/ws*hz[i]
            c['total_score']=round(c['score']+float(z),2)
    else:
        for c in cands: c['total_score']=c['score']
    for c in cands:
        if c.get('ma28_bias',0)>35: c['total_score']-=18
        elif c.get('ma28_bias',0)>25: c['total_score']-=10
        if c.get('daily_return_pct',0)>9.5: c['total_score']-=12
        if c.get('rsi14',0)>78: c['total_score']-=8
        c['total_score']=round(max(c['total_score'],0),2)
    sdf=pd.DataFrame(cands).sort_values('total_score',ascending=False).reset_index(drop=True)
    if not sdf.empty: sdf.insert(0,'rank',range(1,len(sdf)+1))
    print(f'\n【強勢確認】{len(cands)} 檔'); return sdf,cands

# ════════════════════════════════════════
# 起漲預警
# ════════════════════════════════════════
def run_early_filter(pd_,inst,fin,nm):
    cands=[]
    for sid,df in pd_.items():
        if df is None or len(df)<30: continue
        last=df.iloc[-1].to_dict()
        vm5=last.get('vol_ma5',0) or 0
        if vm5<=0: continue
        vr=last.get('volume',0)/vm5; dp=last.get('daily_return',0)*100
        cl=last.get('close',0); m20=last.get('MA20',0) or 0; m28=last.get('MA28',0) or 0
        mb=((cl-m28)/m28*100) if m28>0 else 0
        td=last.get('turnover',0) or 0; rsi=last.get('RSI14',0) or 0
        a10=df['amplitude'].tail(10).mean(); a20=df['amplitude'].tail(20).mean()
        cons=(a10/a20) if a20>0 else 999
        p60=0.0
        if len(df)>=61:
            c60d=df['close'].iloc[-61]; p60=((cl-c60d)/c60d*100) if c60d>0 else 0
        if td<EW_TURNOVER_MIN: continue
        if not(EW_VOL_RATIO_MIN<=vr<=EW_VOL_RATIO_MAX): continue
        if not(EW_RETURN_MIN<=dp<=EW_RETURN_MAX): continue
        if mb>EW_MA28_BIAS_MAX: continue
        if cons>=EW_CONSOL_RATIO: continue
        if p60>EW_PAST60D_MAX and mb>EW_PAST60D_BIAS: continue
        r20=df['daily_return'].tail(20)*100
        if r20.max()>=EW_MAX20D_RET_MAX: continue
        t7=df.tail(7); bm=(t7['close']<=t7['MA20']).sum()
        if bm<EW_ABOVE_MA20_MIN: continue
        if cl<m20*0.975: continue
        info=inst.get(sid,{})
        ft=info.get('foreign_today',0); tt=info.get('trust_today',0)
        ic=info.get('foreign_consec',0)+info.get('trust_consec',0)
        ftn=ft/1000 if abs(ft)>1e6 else ft; yoy=fin.get(sid,None)
        vs_=vr*15; cs_=max(0,(1.20-cons))*14
        isc_=(1 if ic>=2 or ftn>=80 else 0)*18
        ew=0.35*vs_+0.30*cs_+0.35*isc_; bt=8.0
        if yoy is not None and not pd.isna(yoy):
            yv=float(yoy)
            if yv>80: bt+=EW_BONUS_YOY
            elif yv>30: bt+=EW_BONUS_YOY*0.7
            else: bt+=EW_BONUS_YOY*0.4
        if ic>=2 or ftn>80: bt+=EW_BONUS_INST
        if p60<25: bt+=EW_BONUS_60D
        ew+=bt
        if dp>6.5: ew-=8
        if mb>18.5: ew-=9
        elif mb>15: ew-=5
        tew=round(max(ew,0),2)
        cands.append({'stock_id':sid,'name':nm.get(sid,sid),'total_ew_score':tew,'ew_score':tew,
            'close':cl,'vol_ratio':round(vr,2),'ma28_bias':round(mb,2),
            'daily_return_pct':round(dp,2),'rsi14':round(rsi,1),'inst_consec_days':ic,
            'foreign_today':ft,'trust_today':tt,
            'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0),
            'yoy_revenue_pct':yoy})
    if cands:
        edf=pd.DataFrame(cands).sort_values('total_ew_score',ascending=False).reset_index(drop=True)
        edf.insert(0,'rank',range(1,len(edf)+1))
    else: edf=pd.DataFrame()
    print(f'【起漲預警】{len(cands)} 檔'); return edf,cands

# ════════════════════════════════════════
# CSV
# ════════════════════════════════════════
def export_csv(pd_,inst,fin,nm,sdf,edf):
    ss=set(sdf['stock_id'].tolist()) if not sdf.empty else set()
    es=set(edf['stock_id'].tolist()) if not edf.empty else set()
    ssm=({r['stock_id']:r.get('total_score',0) for _,r in sdf.iterrows()} if not sdf.empty else {})
    esm=({r['stock_id']:r.get('ew_score',0) for _,r in edf.iterrows()} if not edf.empty else {})
    rows=[]
    for sid,df in pd_.items():
        if df is None or df.empty: continue
        last=df.iloc[-1].to_dict(); vm5=last.get('vol_ma5',0) or 0; cl=last.get('close',0)
        m28=last.get('MA28',0) or 0; vr=(last.get('volume',0)/vm5) if vm5>0 else 0
        dp=last.get('daily_return',0)*100; mb=((cl-m28)/m28*100) if m28>0 else 0
        td=last.get('turnover',0) or 0; info=inst.get(sid,{})
        is_s=sid in ss; is_e=sid in es; ts=ssm.get(sid,0); ep=esm.get(sid,0)
        try:
            tsf=float(ts) if ts else 0; esf=float(ep) if ep else 0
            if is_s and is_e: comp=round(esf*COMPOSITE_EARLY_W+tsf*COMPOSITE_TOTAL_W,2)
            elif is_s: comp=round(tsf*COMPOSITE_TOTAL_W,2)
            elif is_e: comp=round(esf*COMPOSITE_EARLY_W,2)
            else: comp=0.0
        except: comp=0.0
        rj=[]
        if not(B1_VOL_RATIO_MIN<=vr<=B1_VOL_RATIO_MAX): rj.append(f'量比{vr:.2f}')
        if td<=A_TURNOVER_MIN: rj.append('成交額不足')
        if mb>EW_MA28_BIAS_MAX: rj.append(f'乖離{mb:.1f}%')
        ej=[]
        if not(EW_VOL_RATIO_MIN<=vr<=EW_VOL_RATIO_MAX): ej.append(f'量比{vr:.2f}')
        if not(EW_RETURN_MIN<=dp<=EW_RETURN_MAX): ej.append(f'漲幅{dp:.1f}%')
        if mb>EW_MA28_BIAS_MAX: ej.append(f'乖離{mb:.1f}%')
        if td<EW_TURNOVER_MIN: ej.append('成交額不足')
        rows.append({
            'stock_id':sid,'name':nm.get(sid,sid),'close':round(cl,2),'vol_ratio':round(vr,2),
            'daily_return_pct':round(dp,2),'ma28_bias_pct':round(mb,2),
            'turnover_億':round(td/1e8,2),'rsi14':round(last.get('RSI14',0) or 0,1),
            'inst_consec_days':info.get('foreign_consec',0)+info.get('trust_consec',0),
            'yoy_revenue_pct':fin.get(sid,None),
            'foreign_today':info.get('foreign_today',0),'trust_today':info.get('trust_today',0),
            'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0),
            'is_strong_confirm':is_s,'is_early_breakout':is_e,
            'total_score':ts if ts else 0,'early_score':ep if ep else 0,
            'composite_score':comp,
            'reject_reason':'；'.join(rj) if rj else ('通過' if is_s else '未通過'),
            'early_reject_reason':'；'.join(ej) if ej else ('通過' if is_e else '未通過'),
        })
    full=pd.DataFrame(rows).sort_values('composite_score',ascending=False).reset_index(drop=True)
    full.insert(0,'rank',range(1,len(full)+1))
    cols=['rank','stock_id','name','close','vol_ratio','daily_return_pct','ma28_bias_pct',
          'turnover_億','rsi14','inst_consec_days','yoy_revenue_pct','foreign_today',
          'trust_today','foreign_3d','trust_3d','is_strong_confirm','is_early_breakout',
          'total_score','early_score','composite_score','reject_reason','early_reject_reason']
    full=full[cols]
    os.makedirs('output',exist_ok=True)
    fn=f'output/Twgogogo_{TODAY_STR}.csv'
    full.to_csv(fn,index=False,encoding='utf-8-sig')
    print(f'\n✅ {fn}（{len(full)} 筆）強勢{full["is_strong_confirm"].sum()} 預警{full["is_early_breakout"].sum()}')
    return fn,full

# ════════════════════════════════════════
# 通知
# ════════════════════════════════════════
def send_telegram(sdf,edf,sc,ec,ns):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: print('⚠️ 無Telegram'); return
    lines=[f"📊 *台股GOGOGO v2.0 — {TODAY_DISP}*","",f"掃描{ns}檔 強勢{sc} 預警{ec}",""]
    if not sdf.empty:
        lines.append("*🔥 強勢Top5:*")
        for _,r in sdf.head(5).iterrows():
            lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} {r['total_score']:.1f}分")
        lines.append("")
    if not edf.empty:
        lines.append("*🌱 預警Top5:*")
        for _,r in edf.head(5).iterrows():
            lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} {r['total_ew_score']:.1f}分")
    if GITHUB_PAGES_URL: lines.append(f"\n🌐 [報告]({GITHUB_PAGES_URL})")
    try:
        resp=requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={'chat_id':TELEGRAM_CHAT_ID,'text':'\n'.join(lines),'parse_mode':'Markdown'},timeout=15)
        print('✅ TG已發送' if resp.status_code==200 else f'⚠️{resp.text}')
    except Exception as e: print(f'⚠️TG:{e}')

def send_email(csv_fn,sdf,edf,sc,ec,ns):
    if not GMAIL_USER or not GMAIL_APP_PASS or not EMAIL_TO: print('⚠️ 無Email'); return
    msg=MIMEMultipart('mixed')
    msg['Subject']=f'台股GOGOGO {TODAY_DISP} 強勢{sc} 預警{ec}'
    msg['From']=GMAIL_USER; msg['To']=EMAIL_TO
    body=f'台股GOGOGO v2.0 {TODAY_DISP}\n掃描{ns}檔 強勢{sc} 預警{ec}'
    if GITHUB_PAGES_URL: body+=f'\n報告：{GITHUB_PAGES_URL}'
    msg.attach(MIMEText(body,'plain','utf-8'))
    if os.path.exists(csv_fn):
        with open(csv_fn,'rb') as f:
            part=MIMEBase('application','octet-stream'); part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition','attachment',filename=os.path.basename(csv_fn))
        msg.attach(part)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com',465) as s:
            s.login(GMAIL_USER,GMAIL_APP_PASS); s.sendmail(GMAIL_USER,EMAIL_TO.split(','),msg.as_string())
        print('✅ Email已發送')
    except Exception as e: print(f'⚠️Email:{e}')

# ════════════════════════════════════════
# main
# ════════════════════════════════════════
def main():
    print("="*60)
    print("台股GOGOGO 上市專用模型 v2.0")
    print("="*60)
    all_ids,nm=load_stock_list()
    fids=twse_prefilter(all_ids,nm)
    n=len(fids)
    print(f'\n[Token分配] {n} 檔')
    print(f'  Token1: 股價({n+1})+營收({n})={n*2+1}/600')
    print(f'  Token2: 籌碼({n})={n}/600')
    pd_=fetch_all_prices(fids,FINMIND_TOKEN_1)
    vids=list(pd_.keys())
    fin=fetch_all_revenue(vids,FINMIND_TOKEN_1)
    inst=fetch_all_inst(vids,FINMIND_TOKEN_2)
    sdf,sc=run_strong_filter(pd_,inst,fin,nm)
    edf,ec=run_early_filter(pd_,inst,fin,nm)
    csv_fn,full=export_csv(pd_,inst,fin,nm,sdf,edf)
    ns=len(pd_)
    send_telegram(sdf,edf,len(sc),len(ec),ns)
    send_email(csv_fn,sdf,edf,len(sc),len(ec),ns)
    print("\n"+"="*60)
    print(f"✅ Twgogogo_{TODAY_STR}.csv 已產生")
    print("="*60)

if __name__=='__main__':
    main()
