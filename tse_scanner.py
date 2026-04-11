"""
台股GOGOGO 上市專用模型 v3.0 — 正式版
計分規則 v3.0：量比下限1.5、60日過熱>40%+MA28>20%、YoY>15%加15分、量價×1.1
配色：暗紫科技 + 橘金標題
Phase 0: TWSE OpenAPI pre-filter（≥2.5億）
Phase 1: FinMind Token1 → 股價+營收
Phase 2: TWSE T86 → 籌碼（免Token）
輸出: Twgogogo_YYYYMMDD.csv + TSE_report_YYYYMMDD.html
"""
import subprocess,sys,os,time,warnings,json,smtplib,base64,io
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
import requests
from scipy import stats as scipy_stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib as mpl
import mplfinance as mpf
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',30)

FINMIND_TOKEN_1=os.environ.get("FINMIND_TOKEN_1","") or os.environ.get("FINMIND_TOKEN","")
TELEGRAM_TOKEN=os.environ.get("TELEGRAM_TOKEN","")
TELEGRAM_CHAT_ID=os.environ.get("TELEGRAM_CHAT_ID","")
GMAIL_USER=os.environ.get("GMAIL_USER","")
GMAIL_APP_PASS=os.environ.get("GMAIL_APP_PASS","")
EMAIL_TO=os.environ.get("EMAIL_TO","")
GITHUB_PAGES_URL=os.environ.get("REPORT_URL","")
TSE_CSV_PATH=os.environ.get("TSE_CSV_PATH","stock_list.csv")

# ═══ v3.0 計分常數 ═══
PREFILTER_TURNOVER=250_000_000
A_VOL_MA5_MIN=800; A_TURNOVER_MIN=250_000_000; A_PRICE_MIN=10
A_LIMIT_DAYS=3; A_LIMIT_THRESHOLD=0.095
# ★ v3.0: 量比下限 1.3→1.5
B1_VOL_RATIO_MIN=1.5; B1_VOL_RATIO_MAX=4.0; B2_RETURN_MIN=0.01
B2_RETURN_MAX_PCT=9.0; B4_CLOSE_RATIO=0.65; B_PASS_COUNT=2
C_CONSEC_DAYS_MIN=3; C_SINGLE_MIN=200
D_RSI_MAX=78; D_RETURN_MAX=0.10
# ★ v3.0: 量價係數調整（vol×1.8, h20×1.3, bias×1.1, ret×0.9），整體×1.1
W_VOL_RATIO=1.8; W_HIGH20=1.3; W_MA28_BIAS=1.1; W_RETURN_PCT=0.9
W_PRICE_BOOST=1.1  # ★ 量價子分數額外×1.1
W_INST_DAYS=2.2    # 籌碼不乘1.1
# ★ v3.0: 起漲量比也跟著改1.5
EW_VOL_RATIO_MIN=1.5; EW_VOL_RATIO_MAX=4.0; EW_RETURN_MAX=9.0; EW_RETURN_MIN=-2.0
EW_MA28_BIAS_MAX=25.0; EW_CONSOL_RATIO=1.12; EW_TURNOVER_MIN=250_000_000
EW_ABOVE_MA20_MIN=0; EW_MAX20D_RET_MAX=20.0; EW_INST_MIN=80
# ★ v3.0: 60日過熱 45%→40%, MA28 25%→20%
EW_PAST60D_MAX=40.0; EW_PAST60D_BIAS=20.0
# ★ v3.0: YoY門檻 20%→15%, 加分 18→15
EW_BONUS_YOY=15.0; EW_BONUS_YOY_THRESHOLD=15.0
EW_BONUS_INST=24.0; EW_BONUS_60D=22.0
COMPOSITE_EARLY_W=0.52; COMPOSITE_TOTAL_W=0.48; INST_CONSEC_WEIGHT=2.2
MIN_DAYS=60; ERROR_LOG="error_log.txt"
TOP_STRONG=10; TOP_EARLY=10; TOP_COMPOSITE=10; TOP_CHART=5
BATCH_PRICE=40; DELAY_PRICE=1.5; BATCH_REV=40; DELAY_REV=1.5

TODAY=datetime.today()
END_DATE=TODAY.strftime('%Y-%m-%d')
START_DATE=(TODAY-timedelta(days=400)).strftime('%Y-%m-%d')
TODAY_STR=TODAY.strftime('%Y%m%d')
TODAY_DISP=TODAY.strftime('%Y/%m/%d')
print(f"[台股GOGOGO v3.0] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[系統] {START_DATE} → {END_DATE}")

def init_chinese_font():
    mpl.rcParams['axes.unicode_minus']=False
    for p in ['/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc','/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc','/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc']:
        if os.path.exists(p):
            try:
                prop=fm.FontProperties(fname=p); n=prop.get_name()
                mpl.rcParams['font.sans-serif']=[n,'DejaVu Sans']; fm.fontManager.addfont(p)
                print(f'  ✅ 字型：{n}'); return p,prop
            except: pass
    print('  ⚠️ 無中文字型'); return None,None

def log_error(msg):
    with open(ERROR_LOG,'a',encoding='utf-8') as f: f.write(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}\n')

def calc_rsi(s,p=14):
    d=s.diff(); g=d.clip(lower=0); l=(-d).clip(lower=0)
    ag=g.ewm(alpha=1/p,adjust=False).mean(); al=l.ewm(alpha=1/p,adjust=False).mean()
    return 100-(100/(1+ag/al.replace(0,np.nan)))

def calc_macd(s,f=12,sl=26,sg=9):
    m=s.ewm(span=f,adjust=False).mean()-s.ewm(span=sl,adjust=False).mean()
    return m-m.ewm(span=sg,adjust=False).mean()

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
    for c in ['open','high','low','close','volume']: df[c]=pd.to_numeric(df[c],errors='coerce')
    df=df.dropna(subset=['close','volume'])
    if len(df)<MIN_DAYS: return None
    if 'turnover' in df.columns:
        df['turnover']=pd.to_numeric(df['turnover'],errors='coerce').fillna(0)
        m=df['turnover']<=0; df.loc[m,'turnover']=df.loc[m,'close']*df.loc[m,'volume']*1000
    else: df['turnover']=df['close']*df['volume']*1000
    df['MA5']=df['close'].rolling(5).mean(); df['MA20']=df['close'].rolling(20).mean()
    df['MA28']=df['close'].rolling(28).mean(); df['vol_ma5']=df['volume'].rolling(5).mean()
    df['high20']=df['high'].rolling(20).max(); df['daily_return']=df['close'].pct_change()
    df['RSI14']=calc_rsi(df['close'],14); h=calc_macd(df['close'])
    df['MACD_hist']=h; df['MACD_hist_prev']=h.shift(1); df['amplitude']=df['high']-df['low']
    return df

def fig_to_base64(fig):
    buf=io.BytesIO(); fig.savefig(buf,format='png',dpi=120,bbox_inches='tight',facecolor='#0a0a12')
    buf.seek(0); return base64.b64encode(buf.read()).decode('utf-8')

# ═══ Phase 0: TWSE pre-filter ═══
def load_stock_list():
    df_csv=None
    for enc in ['utf-8-sig','utf-8','cp950','big5','latin1']:
        try: df_csv=pd.read_csv(TSE_CSV_PATH,encoding=enc,dtype=str); print(f'✅ CSV（{enc}）{len(df_csv)} 筆'); break
        except (UnicodeDecodeError,UnicodeError): continue
        except FileNotFoundError: raise
    if df_csv is None: raise RuntimeError(f'無法讀取 {TSE_CSV_PATH}')
    df_csv.columns=df_csv.columns.str.strip()
    df_csv['stock_id']=df_csv['stock_id'].astype(str).str.strip()
    df_csv['name']=df_csv['name'].astype(str).str.strip()
    df_csv=df_csv[df_csv['stock_id'].str.match(r'^\d{4,5}$')].copy()
    return df_csv['stock_id'].tolist(), dict(zip(df_csv['stock_id'],df_csv['name']))

def twse_prefilter(all_ids, name_map):
    print(f'\n[Phase 0] TWSE pre-filter（≥{PREFILTER_TURNOVER/1e8:.1f}億）')
    hdr={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    twse_df=None
    try:
        r=requests.get('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',headers=hdr,timeout=30)
        if r.status_code==200:
            data=r.json()
            if isinstance(data,list) and len(data)>100: twse_df=pd.DataFrame(data); print(f'  ✅ {len(twse_df)} 筆')
    except Exception as e: print(f'  失敗：{e}')
    if twse_df is None: print('  ⚠️ 無資料，用全清單'); return all_ids
    def pn(s):
        try: return float(str(s).replace(',','').strip())
        except: return 0
    sc='Code' if 'Code' in twse_df.columns else twse_df.columns[0]
    tc=next((c for c in twse_df.columns if 'TradeValue' in str(c) or '成交金額' in str(c)),None)
    cc=next((c for c in twse_df.columns if 'ClosingPrice' in str(c) or '收盤' in str(c)),None)
    if not tc or not cc: return all_ids
    twse_df['sid']=twse_df[sc].astype(str).str.strip(); twse_df['tv']=twse_df[tc].apply(pn); twse_df['cp']=twse_df[cc].apply(pn)
    mt=twse_df[twse_df['sid'].isin(set(all_ids))]
    ps=mt[(mt['tv']>=PREFILTER_TURNOVER)&(mt['cp']>=A_PRICE_MIN)]
    fids=ps['sid'].tolist()
    print(f'  匹配{len(mt)}→通過{len(fids)}（淘汰{len(mt)-len(fids)}）')
    return fids if len(fids)>=50 else all_ids

# ═══ FinMind REST ═══
def fm_rest(dataset,sid,token,start=None,end=None):
    try:
        r=requests.get('https://api.finmindtrade.com/api/v4/data',
            params={'dataset':dataset,'data_id':sid,'token':token,
                    'start_date':start or START_DATE,'end_date':end or END_DATE},timeout=25)
        rj=r.json()
        if rj.get('status')==200 and rj.get('data'): return pd.DataFrame(rj['data'])
        return None
    except Exception as e: log_error(f'{sid} {dataset}：{e}'); return None

# ═══ Phase 1: 股價+營收 ═══
def fetch_all_prices(ids,token):
    pd_={}; t=len(ids); bs=(t-1)//BATCH_PRICE+1
    print(f'\n[K線] {t} 檔 {bs} 批')
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
    print(f'\n[營收] {t} 檔')
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

# ═══ Phase 2: 籌碼 TWSE T86 ═══
def _to_int(x):
    try:
        s=str(x).replace(',','').replace('+','').replace(' ','').replace('－','-').strip()
        return int(s) if s and s!='--' else 0
    except: return 0

def _fetch_t86_one_day(date_str):
    url=f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=json'
    try:
        r=requests.get(url,timeout=25,headers={'User-Agent':'Mozilla/5.0'})
        j=r.json(); data=next((j[k] for k in ['data','data9','data0'] if k in j and j[k]),None)
        if not data: return pd.DataFrame()
        df=pd.DataFrame(data); nc=len(df.columns)
        if nc<13: return pd.DataFrame()
        df.columns=list(range(nc)); tc=13 if nc>=24 else(10 if nc>=11 else 7)
        result=pd.DataFrame({'stock_id':df[0].astype(str).str.strip().str.zfill(4),
            'foreign_net':df[4].apply(_to_int),'trust_net':df[tc].apply(_to_int)})
        return result[result['stock_id'].str.match(r'^\d{4}$')].reset_index(drop=True)
    except: return pd.DataFrame()

def fetch_all_inst(ids, token_unused):
    EMPTY={'foreign_consec':0,'trust_consec':0,'foreign_today':0.0,'trust_today':0.0,'foreign_3d':0.0,'trust_3d':0.0}
    inst={s:dict(EMPTY) for s in ids}
    print(f'\n[籌碼] TWSE T86（免Token）')
    trade_dates=[]; d=datetime.today(); checked=0
    while len(trade_dates)<30 and checked<60:
        s=d.strftime('%Y%m%d')
        if d.weekday()<5:
            df_t=_fetch_t86_one_day(s)
            if not df_t.empty: trade_dates.append(s)
            time.sleep(0.3)
        d-=timedelta(days=1); checked+=1
        if len(trade_dates)%5==0 and trade_dates: print(f'  已找{len(trade_dates)}天...',flush=True)
    trade_dates=sorted(trade_dates)
    if not trade_dates: print('  ⚠️ T86無資料'); return inst
    print(f'  T86 {len(trade_dates)}天：{trade_dates[0]}→{trade_dates[-1]}')
    daily_inst={}
    for dt in trade_dates:
        df_d=_fetch_t86_one_day(dt)
        if not df_d.empty: daily_inst[dt]=df_d
        time.sleep(0.25)
    sd=sorted(daily_inst.keys())
    for sid in ids:
        fv=[]; tv=[]
        for dt in sd:
            ddf=daily_inst.get(dt,pd.DataFrame()); hit=ddf[ddf['stock_id']==sid] if not ddf.empty else pd.DataFrame()
            fv.append(int(hit['foreign_net'].iloc[0]) if not hit.empty else 0)
            tv.append(int(hit['trust_net'].iloc[0]) if not hit.empty else 0)
        inst[sid]={'foreign_consec':consec_buy_days(pd.Series(fv)),'trust_consec':consec_buy_days(pd.Series(tv)),
            'foreign_today':float(fv[-1]) if fv else 0,'trust_today':float(tv[-1]) if tv else 0,
            'foreign_3d':float(sum(fv[-3:])) if fv else 0,'trust_3d':float(sum(tv[-3:])) if tv else 0}
    print(f'  ✅ 籌碼完成 {len(ids)} 檔'); return inst

# ═══ 篩選模組 ═══
def compute_limit_flag(df):
    if len(df)<A_LIMIT_DAYS: return False
    r=df.tail(A_LIMIT_DAYS)['daily_return'].fillna(0)
    return all(v>=A_LIMIT_THRESHOLD for v in r) or all(v<=-A_LIMIT_THRESHOLD for v in r)
def module_a(r):
    return r.get('vol_ma5',0)>A_VOL_MA5_MIN and r.get('turnover_today',0)>A_TURNOVER_MIN and r.get('close',0)>A_PRICE_MIN and not r.get('limit_flag',False)
def module_b(r):
    sigs=[]; vr=r.get('vol_ratio',0); dp=r.get('daily_return',0)*100; cl=r.get('close',0)
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
    if r.get('RSI14',0)>=D_RSI_MAX and r.get('daily_return',0)>=D_RETURN_MAX: return False
    mc,mp=r.get('MACD_hist',None),r.get('MACD_hist_prev',None)
    if mc is not None and mp is not None and not np.isnan(mc) and not np.isnan(mp):
        if mc<-0.5 and mc<mp: return False
    return True

# ═══ 強勢確認 ═══
def run_strong_filter(pd_,inst,fin,nm):
    cands=[]
    for sid,df in pd_.items():
        if df is None or df.empty: continue
        last=df.iloc[-1].to_dict(); vm5=last.get('vol_ma5',0) or 0
        last['vol_ratio']=(last.get('volume',0)/vm5) if vm5>0 else 0
        last['turnover_today']=last.get('turnover',0) or 0; last['limit_flag']=compute_limit_flag(df)
        ma28=last.get('MA28',0) or 0; mb=((last['close']-ma28)/ma28*100) if ma28>0 else 0
        if mb>EW_MA28_BIAS_MAX: continue
        if not module_a(last): continue
        bok,bsig=module_b(last)
        if not bok: continue
        cok,csig=module_c(sid,inst)
        if not cok: continue
        if not module_d(last): continue
        p60=0.0
        if len(df)>=61: c60=df['close'].iloc[-61]; p60=((last['close']-c60)/c60*100) if c60>0 else 0
        if p60>EW_PAST60D_MAX and mb>EW_PAST60D_BIAS: continue
        info=inst.get(sid,{})
        ic=info.get('foreign_consec',0)+info.get('trust_consec',0)
        dp=last.get('daily_return',0)*100
        h20=1.0 if last['close']>=(last.get('high20') or 1e9) else 0.0
        # ★ v3.0: 量價子分數 ×1.1，籌碼不乘
        price_sub = (last['vol_ratio']*W_VOL_RATIO + h20*W_HIGH20 + mb*W_MA28_BIAS + dp*W_RETURN_PCT) * W_PRICE_BOOST
        inst_sub = ic * INST_CONSEC_WEIGHT * W_INST_DAYS  # 不乘1.1
        sc = price_sub + inst_sub
        cands.append({'stock_id':sid,'name':nm.get(sid,sid),'score':round(sc,2),
            'close':last['close'],'turnover_today':last.get('turnover_today',0),
            'vol_ratio':round(last['vol_ratio'],2),'ma28_bias':round(mb,2),'daily_return_pct':round(dp,2),
            'rsi14':round(last.get('RSI14',0) or 0,1),'inst_consec':ic,
            'foreign_today':info.get('foreign_today',0),'trust_today':info.get('trust_today',0),
            'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0),
            'yoy_revenue_pct':fin.get(sid,None),'past_60d_cum':round(p60,1),
            'signal_b':' + '.join(bsig),'signal_c':' + '.join(csig),
            'strength':'強' if sc>18 else('中' if sc>=12 else '弱'),
            '_vr':last['vol_ratio'],'_mb':mb,'_ic':float(ic*INST_CONSEC_WEIGHT),'_dp':dp,'_h20':h20})
    if len(cands)>=2:
        vz=safe_zscore([c['_vr'] for c in cands]); mz=safe_zscore([c['_mb'] for c in cands])
        iz=safe_zscore([c['_ic'] for c in cands]); dz=safe_zscore([c['_dp'] for c in cands])
        hz=safe_zscore([c['_h20'] for c in cands])
        ws=W_VOL_RATIO+W_HIGH20+W_MA28_BIAS+W_RETURN_PCT+W_INST_DAYS
        for i,c in enumerate(cands):
            z=W_VOL_RATIO/ws*vz[i]+W_HIGH20/ws*mz[i]+W_MA28_BIAS/ws*iz[i]+W_RETURN_PCT/ws*dz[i]+W_INST_DAYS/ws*hz[i]
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

# ═══ 起漲預警 ═══
def run_early_filter(pd_,inst,fin,nm):
    cands=[]
    for sid,df in pd_.items():
        if df is None or len(df)<30: continue
        last=df.iloc[-1].to_dict(); vm5=last.get('vol_ma5',0) or 0
        if vm5<=0: continue
        vr=last.get('volume',0)/vm5; dp=last.get('daily_return',0)*100
        cl=last.get('close',0); m20=last.get('MA20',0) or 0; m28=last.get('MA28',0) or 0
        mb=((cl-m28)/m28*100) if m28>0 else 0; td=last.get('turnover',0) or 0; rsi=last.get('RSI14',0) or 0
        a10=df['amplitude'].tail(10).mean(); a20=df['amplitude'].tail(20).mean()
        cons=(a10/a20) if a20>0 else 999
        p60=0.0
        if len(df)>=61: c60d=df['close'].iloc[-61]; p60=((cl-c60d)/c60d*100) if c60d>0 else 0
        if td<EW_TURNOVER_MIN or not(EW_VOL_RATIO_MIN<=vr<=EW_VOL_RATIO_MAX): continue
        if not(EW_RETURN_MIN<=dp<=EW_RETURN_MAX) or mb>EW_MA28_BIAS_MAX or cons>=EW_CONSOL_RATIO: continue
        if p60>EW_PAST60D_MAX and mb>EW_PAST60D_BIAS: continue
        if df['daily_return'].tail(20).max()*100>=EW_MAX20D_RET_MAX: continue
        t7=df.tail(7)
        if (t7['close']<=t7['MA20']).sum()<EW_ABOVE_MA20_MIN or cl<m20*0.975: continue
        info=inst.get(sid,{}); ft=info.get('foreign_today',0); tt=info.get('trust_today',0)
        ic=info.get('foreign_consec',0)+info.get('trust_consec',0)
        ftn=ft/1000 if abs(ft)>1e6 else ft; yoy=fin.get(sid,None)
        ew=0.35*(vr*15)+0.30*(max(0,(1.20-cons))*14)+0.35*((1 if ic>=2 or ftn>=80 else 0)*18)
        bt=8.0
        # ★ v3.0: YoY門檻15%加15分
        if yoy is not None and not pd.isna(yoy):
            yv=float(yoy)
            if yv>80: bt+=EW_BONUS_YOY
            elif yv>EW_BONUS_YOY_THRESHOLD: bt+=EW_BONUS_YOY*0.7
            else: bt+=EW_BONUS_YOY*0.4
        if ic>=2 or ftn>80: bt+=EW_BONUS_INST
        if p60<25: bt+=EW_BONUS_60D
        ew+=bt
        if dp>6.5: ew-=8
        if mb>18.5: ew-=9
        elif mb>15: ew-=5
        tew=round(max(ew,0),2)
        cands.append({'stock_id':sid,'name':nm.get(sid,sid),'total_ew_score':tew,'ew_score':tew,
            'close':cl,'turnover_today':td,'vol_ratio':round(vr,2),'ma28_bias':round(mb,2),
            'daily_return_pct':round(dp,2),'rsi14':round(rsi,1),'consol_ratio':round(cons,2),
            'past_60d_cum':round(p60,1),'yoy_revenue_pct':yoy,'inst_consec_days':ic,
            'foreign_today':ft,'trust_today':tt,'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0)})
    if cands:
        edf=pd.DataFrame(cands).sort_values('total_ew_score',ascending=False).reset_index(drop=True)
        edf.insert(0,'rank',range(1,len(edf)+1))
    else: edf=pd.DataFrame()
    print(f'【起漲預警】{len(cands)} 檔'); return edf,cands

# ═══ K線圖 ═══
def draw_kline(sid,price_data,name_map,font_path,label=''):
    df_p=price_data.get(sid)
    if df_p is None or len(df_p)<30: return None
    df_p=df_p.tail(60).copy(); df_p['date']=pd.to_datetime(df_p['date'])
    df_p=df_p.set_index('date').rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
    for c in ['Open','High','Low','Close','Volume']:
        if c not in df_p.columns: return None
    add_plots=[]
    for ma,color,lw in [('MA5','#F5A623',1),('MA20','#4A90E2',1),('MA28','#BD10E0',1.2)]:
        if ma in df_p.columns: add_plots.append(mpf.make_addplot(df_p[ma],panel=0,color=color,width=lw))
    if 'RSI14' in df_p.columns:
        add_plots.append(mpf.make_addplot(df_p['RSI14'],panel=2,color='#E8D44D',width=1.2,ylabel='RSI',ylim=(0,100)))
    if 'MACD_hist' in df_p.columns:
        colors=['#26a641' if v>=0 else '#f85149' for v in df_p['MACD_hist'].fillna(0)]
        add_plots.append(mpf.make_addplot(df_p['MACD_hist'],panel=3,type='bar',color=colors,ylabel='MACD'))
    mc=mpf.make_marketcolors(up='#f85149',down='#26a641',edge='inherit',wick='inherit',volume={'up':'#f85149','down':'#26a641'})
    rc_font=fm.FontProperties(fname=font_path).get_name() if font_path else 'DejaVu Sans'
    style=mpf.make_mpf_style(base_mpf_style='nightclouds',marketcolors=mc,
        rc={'font.family':rc_font,'axes.labelcolor':'#c9d1d9','xtick.color':'#c9d1d9','ytick.color':'#c9d1d9'})
    name=name_map.get(sid,sid); title_str=f'  {sid} {name}  {label}' if font_path else f'  {sid}  {label}'
    try:
        fig,_=mpf.plot(df_p[['Open','High','Low','Close','Volume']],type='candle',style=style,title=title_str,
            volume=True,addplot=add_plots,panel_ratios=(4,1,1.2,1.2),figsize=(14,10),returnfig=True,warn_too_much_data=200)
        b64=fig_to_base64(fig); plt.close(fig); return b64
    except Exception as e: log_error(f'{sid} K線圖：{e}'); return None

# ═══ CSV ═══
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
        rows.append({
            'stock_id':sid,'name':nm.get(sid,sid),'close':round(cl,2),'vol_ratio':round(vr,2),
            'daily_return_pct':round(dp,2),'ma28_bias_pct':round(mb,2),'turnover_億':round(td/1e8,2),
            'rsi14':round(last.get('RSI14',0) or 0,1),
            'inst_consec_days':info.get('foreign_consec',0)+info.get('trust_consec',0),
            'yoy_revenue_pct':fin.get(sid,None),
            'foreign_today':info.get('foreign_today',0),'trust_today':info.get('trust_today',0),
            'foreign_3d':info.get('foreign_3d',0),'trust_3d':info.get('trust_3d',0),
            'is_strong_confirm':is_s,'is_early_breakout':is_e,
            'total_score':ts if ts else 0,'early_score':ep if ep else 0,'composite_score':comp,
            'reject_reason':'通過' if is_s else '未通過','early_reject_reason':'通過' if is_e else '未通過'})
    full=pd.DataFrame(rows).sort_values('composite_score',ascending=False).reset_index(drop=True)
    full.insert(0,'rank',range(1,len(full)+1))
    cols=['rank','stock_id','name','close','vol_ratio','daily_return_pct','ma28_bias_pct',
          'turnover_億','rsi14','inst_consec_days','yoy_revenue_pct','foreign_today',
          'trust_today','foreign_3d','trust_3d','is_strong_confirm','is_early_breakout',
          'total_score','early_score','composite_score','reject_reason','early_reject_reason']
    full=full[cols]; os.makedirs('output',exist_ok=True)
    fn=f'output/Twgogogo_{TODAY_STR}.csv'
    full.to_csv(fn,index=False,encoding='utf-8-sig')
    print(f'\n✅ {fn}（{len(full)} 筆）'); return fn,full

# ═══ HTML 報告（暗紫科技 + 橘金標題）═══
def export_html(price_data,inst,fin,nm,sdf,edf,sc_list,ec_list,s_charts,e_charts,c_charts,full_out):
    def fn(v,d=2):
        try: return f'{float(v):,.{d}f}'
        except: return str(v)
    def ft(v):
        try: return f'{float(v)/1e8:.2f} 億'
        except: return '-'
    def pc(v):
        try:
            f=float(v)
            if f>=5: return f'<span style="color:#fb7185;font-weight:700">{f:+.2f}%</span>'
            if f>=1: return f'<span style="color:#fbbf24">{f:+.2f}%</span>'
            if f<=-3: return f'<span style="color:#34d399">{f:+.2f}%</span>'
            return f'{f:+.2f}%'
        except: return str(v)
    def rc(v):
        try:
            f=float(v)
            if f>=78: return f'<span style="color:#fb7185;font-weight:700">{f:.1f} ⚠️</span>'
            if f>=65: return f'<span style="color:#fbbf24">{f:.1f}</span>'
            return f'{f:.1f}'
        except: return str(v)
    def fy(v):
        try:
            f=float(v)
            if f>=20: return f'<span style="color:#34d399;font-weight:700">+{f:.0f}%</span>'
            if f>=0: return f'<span style="color:#fbbf24">+{f:.0f}%</span>'
            return f'<span style="color:#fb7185">{f:.0f}%</span>'
        except: return '<span style="color:#7c7894">-</span>'
    def sb(v):
        c={'強':'#fb7185','中':'#c084fc','弱':'#7c7894'}.get(v,'#7c7894')
        return f'<span style="background:{c};color:#fff;padding:2px 10px;border-radius:12px;font-weight:700">{v}</span>'
    def build_chart_html(charts,df_ref,scol='total_score'):
        h=''
        for sid,b64 in charts.items():
            name=nm.get(sid,sid); row=df_ref[df_ref['stock_id']==sid] if not df_ref.empty else pd.DataFrame()
            cap=f'{sid} {name}' if row.empty else f'#{int(row.iloc[0]["rank"])} {sid} {name} | {fn(row.iloc[0].get(scol,0))}'
            h+=f'<div class="chart-wrap"><div class="chart-caption">{cap}</div><img src="data:image/png;base64,{b64}" style="width:90%;border-radius:6px;margin:12px auto;display:block"/></div>'
        return h
    # 強勢表格
    sr=''
    if not sdf.empty:
        for _,r in sdf.head(TOP_STRONG).iterrows():
            rk=int(r['rank']); m=['🥇','🥈','🥉'][rk-1] if rk<=3 else f'#{rk}'
            yr=fin.get(r['stock_id'],None)
            sr+=f'<tr><td style="text-align:center">{m}</td><td style="color:#fb7185;font-weight:700">{r["stock_id"]}</td><td style="font-weight:600">{r["name"]}</td>'
            sr+=f'<td><span class="badge-score" style="background:linear-gradient(135deg,#3a1520,#6b2535)">{fn(r["total_score"])}</span></td>'
            sr+=f'<td style="font-size:.82em">{r.get("signal_b","")}<br><span style="color:#7c7894">{r.get("signal_c","")}</span></td>'
            sr+=f'<td style="font-weight:600">{fn(r["close"],1)}</td><td>{ft(r["turnover_today"])}</td><td>{fn(r["vol_ratio"])}x</td>'
            sr+=f'<td>{pc(r["ma28_bias"])}</td><td>{pc(r["daily_return_pct"])}</td><td>{rc(r["rsi14"])}</td>'
            sr+=f'<td>{r["inst_consec"]}天</td><td>{sb(r["strength"])}</td><td>{fy(yr)}</td></tr>'
    else: sr='<tr><td colspan="14" style="text-align:center;color:#7c7894;padding:24px">今日無符合條件個股</td></tr>'
    # 起漲表格
    er=''
    if not edf.empty:
        for _,r in edf.head(TOP_EARLY).iterrows():
            rk=int(r['rank']); m=['🌱','🌿','🍃'][rk-1] if rk<=3 else f'#{rk}'
            er+=f'<tr><td style="text-align:center">{m}</td><td style="color:#2dd4bf;font-weight:700">{r["stock_id"]}</td><td>{r["name"]}</td>'
            er+=f'<td><span class="badge-score" style="background:linear-gradient(135deg,#0a2520,#1a5545)">{fn(r["total_ew_score"])}</span></td>'
            er+=f'<td>{fn(r["close"],1)}</td><td>{ft(r["turnover_today"])}</td><td>{fn(r["vol_ratio"])}x</td>'
            er+=f'<td>{pc(r["ma28_bias"])}</td><td>{pc(r["daily_return_pct"])}</td><td>{rc(r["rsi14"])}</td>'
            er+=f'<td>{fn(r["consol_ratio"])}</td><td>{fy(r["yoy_revenue_pct"])}</td><td>{r["inst_consec_days"]}天</td></tr>'
    else: er='<tr><td colspan="13" style="text-align:center;color:#7c7894;padding:24px">今日無起漲預警</td></tr>'
    # 綜合分表格
    comp_df=full_out[full_out['composite_score']>0].sort_values('composite_score',ascending=False).head(TOP_COMPOSITE).reset_index(drop=True)
    cr=''
    if not comp_df.empty:
        medals=['🏅','🎖️','⭐','✨','💫']
        for i,(_,r) in enumerate(comp_df.iterrows()):
            cr+=f'<tr><td style="text-align:center">{medals[i] if i<5 else "▪️"}</td>'
            cr+=f'<td style="color:#c084fc;font-weight:700">{r["stock_id"]}</td><td style="font-weight:600">{r["name"]}</td>'
            cr+=f'<td><span class="badge-composite">{fn(r["composite_score"])}</span></td>'
            cr+=f'<td>{fn(r["close"],1)}</td><td>{fn(r["vol_ratio"])}x</td><td>{pc(r["ma28_bias_pct"])}</td>'
            cr+=f'<td>{pc(r["daily_return_pct"])}</td><td>{rc(r["rsi14"])}</td><td>{fy(r["yoy_revenue_pct"])}</td>'
            cr+=f'<td>{r["inst_consec_days"]}天</td><td>{fn(r["early_score"])}</td><td>{fn(r["total_score"])}</td></tr>'
    else: cr='<tr><td colspan="13" style="text-align:center;color:#7c7894;padding:24px">無綜合分資料</td></tr>'
    top1_id=comp_df.iloc[0]['stock_id'] if not comp_df.empty else '-'
    top1_name=comp_df.iloc[0]['name'] if not comp_df.empty else ''
    top1_score=fn(comp_df.iloc[0]['composite_score']) if not comp_df.empty else '-'
    sch=build_chart_html(s_charts,sdf,'total_score') if s_charts else ''
    ech=build_chart_html(e_charts,edf,'total_ew_score') if e_charts else ''
    comp_df2=comp_df.copy()
    if not comp_df2.empty: comp_df2.insert(0,'rank',range(1,len(comp_df2)+1))
    cch=build_chart_html(c_charts,comp_df2,'composite_score') if c_charts else ''
    nodata='<p style="color:#7c7894;text-align:center;padding:20px">無K線圖</p>'

    html=f'''<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>台股GOGOGO v3.0 — {TODAY_DISP} 上市選股報告</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700;900&family=Rajdhani:wght@600;700&display=swap');
:root{{--bg:#0a0a12;--bg2:#12121e;--bg3:#1a1a2e;--border:#2a2a45;--accent:#c084fc;--accent2:#a855f7;--teal:#2dd4bf;--coral:#fb7185;
--text:#f0eef6;--text2:#c4c0d4;--text3:#7c7894;--gold:#fbbf24;--green:#34d399;--red:#f87171;--orange:#f97316;--orange2:#ea580c;}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:var(--bg);color:var(--text);font-family:'Noto Sans TC',sans-serif;font-size:15px;line-height:1.65;}}
.header{{background:linear-gradient(135deg,#0f0c1a 0%,#1a1428 30%,#251a18 60%,#1a1020 100%);
  border-bottom:3px solid var(--orange);padding:44px 48px 36px;position:relative;overflow:hidden;}}
.header::before{{content:'';position:absolute;top:-40%;right:-5%;width:500px;height:500px;
  background:radial-gradient(circle,rgba(249,115,22,0.08) 0%,transparent 65%);pointer-events:none;}}
.header-label{{font-family:'Rajdhani',monospace;color:var(--orange);font-size:.75em;font-weight:700;letter-spacing:5px;margin-bottom:10px;}}
.header-label::before{{content:'——';margin-right:10px;color:var(--orange2);}}
.header h1{{font-size:2.2em;font-weight:900;
  background:linear-gradient(135deg,#f97316 0%,#fb923c 40%,#fbbf24 70%,#f97316 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}}
.header-meta{{margin-top:16px;color:var(--text3);font-size:.88em;}}
.header-meta strong{{color:var(--orange);font-weight:700;}}
.header-badge{{display:inline-block;background:linear-gradient(135deg,var(--orange2),var(--orange));
  color:#0a0a12;padding:6px 18px;border-radius:20px;font-size:.78em;font-weight:900;margin-right:12px;}}
.stats-bar{{display:flex;border-bottom:1px solid var(--border);}}
.stat-item{{flex:1;padding:22px 28px;border-right:1px solid var(--border);background:linear-gradient(180deg,var(--bg2),var(--bg));transition:background .3s;}}
.stat-item:hover{{background:var(--bg3);}} .stat-item:last-child{{border-right:none;}}
.stat-label{{font-family:'Rajdhani',sans-serif;font-size:.72em;color:var(--text3);letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;font-weight:700;}}
.stat-value{{font-size:1.7em;font-weight:900;color:var(--orange);}} .stat-sub{{font-size:.72em;color:var(--text3);margin-top:3px;}}
.container{{max-width:1480px;margin:0 auto;padding:36px;}}
.section{{margin-bottom:52px;border:1px solid var(--border);border-radius:16px;overflow:hidden;background:var(--bg2);box-shadow:0 0 40px rgba(192,132,252,.03);}}
.section-header{{padding:22px 30px;display:flex;align-items:center;gap:16px;}}
.section-header.composite{{background:linear-gradient(90deg,#1e1040,#12121e);border-bottom:2px solid var(--accent);}}
.section-header.early{{background:linear-gradient(90deg,#0a201a,#12121e);border-bottom:2px solid var(--teal);}}
.section-header.strong{{background:linear-gradient(90deg,#201510,#12121e);border-bottom:2px solid var(--coral);}}
.section-icon{{font-size:1.8em;}} .section-title h2{{font-size:1.25em;font-weight:900;}}
.section-title p{{font-size:.8em;color:var(--text3);margin-top:3px;}}
.chart-link{{margin-left:auto;color:var(--accent);font-size:.85em;font-weight:700;text-decoration:none;padding:8px 18px;
  border:1px solid rgba(192,132,252,.3);border-radius:10px;background:rgba(192,132,252,.06);transition:all .2s;}}
.chart-link:hover{{background:rgba(192,132,252,.15);border-color:var(--accent);}}
.table-wrap{{overflow-x:auto;}}
table{{width:100%;border-collapse:collapse;font-size:.86em;}}
thead tr{{background:var(--bg3);border-bottom:1px solid var(--border);}}
th{{padding:13px 15px;text-align:left;color:var(--text3);font-weight:700;font-size:.78em;letter-spacing:.5px;white-space:nowrap;}}
td{{padding:15px 15px;border-bottom:1px solid rgba(42,42,69,.6);color:var(--text2);}}
tbody tr{{transition:background .15s;}} tbody tr:hover{{background:rgba(192,132,252,.04);}}
tbody tr:nth-child(1){{background:rgba(192,132,252,.08);}}
tbody tr:nth-child(2){{background:rgba(45,212,191,.04);}}
tbody tr:nth-child(3){{background:rgba(251,113,133,.03);}}
.badge-score{{background:linear-gradient(135deg,#1a2040,#2d3570);color:#fff;padding:4px 14px;border-radius:20px;font-weight:700;font-size:.95em;display:inline-block;}}
.badge-composite{{background:linear-gradient(135deg,#3b1870,#6d28d9);color:#fff;padding:4px 14px;border-radius:20px;font-weight:700;box-shadow:0 0 12px rgba(139,92,246,.2);}}
.charts-grid{{background:var(--bg);padding:28px;}}
.chart-wrap{{margin-bottom:32px;border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#0a0a12;}}
.chart-caption{{padding:12px 20px;background:var(--bg3);font-size:.85em;color:var(--text2);font-weight:700;border-bottom:1px solid var(--border);}}
.legend{{display:flex;gap:18px;flex-wrap:wrap;padding:14px 30px;background:var(--bg3);border-top:1px solid var(--border);font-size:.74em;color:var(--text3);}}
.dot{{width:10px;height:10px;border-radius:50%;display:inline-block;vertical-align:middle;}}
.footer{{text-align:center;padding:28px;color:var(--text3);font-size:.78em;border-top:1px solid var(--border);background:var(--bg2);}}
.fixed-nav{{position:fixed;bottom:28px;right:18px;z-index:9999;display:flex;flex-direction:column;gap:7px;}}
.fixed-nav a{{display:block;text-align:center;padding:12px 12px;background:rgba(10,10,18,.92);color:var(--accent);font-size:14px;
  font-weight:700;text-decoration:none;border-radius:12px;border:1px solid rgba(192,132,252,.35);min-width:76px;backdrop-filter:blur(8px);transition:all .2s;letter-spacing:2px;}}
.fixed-nav a:hover{{background:rgba(192,132,252,.12);border-color:var(--accent);}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(24px)}}to{{opacity:1;transform:translateY(0)}}}}
.section{{animation:fadeUp .6s ease-out both;}} .section:nth-child(2){{animation-delay:.15s;}} .section:nth-child(3){{animation-delay:.3s;}}
</style></head><body>
<div class="header">
<div class="header-label">TSE · 台股 GOGOGO · V3.0</div>
<h1>台股GOGOGO 上市選股報告</h1>
<div class="header-meta">
<span class="header-badge">📅 {TODAY_DISP} 收盤分析</span>
掃描 <strong>{len(price_data)}</strong> 檔上市股 &nbsp; 強勢確認 <strong>{len(sc_list)}</strong> 檔 &nbsp; 起漲預警 <strong>{len(ec_list)}</strong> 檔
</div></div>
<div class="stats-bar">
<div class="stat-item"><div class="stat-label">掃描標的</div><div class="stat-value">{len(price_data)}</div><div class="stat-sub">上市全市場</div></div>
<div class="stat-item"><div class="stat-label">強勢確認股</div><div class="stat-value" style="color:var(--coral)">{len(sc_list)}</div><div class="stat-sub">追高吃肉首選</div></div>
<div class="stat-item"><div class="stat-label">起漲預警股</div><div class="stat-value" style="color:var(--teal)">{len(ec_list)}</div><div class="stat-sub">提前布局候選</div></div>
<div class="stat-item"><div class="stat-label">綜合轉強 TOP1</div><div class="stat-value" style="font-size:1.15em;color:var(--accent)">{top1_id} {top1_name}</div><div class="stat-sub">綜合分 {top1_score}</div></div>
<div class="stat-item"><div class="stat-label">報告日期</div><div class="stat-value" style="font-size:1.15em">{TODAY_DISP}</div><div class="stat-sub">收盤後自動分析</div></div>
</div>
<div class="container">
<div class="section" id="composite-section"><div class="section-header composite"><div class="section-icon">🔮</div><div class="section-title"><h2>綜合轉強潛力股 Top {TOP_COMPOSITE}</h2><p>綜合分 = 起漲分×{COMPOSITE_EARLY_W} + 強勢分×{COMPOSITE_TOTAL_W}</p></div><a href="#composite-charts" class="chart-link">K線圖 ↓</a></div>
<div class="table-wrap"><table><thead><tr><th>排名</th><th>代碼</th><th>名稱</th><th>綜合分</th><th>收盤價</th><th>量比</th><th>MA28乖離</th><th>漲幅%</th><th>RSI14</th><th>營收YoY</th><th>法人連買</th><th>起漲分</th><th>強勢分</th></tr></thead><tbody>{cr}</tbody></table></div>
<div class="legend"><div><span class="dot" style="background:var(--accent)"></span> 綜合分 = early×{COMPOSITE_EARLY_W} + total×{COMPOSITE_TOTAL_W}</div></div></div>

<div class="section" id="early-section"><div class="section-header early"><div class="section-icon">🌱</div><div class="section-title"><h2>即將起漲潛力股 Top {TOP_EARLY}</h2><p>硬條件過濾+加分排名</p></div><a href="#early-charts" class="chart-link">K線圖 ↓</a></div>
<div class="table-wrap"><table><thead><tr><th>排名</th><th>代碼</th><th>名稱</th><th>總分</th><th>收盤價</th><th>成交值</th><th>量比</th><th>MA28乖離</th><th>漲幅%</th><th>RSI14</th><th>收斂比</th><th>營收YoY</th><th>法人連買</th></tr></thead><tbody>{er}</tbody></table></div>
<div class="legend"><div><span class="dot" style="background:var(--teal)"></span> YoY>{EW_BONUS_YOY_THRESHOLD:.0f}%→+{EW_BONUS_YOY:.0f} ｜ 法人連買≥2天→+{EW_BONUS_INST:.0f} ｜ 60日<25%→+{EW_BONUS_60D:.0f}</div></div></div>

<div class="section" id="strong-section"><div class="section-header strong"><div class="section-icon">🔥</div><div class="section-title"><h2>強勢確認股 Top {TOP_STRONG}</h2><p>量價齊揚+法人認同+技術突破</p></div><a href="#strong-charts" class="chart-link">K線圖 ↓</a></div>
<div class="table-wrap"><table><thead><tr><th>排名</th><th>代碼</th><th>名稱</th><th>總分</th><th>訊號</th><th>收盤價</th><th>成交值</th><th>量比</th><th>MA28乖離</th><th>漲幅%</th><th>RSI14</th><th>連買天</th><th>強弱</th><th>營收YoY</th></tr></thead><tbody>{sr}</tbody></table></div>
<div class="legend"><div><span class="dot" style="background:var(--coral)"></span> 量價子分數×1.1 + 連買天數×{INST_CONSEC_WEIGHT} + Z-score</div><div><span style="color:var(--red)">RSI⚠️</span> ≥78 追高需謹慎</div></div></div>

<div class="section" id="composite-charts"><div class="section-header composite"><div class="section-icon">🔮</div><div class="section-title"><h2>綜合轉強 K線圖</h2></div></div><div class="charts-grid">{cch or nodata}</div></div>
<div class="section" id="early-charts"><div class="section-header early"><div class="section-icon">🌱</div><div class="section-title"><h2>起漲預警 K線圖</h2></div></div><div class="charts-grid">{ech or nodata}</div></div>
<div class="section" id="strong-charts"><div class="section-header strong"><div class="section-icon">🔥</div><div class="section-title"><h2>強勢確認 K線圖</h2></div></div><div class="charts-grid">{sch or nodata}</div></div>
</div>
<div class="footer">台股GOGOGO v3.0 ｜ {TODAY_DISP} ｜ early×{COMPOSITE_EARLY_W}+total×{COMPOSITE_TOTAL_W} ｜ 量價×1.1 ｜ 僅供參考</div>
<nav class="fixed-nav"><a href="#composite-section">綜合轉強</a><a href="#early-section">即將起漲</a><a href="#strong-section">強勢確認</a></nav>
</body></html>'''
    os.makedirs('output',exist_ok=True)
    hfn=f'output/TSE_report_{TODAY_STR}.html'
    with open(hfn,'w',encoding='utf-8') as f: f.write(html)
    print(f'✅ HTML：{hfn}（{len(html)//1024} KB）'); return hfn

# ═══ 通知 + main ═══
def send_telegram(sdf,edf,sc,ec,ns):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: print('⚠️ 無Telegram'); return
    lines=[f"📊 *台股GOGOGO v3.0 — {TODAY_DISP}*","",f"掃描{ns}檔 強勢{sc} 預警{ec}",""]
    if not sdf.empty:
        lines.append("*🔥 強勢Top5:*")
        for _,r in sdf.head(5).iterrows(): lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} {r['total_score']:.1f}分")
        lines.append("")
    if not edf.empty:
        lines.append("*🌱 預警Top5:*")
        for _,r in edf.head(5).iterrows(): lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} {r['total_ew_score']:.1f}分")
    if GITHUB_PAGES_URL: lines.append(f"\n🌐 [報告]({GITHUB_PAGES_URL})")
    try:
        resp=requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={'chat_id':TELEGRAM_CHAT_ID,'text':'\n'.join(lines),'parse_mode':'Markdown'},timeout=15)
        print('✅ TG已發送' if resp.status_code==200 else f'⚠️{resp.text}')
    except Exception as e: print(f'⚠️TG:{e}')

def send_email(csv_fn,html_fn,sdf,edf,sc,ec,ns):
    if not GMAIL_USER or not GMAIL_APP_PASS or not EMAIL_TO: print('⚠️ 無Email'); return
    msg=MIMEMultipart('mixed')
    msg['Subject']=f'台股GOGOGO v3.0 {TODAY_DISP} 強勢{sc} 預警{ec}'
    msg['From']=GMAIL_USER; msg['To']=EMAIL_TO
    body=f'台股GOGOGO v3.0 {TODAY_DISP}\n掃描{ns}檔 強勢{sc} 預警{ec}'
    if GITHUB_PAGES_URL: body+=f'\n報告：{GITHUB_PAGES_URL}'
    msg.attach(MIMEText(body,'plain','utf-8'))
    for fpath in [csv_fn,html_fn]:
        if fpath and os.path.exists(fpath):
            with open(fpath,'rb') as f:
                part=MIMEBase('application','octet-stream'); part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition','attachment',filename=os.path.basename(fpath))
            msg.attach(part)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com',465) as s:
            s.login(GMAIL_USER,GMAIL_APP_PASS); s.sendmail(GMAIL_USER,EMAIL_TO.split(','),msg.as_string())
        print('✅ Email已發送（CSV+HTML）')
    except Exception as e: print(f'⚠️Email:{e}')

def main():
    print("="*60); print("台股GOGOGO 上市專用模型 v3.0"); print("="*60)
    font_path,font_prop=init_chinese_font()
    all_ids,nm=load_stock_list()
    fids=twse_prefilter(all_ids,nm)
    n=len(fids)
    print(f'\n[Token] {n} 檔 → 股價({n+1})+營收({n})={n*2+1}/600')
    pd_=fetch_all_prices(fids,FINMIND_TOKEN_1)
    vids=list(pd_.keys())
    fin=fetch_all_revenue(vids,FINMIND_TOKEN_1)
    inst=fetch_all_inst(vids,'')
    sdf,sc=run_strong_filter(pd_,inst,fin,nm)
    edf,ec=run_early_filter(pd_,inst,fin,nm)
    csv_fn,full=export_csv(pd_,inst,fin,nm,sdf,edf)
    print('\n[K線圖] 繪製中...')
    s_charts,e_charts,c_charts={},{},{}
    if not sdf.empty:
        for sid in sdf['stock_id'].head(TOP_CHART).tolist():
            b=draw_kline(sid,pd_,nm,font_path,'強勢確認');
            if b: s_charts[sid]=b
    if not edf.empty:
        for sid in edf['stock_id'].head(TOP_CHART).tolist():
            b=draw_kline(sid,pd_,nm,font_path,'起漲預警');
            if b: e_charts[sid]=b
    comp_top=full[full['composite_score']>0].sort_values('composite_score',ascending=False).head(TOP_CHART)
    for sid in comp_top['stock_id'].tolist():
        b=draw_kline(sid,pd_,nm,font_path,'綜合轉強');
        if b: c_charts[sid]=b
    print(f'  強勢{len(s_charts)} 預警{len(e_charts)} 綜合{len(c_charts)}')
    html_fn=export_html(pd_,inst,fin,nm,sdf,edf,sc,ec,s_charts,e_charts,c_charts,full)
    ns=len(pd_)
    send_telegram(sdf,edf,len(sc),len(ec),ns)
    send_email(csv_fn,html_fn,sdf,edf,len(sc),len(ec),ns)
    print("\n"+"="*60)
    print(f"✅ Twgogogo_{TODAY_STR}.csv 已產生")
    print(f"✅ TSE_report_{TODAY_STR}.html 已產生")
    print("="*60)

if __name__=='__main__': main()
