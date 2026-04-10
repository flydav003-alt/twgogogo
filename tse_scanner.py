"""
台股GOGOGO 上市專用模型 v2.0
基於上櫃操盤手 v7.1 架構，調整硬條件門檻與分數權重
多 FinMind Token 輪替機制：Token1→股價, Token2→籌碼, Token3→營收
輸出：Twgogogo_YYYYMMDD.csv

API Call 預算（582 檔股票）：
  Token1: detect(1) + 股價(582) = 583 / 600
  Token2: 籌碼(582)             = 582 / 600
  Token3: 營收(582)             = 582 / 600
  合計 1,747 calls，3 個帳號 1,800 額度
"""

# ============================================================
# Cell 1：載入套件
# ============================================================
import subprocess, sys, os, time, warnings, json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from scipy import stats as scipy_stats
from FinMind.data import DataLoader

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 30)

def install_system_deps():
    try:
        subprocess.run(["sudo","apt-get","install","-y","-q","fonts-noto-cjk"],
                       capture_output=True, check=False)
        print("✅ 中文字型安裝完成")
    except Exception as e:
        print(f"⚠️  字型安裝失敗（不影響主流程）：{e}")

# ============================================================
# Cell 2：系統常數（上市 v2.0 門檻）
# ============================================================

# ── FinMind 多 Token ──
# Token1 負責股價（583 calls），Token2 負責籌碼（582），Token3 負責營收（582）
FINMIND_TOKEN_1 = os.environ.get("FINMIND_TOKEN_1", "") or os.environ.get("FINMIND_TOKEN", "")
FINMIND_TOKEN_2 = os.environ.get("FINMIND_TOKEN_2", "")
FINMIND_TOKEN_3 = os.environ.get("FINMIND_TOKEN_3", "")
# 若未設定 Token2/3，自動 fallback
if not FINMIND_TOKEN_2:
    FINMIND_TOKEN_2 = FINMIND_TOKEN_1
if not FINMIND_TOKEN_3:
    FINMIND_TOKEN_3 = FINMIND_TOKEN_2

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GMAIL_USER       = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASS   = os.environ.get("GMAIL_APP_PASS", "")
EMAIL_TO         = os.environ.get("EMAIL_TO", "")
GITHUB_PAGES_URL = os.environ.get("REPORT_URL", "")

TSE_CSV_PATH = os.environ.get("TSE_CSV_PATH", "stock_list.csv")

# ── 強勢確認股 A 模組（流動性）──
A_VOL_MA5_MIN     = 800
A_TURNOVER_MIN    = 250_000_000   # 上市 2.5 億
A_PRICE_MIN       = 10
A_LIMIT_DAYS      = 3
A_LIMIT_THRESHOLD = 0.095

# ── 強勢確認股 B 模組（技術面）── 上市 v2.0 門檻
B1_VOL_RATIO_MIN  = 1.3
B1_VOL_RATIO_MAX  = 4.0
B2_RETURN_MIN     = 0.01
B2_RETURN_MAX_PCT = 9.0
B4_CLOSE_RATIO    = 0.65
B_PASS_COUNT      = 2

# ── 強勢確認股 C 模組（籌碼）──
C_CONSEC_DAYS_MIN = 3
C_SINGLE_MIN      = 200

# ── 強勢確認股 D 模組（過熱過濾）──
D_RSI_MAX    = 78
D_RETURN_MAX = 0.10

# ── 強勢確認股評分權重 ──
W_VOL_RATIO  = 1.6
W_HIGH20     = 1.4
W_MA28_BIAS  = 1.0
W_INST_DAYS  = 3.0
W_RETURN_PCT = 0.8

# ── 起漲預警硬條件（上市 v2.0）──
EW_VOL_RATIO_MIN  = 1.3
EW_VOL_RATIO_MAX  = 4.0
EW_RETURN_MAX     = 9.0
EW_RETURN_MIN     = -2.0
EW_MA28_BIAS_MAX  = 25.0
EW_CONSOL_RATIO   = 1.12
EW_TURNOVER_MIN   = 250_000_000
EW_ABOVE_MA20_MIN = 0
EW_MAX20D_RET_MAX = 20.0
EW_INST_MIN       = 80
EW_PAST60D_MAX    = 45.0
EW_PAST60D_BIAS   = 25.0

EW_BONUS_YOY      = 16.0
EW_BONUS_INST     = 24.0
EW_BONUS_60D      = 22.0

# ── 綜合分權重（上市 v2.0）──
COMPOSITE_EARLY_W  = 0.52
COMPOSITE_TOTAL_W  = 0.48
INST_CONSEC_WEIGHT = 2.2

MIN_DAYS     = 60
BATCH_SIZE   = 40
BATCH_DELAY  = 1.5
ERROR_LOG    = "error_log.txt"

TODAY      = datetime.today()
END_DATE   = TODAY.strftime('%Y-%m-%d')
START_DATE = (TODAY - timedelta(days=400)).strftime('%Y-%m-%d')
TODAY_STR  = TODAY.strftime('%Y%m%d')
TODAY_DISP = TODAY.strftime('%Y/%m/%d')

print(f"[上市專用模型v2.0] 啟動時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[系統] 資料區間：{START_DATE} → {END_DATE}")

# ============================================================
# Cell 3：工具函式（與原 OTC 完全相同）
# ============================================================

def log_error(msg):
    with open(ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}\n')

def calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_macd(series, fast=12, slow=26, signal=9):
    ema_f = series.ewm(span=fast,   adjust=False).mean()
    ema_s = series.ewm(span=slow,   adjust=False).mean()
    macd  = ema_f - ema_s
    sig   = macd.ewm(span=signal, adjust=False).mean()
    return macd - sig

def consec_buy_days(series):
    if series is None or len(series) == 0:
        return 0
    vals  = series.dropna().values[::-1]
    count = 0
    for v in vals:
        if v > 0:
            count += 1
        else:
            break
    return count

def safe_zscore(arr):
    a = np.array(arr, dtype=float)
    if len(a) < 2 or np.nanstd(a) == 0:
        return np.zeros_like(a)
    return scipy_stats.zscore(a, nan_policy='omit')

def calc_indicators(df):
    if df is None or df.empty:
        return None
    df = df.rename(columns={
        'max': 'high', 'min': 'low',
        'Trading_Volume': 'volume', 'Trading_money': 'turnover',
    })
    for col in ['date','open','high','low','close','volume']:
        if col not in df.columns:
            return None
    df = df.sort_values('date').reset_index(drop=True)
    for col in ['open','high','low','close','volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['close','volume'])
    if len(df) < MIN_DAYS:
        return None
    if 'turnover' in df.columns:
        df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce').fillna(0)
        m = df['turnover'] <= 0
        df.loc[m, 'turnover'] = df.loc[m,'close'] * df.loc[m,'volume'] * 1000
    else:
        df['turnover'] = df['close'] * df['volume'] * 1000

    df['MA5']          = df['close'].rolling(5).mean()
    df['MA20']         = df['close'].rolling(20).mean()
    df['MA28']         = df['close'].rolling(28).mean()
    df['vol_ma5']      = df['volume'].rolling(5).mean()
    df['high20']       = df['high'].rolling(20).max()
    df['daily_return'] = df['close'].pct_change()
    df['RSI14']        = calc_rsi(df['close'], 14)
    hist               = calc_macd(df['close'])
    df['MACD_hist']      = hist
    df['MACD_hist_prev'] = hist.shift(1)
    df['amplitude']    = df['high'] - df['low']
    return df

# ============================================================
# Cell 4：讀取 CSV + FinMind 多 Token 初始化
# ============================================================

def load_stock_list():
    df_csv = None
    for enc in ['cp950','utf-8-sig','utf-8','big5','latin1']:
        try:
            df_csv = pd.read_csv(TSE_CSV_PATH, encoding=enc, dtype=str)
            print(f'✅ CSV 讀取成功（{enc}），共 {len(df_csv)} 筆')
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
        except FileNotFoundError:
            raise FileNotFoundError(f'找不到 {TSE_CSV_PATH}，請先上傳到倉庫根目錄')
    if df_csv is None:
        raise RuntimeError(f'無法讀取 {TSE_CSV_PATH}')
    df_csv.columns = df_csv.columns.str.strip()
    df_csv['stock_id'] = df_csv['stock_id'].astype(str).str.strip()
    df_csv['name']     = df_csv['name'].astype(str).str.strip()
    df_csv = df_csv[df_csv['stock_id'].str.match(r'^\d{4,5}$')].copy()
    stock_ids = df_csv['stock_id'].tolist()
    name_map  = dict(zip(df_csv['stock_id'], df_csv['name']))
    return stock_ids, name_map


def init_tokens(stock_ids):
    """初始化三組 token 並印出 call 預算"""
    tokens = {
        'price': FINMIND_TOKEN_1,
        'inst':  FINMIND_TOKEN_2,
        'rev':   FINMIND_TOKEN_3,
    }
    unique = len(set(t for t in tokens.values() if t))
    n = len(stock_ids)

    print(f'\n{"="*50}')
    print(f'[Token 配置] 偵測到 {unique} 組不同 Token')
    print(f'  股價 Token1: ...{tokens["price"][-6:] if tokens["price"] else "未設定"}')
    print(f'  籌碼 Token2: ...{tokens["inst"][-6:]  if tokens["inst"]  else "未設定"}')
    print(f'  營收 Token3: ...{tokens["rev"][-6:]   if tokens["rev"]   else "未設定"}')
    print(f'\n[API Call 預算] {n} 檔股票')
    print(f'  Token1 股價：{n+1:>4} / 600  (detect 1 + 股價 {n})')
    print(f'  Token2 籌碼：{n:>4} / 600')
    print(f'  Token3 營收：{n:>4} / 600')
    print(f'  合計：{n*3+1} / {unique*600}')

    if unique == 1 and n > 199:
        print(f'  ⚠️  只有 1 組 Token！{n*3+1} calls 將超出 600 上限！')
        print(f'      強烈建議設定 FINMIND_TOKEN_2 + FINMIND_TOKEN_3')
    elif unique == 2 and n > 599:
        print(f'  ⚠️  Token2 = Token3，籌碼+營收共 {n*2} calls 將超出 600！')
        print(f'      建議設定獨立的 FINMIND_TOKEN_3')
    print(f'{"="*50}\n')

    if not tokens['price']:
        raise RuntimeError('❌ 未設定任何 FINMIND_TOKEN，無法執行')
    return tokens


def detect_api_mode(token, stock_ids):
    """用 REST API 測試一次，確認 token 是否有效"""
    test_sid = stock_ids[0]
    try:
        r = requests.get(
            'https://api.finmindtrade.com/api/v4/data',
            params={'dataset':'TaiwanStockPrice','data_id':test_sid,
                    'start_date':START_DATE,'end_date':END_DATE,
                    'token':token}, timeout=20)
        rj = r.json()
        if rj.get('status') == 200 and rj.get('data'):
            print(f'✅ REST API 正常（測試 {test_sid}，{len(rj["data"])} 筆）')
            return True
        print(f'⚠️  REST 回傳異常：{rj.get("msg","")}')
        return False
    except Exception as e:
        print(f'❌ REST API 連線失敗：{e}')
        return False

# ============================================================
# Cell 5：抓取 K 線（Token1）
# ============================================================

def fetch_price_rest(sid, token):
    try:
        r = requests.get(
            'https://api.finmindtrade.com/api/v4/data',
            params={'dataset':'TaiwanStockPrice','data_id':sid,
                    'start_date':START_DATE,'end_date':END_DATE,
                    'token':token}, timeout=20)
        rj = r.json()
        if rj.get('status') == 200 and rj.get('data'):
            return pd.DataFrame(rj['data'])
        if 'request' in str(rj.get('msg','')).lower():
            print(f'\n  ⚠️ Token 限速：{rj.get("msg","")}')
        return None
    except Exception as e:
        log_error(f'{sid} K線：{e}')
        return None

def fetch_all_prices(stock_ids, token):
    price_data = {}
    total   = len(stock_ids)
    batches = (total - 1) // BATCH_SIZE + 1
    print(f'[K線抓取] {total} 檔，{batches} 批次（Token: ...{token[-6:]}）')
    for i in range(0, total, BATCH_SIZE):
        batch    = stock_ids[i:i+BATCH_SIZE]
        batch_no = i // BATCH_SIZE + 1
        print(f'  批次 {batch_no}/{batches}...', end=' ', flush=True)
        ok = 0
        for sid in batch:
            raw = fetch_price_rest(sid, token)
            if raw is None:
                continue
            proc = calc_indicators(raw)
            if proc is not None:
                price_data[sid] = proc
                ok += 1
        print(f'✓{ok} ✗{len(batch)-ok}  累計 {len(price_data)} 檔')
        if i + BATCH_SIZE < total:
            time.sleep(BATCH_DELAY)
    print(f'✅ K線完成  有效 {len(price_data)} / {total} 檔')
    return price_data

# ============================================================
# Cell 6：抓取籌碼（Token2）
# ============================================================

def parse_inst(raw):
    if raw is None or raw.empty or 'name' not in raw.columns:
        return None, None
    df = raw.sort_values('date').copy()
    def net_s(kws):
        pat = '|'.join(kws)
        sub = df[df['name'].str.contains(pat, na=False)].copy()
        if sub.empty:
            return pd.Series(dtype=float)
        sub['net'] = (pd.to_numeric(sub['buy'],  errors='coerce').fillna(0) -
                      pd.to_numeric(sub['sell'], errors='coerce').fillna(0))
        return sub.groupby('date')['net'].sum()
    return net_s(['Foreign_Investor','外資']), net_s(['Investment_Trust','投信'])

def _fetch_inst_raw(sid, token, dataset):
    """單檔籌碼抓取，回傳 raw DataFrame 或 None"""
    try:
        r = requests.get(
            'https://api.finmindtrade.com/api/v4/data',
            params={'dataset': dataset, 'data_id': sid,
                    'start_date': START_DATE, 'end_date': END_DATE,
                    'token': token}, timeout=20)
        rj = r.json()
        if rj.get('status') == 200 and rj.get('data'):
            return pd.DataFrame(rj['data'])
        # 限速警告
        msg = str(rj.get('msg', ''))
        if 'request' in msg.lower() or 'limit' in msg.lower():
            return 'RATE_LIMIT'
        return None
    except Exception as e:
        log_error(f'{sid} 籌碼({dataset})：{e}')
        return None

def _detect_inst_dataset(token, sample_id):
    """
    自動偵測哪個 dataset 有資料：
    優先用 TaiwanStockInstitutionalInvestorsBuySell（新版）
    fallback 到 TaiwanStockInstitutionalInvestors（舊版）
    """
    for ds in ['TaiwanStockInstitutionalInvestorsBuySell',
               'TaiwanStockInstitutionalInvestors']:
        result = _fetch_inst_raw(sample_id, token, ds)
        if result is not None and result != 'RATE_LIMIT' and not result.empty:
            print(f'  [籌碼偵測] ✅ 使用 dataset：{ds}（欄位：{list(result.columns[:6])}）')
            return ds
    print(f'  [籌碼偵測] ⚠️  兩個 dataset 均無資料，以空值繼續')
    return 'TaiwanStockInstitutionalInvestorsBuySell'

def parse_inst(raw):
    if raw is None or raw.empty or 'name' not in raw.columns:
        return None, None
    df = raw.sort_values('date').copy()

    # ── 自動偵測買賣欄位（新版叫 buy/sell，舊版也叫 buy/sell，但有時是 Buy/Sell）──
    buy_col  = next((c for c in ['buy',  'Buy',  'buy_amount']  if c in df.columns), None)
    sell_col = next((c for c in ['sell', 'Sell', 'sell_amount'] if c in df.columns), None)
    if buy_col is None or sell_col is None:
        log_error(f'parse_inst 找不到 buy/sell 欄位，現有欄位：{list(df.columns)}')
        return None, None

    def net_s(kws):
        pat = '|'.join(kws)
        sub = df[df['name'].str.contains(pat, na=False)].copy()
        if sub.empty:
            return pd.Series(dtype=float)
        sub['net'] = (pd.to_numeric(sub[buy_col],  errors='coerce').fillna(0) -
                      pd.to_numeric(sub[sell_col], errors='coerce').fillna(0))
        return sub.groupby('date')['net'].sum()

    return net_s(['Foreign_Investor', '外資']), net_s(['Investment_Trust', '投信'])

def fetch_all_inst(valid_ids, token):
    EMPTY = {'foreign_consec':0,'trust_consec':0,'foreign_today':0.0,'trust_today':0.0,
             'foreign_3d':0.0,'trust_3d':0.0}
    inst_data = {sid: dict(EMPTY) for sid in valid_ids}
    total   = len(valid_ids)
    batches = (total - 1) // BATCH_SIZE + 1

    # ── 自動偵測 dataset（用第一檔測試）──
    inst_dataset = _detect_inst_dataset(token, valid_ids[0])

    print(f'\n[籌碼抓取] {total} 檔（Token: ...{token[-6:]}）')
    rate_limit_count = 0
    for i in range(0, total, BATCH_SIZE):
        batch    = valid_ids[i:i+BATCH_SIZE]
        batch_no = i // BATCH_SIZE + 1
        print(f'  批次 {batch_no}/{batches}...', end=' ', flush=True)
        ok = 0
        for sid in batch:
            raw = _fetch_inst_raw(sid, token, inst_dataset)
            if raw == 'RATE_LIMIT':
                rate_limit_count += 1
                if rate_limit_count <= 3:
                    print(f'\n  ⚠️  Token 限速，等待 5 秒...', end=' ', flush=True)
                time.sleep(5)
                raw = _fetch_inst_raw(sid, token, inst_dataset)  # 重試一次
            if raw is None or raw == 'RATE_LIMIT' or raw.empty:
                continue
            f_net, t_net = parse_inst(raw)
            if f_net is None and t_net is None:
                continue
            inst_data[sid] = {
                'foreign_consec': consec_buy_days(f_net),
                'trust_consec':   consec_buy_days(t_net),
                'foreign_today':  float(f_net.iloc[-1]) if f_net is not None and len(f_net)>0 else 0.0,
                'trust_today':    float(t_net.iloc[-1]) if t_net is not None and len(t_net)>0 else 0.0,
                'foreign_3d':     float(f_net.iloc[-3:].sum()) if f_net is not None and len(f_net)>=3 else 0.0,
                'trust_3d':       float(t_net.iloc[-3:].sum()) if t_net is not None and len(t_net)>=3 else 0.0,
            }
            ok += 1
        print(f'OK {ok}/{len(batch)}')
        if i + BATCH_SIZE < total:
            time.sleep(BATCH_DELAY)
    total_ok = sum(1 for v in inst_data.values() if v.get('foreign_consec',0)+v.get('trust_consec',0) > 0
                   or v.get('foreign_today',0) != 0 or v.get('trust_today',0) != 0)
    print(f'✅ 籌碼完成  有效資料 {total_ok} / {total} 檔')
    return inst_data

# ============================================================
# Cell 7：抓取月營收 YoY（Token3）
# ============================================================

def calc_yoy_revenue(sid, token):
    """
    抓月營收 YoY。策略：
    1. 先抓最新一期（days=500 確保有 13 個月）
    2. 若最新期無資料，往前找最近有資料的那期（最多退 2 期）
    3. 限速時 retry 最多 3 次，每次等待遞增
    """
    for attempt in range(3):
        try:
            r = requests.get(
                'https://api.finmindtrade.com/api/v4/data',
                params={'dataset': 'TaiwanStockMonthRevenue', 'data_id': sid,
                        'start_date': (TODAY - timedelta(days=500)).strftime('%Y-%m-%d'),
                        'end_date': END_DATE, 'token': token}, timeout=25)
            rj = r.json()

            # ── 限速偵測 ──
            msg = str(rj.get('msg', ''))
            if 'request' in msg.lower() or 'limit' in msg.lower():
                wait = (attempt + 1) * 4
                log_error(f'{sid} 月營收限速 attempt{attempt+1}，等待 {wait}s')
                time.sleep(wait)
                continue

            if not (rj.get('status') == 200 and rj.get('data')):
                return None

            rev_df = pd.DataFrame(rj['data'])
            rev_col = next((c for c in ['revenue', 'Revenue', 'monthly_revenue']
                            if c in rev_df.columns), None)
            if rev_col is None or rev_df.empty:
                return None

            rev_df = rev_df.sort_values('date').reset_index(drop=True)
            rev_df[rev_col] = pd.to_numeric(rev_df[rev_col], errors='coerce')
            rev_df = rev_df.dropna(subset=[rev_col])
            rev_df = rev_df[rev_df[rev_col] > 0].reset_index(drop=True)

            if len(rev_df) < 13:
                return None

            # ── 找最近有效期（最多往前 2 期，相容月底前還未公告的情況）──
            for offset in [0, 1, 2]:
                idx_latest = len(rev_df) - 1 - offset
                idx_prev   = idx_latest - 12
                if idx_prev < 0:
                    continue
                latest_rev = rev_df[rev_col].iloc[idx_latest]
                prev_rev   = rev_df[rev_col].iloc[idx_prev]
                if prev_rev > 0 and latest_rev > 0:
                    yoy = round((latest_rev - prev_rev) / prev_rev * 100, 1)
                    if offset > 0:
                        log_error(f'{sid} 月營收：使用前 {offset} 期（{rev_df["date"].iloc[idx_latest]}）')
                    return yoy

            return None

        except Exception as e:
            log_error(f'{sid} 月營收 attempt{attempt+1}：{e}')
            time.sleep(2)
    return None

def fetch_all_revenue(valid_ids, token):
    fin_data = {}
    total   = len(valid_ids)
    batches = (total - 1) // BATCH_SIZE + 1
    print(f'\n[月營收抓取] {total} 檔（Token: ...{token[-6:]}）')
    for i in range(0, total, BATCH_SIZE):
        batch    = valid_ids[i:i+BATCH_SIZE]
        batch_no = i // BATCH_SIZE + 1
        print(f'  批次 {batch_no}/{batches}...', end=' ', flush=True)
        ok = fail = 0
        for sid in batch:
            yoy = calc_yoy_revenue(sid, token)
            fin_data[sid] = yoy
            if yoy is not None: ok += 1
            else: fail += 1
        print(f'有YoY {ok} / 無資料 {fail}')
        if i + BATCH_SIZE < total:
            time.sleep(2.5)   # 從 1.5 → 2.5，避免 Token3 限速
    print(f'✅ 月營收完成  有YoY {sum(1 for v in fin_data.values() if v is not None)} / {total} 檔')
    return fin_data

# ============================================================
# Cell 8：篩選模組 A/B/C/D — 強勢確認股
# ============================================================

def compute_limit_flag(df):
    if len(df) < A_LIMIT_DAYS:
        return False
    r = df.tail(A_LIMIT_DAYS)['daily_return'].fillna(0)
    return all(v >= A_LIMIT_THRESHOLD for v in r) or all(v <= -A_LIMIT_THRESHOLD for v in r)

def module_a(r):
    if r.get('vol_ma5',0)        <= A_VOL_MA5_MIN:  return False
    if r.get('turnover_today',0) <= A_TURNOVER_MIN:  return False
    if r.get('close',0)          <= A_PRICE_MIN:     return False
    if r.get('limit_flag',False):                     return False
    return True

def module_b(r):
    signals, close = [], r.get('close',0)
    vol_ratio = r.get('vol_ratio',0)
    dpct = r.get('daily_return',0) * 100
    if not (B1_VOL_RATIO_MIN <= vol_ratio <= B1_VOL_RATIO_MAX):
        return False, []
    if not (EW_RETURN_MIN <= dpct <= B2_RETURN_MAX_PCT):
        return False, []
    if vol_ratio >= B1_VOL_RATIO_MIN:
        signals.append(f'爆量{vol_ratio:.1f}倍')
    if close >= (r.get('high20') or float('inf')) and r.get('daily_return',0) > B2_RETURN_MIN:
        signals.append('突破20日高點')
    ma5, ma28 = r.get('MA5',0) or 0, r.get('MA28',0) or 0
    if close > ma28 > 0 and close > ma5 > 0:
        signals.append('均線多頭排列')
    h, l, o = r.get('high',0), r.get('low',0), r.get('open',0)
    hl = h - l
    if close > o and hl > 0 and close >= l + hl * B4_CLOSE_RATIO:
        signals.append('強勢紅K收盤')
    return len(signals) >= B_PASS_COUNT, signals

def module_c(sid, inst_data):
    info, signals = inst_data.get(sid,{}), []
    for tag, ck, tk in [('外資','foreign_consec','foreign_today'),('投信','trust_consec','trust_today')]:
        c, t = info.get(ck,0), info.get(tk,0)
        if c >= C_CONSEC_DAYS_MIN:   signals.append(f'{tag}連買{c}天')
        elif t >= C_SINGLE_MIN:      signals.append(f'{tag}買超{int(t)}張')
    return len(signals) >= 1, signals

def module_d(r):
    rsi  = r.get('RSI14',0)
    ret  = r.get('daily_return',0)
    macd = r.get('MACD_hist',None)
    macp = r.get('MACD_hist_prev',None)
    if rsi >= D_RSI_MAX and ret >= D_RETURN_MAX:
        return False
    if macd is not None and macp is not None and not np.isnan(macd) and not np.isnan(macp):
        if macd < -0.5 and macd < macp:
            return False
    return True

# ============================================================
# Cell 9：強勢確認股篩選 + 評分
# ============================================================

def run_strong_filter(price_data, inst_data, fin_data, name_map):
    funnel = {'總有效':len(price_data),'A流動性':0,'B技術':0,'C籌碼':0,'D過濾':0}
    candidates = []
    for sid, df in price_data.items():
        if df is None or df.empty: continue
        last = df.iloc[-1].to_dict()
        last['stock_id'] = sid
        vm5 = last.get('vol_ma5',0) or 0
        last['vol_ratio']      = (last.get('volume',0)/vm5) if vm5>0 else 0
        last['turnover_today'] = last.get('turnover',0) or 0
        last['limit_flag']     = compute_limit_flag(df)
        ma28 = last.get('MA28',0) or 0
        ma28_bias = ((last['close']-ma28)/ma28*100) if ma28>0 else 0
        if ma28_bias > EW_MA28_BIAS_MAX: continue
        if not module_a(last): continue
        funnel['A流動性'] += 1
        b_ok, b_sig = module_b(last)
        if not b_ok: continue
        funnel['B技術'] += 1
        c_ok, c_sig = module_c(sid, inst_data)
        if not c_ok: continue
        funnel['C籌碼'] += 1
        if not module_d(last): continue
        funnel['D過濾'] += 1
        if len(df) >= 61:
            c60 = df['close'].iloc[-61]
            past_60d = ((last.get('close',0) - c60) / c60 * 100) if c60 > 0 else 0.0
        else:
            past_60d = 0.0
        if past_60d > EW_PAST60D_MAX and ma28_bias > EW_PAST60D_BIAS: continue

        info = inst_data.get(sid,{})
        inst_consec_raw = info.get('foreign_consec',0) + info.get('trust_consec',0)
        inst_consec_w   = inst_consec_raw * INST_CONSEC_WEIGHT
        dpct = last.get('daily_return',0)*100
        h20  = 1.0 if last.get('close',0) >= (last.get('high20') or float('inf')) else 0.0
        score = (last['vol_ratio'] * W_VOL_RATIO + h20 * W_HIGH20 +
                 ma28_bias * W_MA28_BIAS + inst_consec_w * W_INST_DAYS + dpct * W_RETURN_PCT)
        candidates.append({
            'stock_id': sid, 'name': name_map.get(sid,sid),
            'score': round(score,2), 'close': last.get('close',0),
            'turnover_today': last.get('turnover_today',0),
            'vol_ratio': round(last['vol_ratio'],2), 'ma28_bias': round(ma28_bias,2),
            'daily_return_pct': round(dpct,2), 'rsi14': round(last.get('RSI14',0) or 0,1),
            'inst_consec': inst_consec_raw,
            'foreign_today': info.get('foreign_today',0), 'trust_today': info.get('trust_today',0),
            'foreign_3d': info.get('foreign_3d',0), 'trust_3d': info.get('trust_3d',0),
            'yoy_revenue_pct': fin_data.get(sid, None), 'past_60d_cum': round(past_60d, 1),
            '_vr': last['vol_ratio'], '_mb': ma28_bias,
            '_ic': float(inst_consec_w), '_dp': dpct, '_h20': h20,
        })

    if len(candidates) >= 2:
        vz = safe_zscore([c['_vr'] for c in candidates])
        mz = safe_zscore([c['_mb'] for c in candidates])
        iz = safe_zscore([c['_ic'] for c in candidates])
        dz = safe_zscore([c['_dp'] for c in candidates])
        hz = safe_zscore([c['_h20'] for c in candidates])
        w_sum = W_VOL_RATIO + W_HIGH20 + W_MA28_BIAS + W_INST_DAYS + W_RETURN_PCT
        for i, c in enumerate(candidates):
            z = (W_VOL_RATIO/w_sum*vz[i] + W_HIGH20/w_sum*mz[i] +
                 W_MA28_BIAS/w_sum*iz[i] + W_INST_DAYS/w_sum*dz[i] + W_RETURN_PCT/w_sum*hz[i])
            c['z_score']    = round(float(z),3)
            c['total_score'] = round(c['score'] + float(z),2)
    else:
        for c in candidates:
            c['z_score'] = 0.0
            c['total_score'] = c['score']

    for c in candidates:
        if c.get('ma28_bias',0) > 35:         c['total_score'] -= 18
        elif c.get('ma28_bias',0) > 25:       c['total_score'] -= 10
        if c.get('daily_return_pct',0) > 9.5: c['total_score'] -= 12
        if c.get('rsi14',0) > 78:             c['total_score'] -= 8
        c['total_score'] = round(max(c['total_score'], 0), 2)

    if not candidates:
        print('⚠️  強勢確認股：無候選（請確認籌碼資料是否正常）')
        strong_df = pd.DataFrame()
    else:
        strong_df = (pd.DataFrame(candidates).sort_values('total_score', ascending=False).reset_index(drop=True))
        strong_df.insert(0, 'rank', range(1, len(strong_df)+1))
    print(f'\n【強勢確認股漏斗】')
    base = funnel['總有效'] or 1
    for k, v in funnel.items():
        print(f'  {k}：{v} 檔 ({v/base*100:.1f}%)')
    print(f'強勢確認股候選：{len(candidates)} 檔')
    return strong_df, candidates

# ============================================================
# Cell 10：起漲預警篩選 + 評分
# ============================================================

def run_early_filter(price_data, inst_data, fin_data, name_map):
    candidates = []
    for sid, df in price_data.items():
        if df is None or len(df) < 30: continue
        last = df.iloc[-1].to_dict()
        vm5 = last.get('vol_ma5', 0) or 0
        if vm5 <= 0: continue
        vol_ratio = last.get('volume', 0) / vm5
        dpct      = last.get('daily_return', 0) * 100
        close     = last.get('close', 0)
        ma20      = last.get('MA20', 0) or 0
        ma28      = last.get('MA28', 0) or 0
        ma28_bias = ((close - ma28) / ma28 * 100) if ma28 > 0 else 0
        to_day    = last.get('turnover', 0) or 0
        rsi14     = last.get('RSI14', 0) or 0
        amp10     = df['amplitude'].tail(10).mean()
        amp20_val = df['amplitude'].tail(20).mean()
        consol    = (amp10 / amp20_val) if amp20_val > 0 else 999
        if len(df) >= 61:
            c60d = df['close'].iloc[-61]
            past_60d = ((close - c60d) / c60d * 100) if c60d > 0 else 0
        else:
            past_60d = 0.0
        if to_day < EW_TURNOVER_MIN: continue
        if not (EW_VOL_RATIO_MIN <= vol_ratio <= EW_VOL_RATIO_MAX): continue
        if not (EW_RETURN_MIN <= dpct <= EW_RETURN_MAX): continue
        if ma28_bias > EW_MA28_BIAS_MAX: continue
        if consol >= EW_CONSOL_RATIO: continue
        if past_60d > EW_PAST60D_MAX and ma28_bias > EW_PAST60D_BIAS: continue
        ret20 = df['daily_return'].tail(20) * 100
        if ret20.max() >= EW_MAX20D_RET_MAX: continue
        tail7 = df.tail(7)
        below_ma = (tail7['close'] <= tail7['MA20']).sum()
        if below_ma < EW_ABOVE_MA20_MIN: continue
        if close < ma20 * 0.975: continue

        info = inst_data.get(sid, {})
        f_today = info.get('foreign_today', 0)
        t_today = info.get('trust_today',  0)
        inst_consec_raw = info.get('foreign_consec', 0) + info.get('trust_consec', 0)
        f_today_n = f_today / 1000 if abs(f_today) > 1_000_000 else f_today
        yoy = fin_data.get(sid, None)

        vol_ratio_score = vol_ratio * 15
        consol_score    = max(0, (1.20 - consol)) * 14
        inst_score = (1 if inst_consec_raw >= 2 or f_today_n >= 80 else 0) * 18
        ew_score = 0.35*vol_ratio_score + 0.30*consol_score + 0.35*inst_score

        bonus_total = 8.0
        if yoy is not None and not pd.isna(yoy):
            yov = float(yoy)
            if yov > 80:    bonus_total += EW_BONUS_YOY
            elif yov > 30:  bonus_total += EW_BONUS_YOY * 0.7
            else:           bonus_total += EW_BONUS_YOY * 0.4
        if inst_consec_raw >= 2 or f_today_n > 80:
            bonus_total += EW_BONUS_INST
        if past_60d < 25:
            bonus_total += EW_BONUS_60D
        ew_score += bonus_total

        if dpct > 6.5:       ew_score -= 8
        if ma28_bias > 18.5: ew_score -= 9
        elif ma28_bias > 15.0: ew_score -= 5
        total_ew_score = round(max(ew_score, 0), 2)

        candidates.append({
            'stock_id': sid, 'name': name_map.get(sid, sid),
            'total_ew_score': total_ew_score, 'ew_score': total_ew_score,
            'close': close, 'turnover_today': to_day,
            'vol_ratio': round(vol_ratio, 2), 'ma28_bias': round(ma28_bias, 2),
            'daily_return_pct': round(dpct, 2), 'rsi14': round(rsi14, 1),
            'consol_ratio': round(consol, 2), 'past_60d_cum': round(past_60d, 1),
            'yoy_revenue_pct': yoy, 'inst_consec_days': inst_consec_raw,
            'foreign_today': f_today, 'trust_today': t_today,
            'foreign_3d': info.get('foreign_3d', 0), 'trust_3d': info.get('trust_3d', 0),
        })

    if candidates:
        early_df = (pd.DataFrame(candidates).sort_values('total_ew_score', ascending=False).reset_index(drop=True))
        early_df.insert(0, 'rank', range(1, len(early_df)+1))
    else:
        early_df = pd.DataFrame()
    print(f'起漲預警候選：{len(candidates)} 檔')
    return early_df, candidates

# ============================================================
# Cell 11：輸出 CSV — Twgogogo_YYYYMMDD.csv
# ============================================================

def export_csv(price_data, inst_data, fin_data, name_map, strong_df, early_df):
    strong_set = set(strong_df['stock_id'].tolist()) if not strong_df.empty else set()
    early_set  = set(early_df['stock_id'].tolist())  if not early_df.empty else set()
    strong_score_map = ({r['stock_id']: r.get('total_score',0) for _,r in strong_df.iterrows()}
                        if not strong_df.empty else {})
    early_score_map  = ({r['stock_id']: r.get('ew_score',0) for _,r in early_df.iterrows()}
                        if not early_df.empty else {})

    full_rows = []
    for sid, df in price_data.items():
        if df is None or df.empty: continue
        last = df.iloc[-1].to_dict()
        vm5    = last.get('vol_ma5', 0) or 0
        close  = last.get('close', 0)
        ma28   = last.get('MA28',  0) or 0
        vol_r  = (last.get('volume', 0) / vm5) if vm5 > 0 else 0
        dpct   = last.get('daily_return', 0) * 100
        mb     = ((close - ma28) / ma28 * 100) if ma28 > 0 else 0
        to_day = last.get('turnover', 0) or 0
        info   = inst_data.get(sid, {})
        f_today = info.get('foreign_today', 0)
        t_today = info.get('trust_today',  0)
        f_3d    = info.get('foreign_3d',   0)
        t_3d    = info.get('trust_3d',     0)
        inst_c  = info.get('foreign_consec', 0) + info.get('trust_consec', 0)
        yoy_rev = fin_data.get(sid, None)
        is_strong = sid in strong_set
        is_early  = sid in early_set

        reject_parts = []
        if vm5 <= A_VOL_MA5_MIN:     reject_parts.append(f'均量{vm5:.0f}≤{A_VOL_MA5_MIN}張')
        if to_day <= A_TURNOVER_MIN: reject_parts.append('成交額不足')
        if close <= A_PRICE_MIN:     reject_parts.append(f'股價{close}≤{A_PRICE_MIN}元')
        if compute_limit_flag(df):   reject_parts.append('連停排除')
        if not (B1_VOL_RATIO_MIN <= vol_r <= B1_VOL_RATIO_MAX):
            reject_parts.append(f'量比{vol_r:.2f}不在1.3~4.0')
        if not (EW_RETURN_MIN <= dpct <= B2_RETURN_MAX_PCT):
            reject_parts.append(f'漲幅{dpct:.1f}%超限')
        if mb > EW_MA28_BIAS_MAX: reject_parts.append(f'MA28乖離{mb:.1f}%')
        reject_str = '；'.join(reject_parts) if reject_parts else ('通過強勢篩選' if is_strong else '未通過')

        ew_reject = []
        if not (EW_VOL_RATIO_MIN <= vol_r <= EW_VOL_RATIO_MAX): ew_reject.append(f'量比{vol_r:.2f}')
        if not (EW_RETURN_MIN <= dpct <= EW_RETURN_MAX): ew_reject.append(f'漲幅{dpct:.1f}%')
        if mb > EW_MA28_BIAS_MAX: ew_reject.append(f'MA28乖離{mb:.1f}%')
        if to_day < EW_TURNOVER_MIN: ew_reject.append('成交額不足')
        ew_rej_str = '；'.join(ew_reject) if ew_reject else ('通過預警篩選' if is_early else '未通過預警')

        ts = strong_score_map.get(sid, 0)
        es = early_score_map.get(sid,  0)
        try:
            ts_f = float(ts) if ts else 0.0
            es_f = float(es) if es else 0.0
            if is_strong and is_early:
                composite = round(es_f * COMPOSITE_EARLY_W + ts_f * COMPOSITE_TOTAL_W, 2)
            elif is_strong:
                composite = round(ts_f * COMPOSITE_TOTAL_W, 2)
            elif is_early:
                composite = round(es_f * COMPOSITE_EARLY_W, 2)
            else:
                composite = 0.0
        except (ValueError, TypeError):
            composite = 0.0

        full_rows.append({
            'stock_id': sid, 'name': name_map.get(sid, sid),
            'close': round(close, 2), 'vol_ratio': round(vol_r, 2),
            'daily_return_pct': round(dpct, 2), 'ma28_bias_pct': round(mb, 2),
            'turnover_億': round(to_day / 1e8, 2),
            'rsi14': round(last.get('RSI14', 0) or 0, 1),
            'inst_consec_days': inst_c,
            'yoy_revenue_pct': yoy_rev,
            'foreign_today': f_today, 'trust_today': t_today,
            'foreign_3d': f_3d, 'trust_3d': t_3d,
            'is_strong_confirm': is_strong, 'is_early_breakout': is_early,
            'total_score': ts if ts else 0,
            'early_score': es if es else 0,
            'composite_score': composite,
            'reject_reason': reject_str, 'early_reject_reason': ew_rej_str,
        })

    full_out = (pd.DataFrame(full_rows).sort_values('composite_score', ascending=False).reset_index(drop=True))
    full_out.insert(0, 'rank', range(1, len(full_out)+1))

    col_order = [
        'rank','stock_id','name','close','vol_ratio','daily_return_pct',
        'ma28_bias_pct','turnover_億','rsi14','inst_consec_days',
        'yoy_revenue_pct','foreign_today','trust_today','foreign_3d','trust_3d',
        'is_strong_confirm','is_early_breakout','total_score','early_score',
        'composite_score','reject_reason','early_reject_reason'
    ]
    full_out = full_out[col_order]

    os.makedirs('output', exist_ok=True)
    csv_fname = f'output/Twgogogo_{TODAY_STR}.csv'
    full_out.to_csv(csv_fname, index=False, encoding='utf-8-sig')
    print(f'\n✅ CSV 已儲存：{csv_fname}（{len(full_out)} 筆）')
    print(f'   強勢確認：{full_out["is_strong_confirm"].sum()} 筆  '
          f'起漲預警：{full_out["is_early_breakout"].sum()} 筆')
    return csv_fname, full_out

# ============================================================
# Cell 12：Telegram + Email
# ============================================================

def send_telegram(strong_df, early_df, strong_count, early_count, total_scanned):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print('⚠️  未設定 Telegram Token，跳過通知')
        return
    lines = [
        f"📊 *台股GOGOGO 上市選股 v2.0 — {TODAY_DISP}*", "",
        f"🔍 掃描：{total_scanned} 檔上市股",
        f"🔥 強勢確認：{strong_count} 檔",
        f"🌱 起漲預警：{early_count} 檔", "",
    ]
    if not strong_df.empty:
        lines.append("*🔥 強勢確認 Top 5：*")
        for _, r in strong_df.head(5).iterrows():
            lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} | 分:{r['total_score']:.1f}")
        lines.append("")
    if not early_df.empty:
        lines.append("*🌱 起漲預警 Top 5：*")
        for _, r in early_df.head(5).iterrows():
            lines.append(f"  #{int(r['rank'])} {r['stock_id']} {r['name']} | 分:{r['total_ew_score']:.1f}")
        lines.append("")
    if GITHUB_PAGES_URL:
        lines.append(f"🌐 [完整報告]({GITHUB_PAGES_URL})")
    msg = '\n'.join(lines)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg,
                  'parse_mode': 'Markdown', 'disable_web_page_preview': False}, timeout=15)
        print('✅ Telegram 通知已發送' if resp.status_code == 200 else f'⚠️ Telegram：{resp.text}')
    except Exception as e:
        print(f'⚠️ Telegram 錯誤：{e}')

def send_email(csv_fname, strong_df, early_df, strong_count, early_count, total_scanned):
    if not GMAIL_USER or not GMAIL_APP_PASS or not EMAIL_TO:
        print('⚠️  未設定 Email，跳過通知')
        return
    msg = MIMEMultipart('mixed')
    msg['Subject'] = f'台股GOGOGO 上市選股 {TODAY_DISP} — 強勢{strong_count}檔 預警{early_count}檔'
    msg['From']    = GMAIL_USER
    msg['To']      = EMAIL_TO
    body = [f'台股GOGOGO 上市專用模型 v2.0 — {TODAY_DISP}', '',
            f'掃描：{total_scanned} 檔 | 強勢：{strong_count} 檔 | 預警：{early_count} 檔']
    if GITHUB_PAGES_URL:
        body.append(f'\n完整報告：{GITHUB_PAGES_URL}')
    body.append('\n--- 本郵件由系統自動發送，不構成投資建議 ---')
    msg.attach(MIMEText('\n'.join(body), 'plain', 'utf-8'))
    if os.path.exists(csv_fname):
        with open(csv_fname, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(csv_fname))
        msg.attach(part)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASS)
            smtp.sendmail(GMAIL_USER, EMAIL_TO.split(','), msg.as_string())
        print('✅ Email 已發送')
    except Exception as e:
        print(f'⚠️ Email 發送失敗：{e}')

# ============================================================
# 主程式
# ============================================================

def main():
    print("=" * 60)
    print("台股GOGOGO 上市專用模型 v2.0 — 開始執行")
    print("=" * 60)
    install_system_deps()

    stock_ids, name_map = load_stock_list()
    print(f'有效代碼：{len(stock_ids)} 檔')

    tokens = init_tokens(stock_ids)
    ok = detect_api_mode(tokens['price'], stock_ids)
    if not ok:
        raise RuntimeError('❌ FinMind API 連線失敗，請檢查 Token')

    price_data = fetch_all_prices(stock_ids, tokens['price'])
    valid_ids  = list(price_data.keys())
    inst_data  = fetch_all_inst(valid_ids, tokens['inst'])
    fin_data   = fetch_all_revenue(valid_ids, tokens['rev'])

    strong_df, strong_candidates = run_strong_filter(price_data, inst_data, fin_data, name_map)
    early_df,  early_candidates  = run_early_filter(price_data, inst_data, fin_data, name_map)

    csv_fname, full_out = export_csv(price_data, inst_data, fin_data, name_map, strong_df, early_df)

    n_scanned = len(price_data)
    send_telegram(strong_df, early_df, len(strong_candidates), len(early_candidates), n_scanned)
    send_email(csv_fname, strong_df, early_df, len(strong_candidates), len(early_candidates), n_scanned)

    print("\n" + "=" * 60)
    print(f"✅ Twgogogo_{TODAY_STR}.csv 已產生")
    print("=" * 60)

if __name__ == '__main__':
    main()
