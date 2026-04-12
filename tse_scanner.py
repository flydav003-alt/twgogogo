# ═══ HTML 報告（暗紫科技 + 橘金標題）═══
# ★ v3.0 修改：股票代碼欄位自動加上 Yahoo 股市連結
# 連結格式：https://tw.stock.yahoo.com/quote/{代碼}.TW
# 每天篩選出的任何代碼都會自動生成對應連結，不需手動維護

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

    # ★ 新增：自動產生 Yahoo 股市連結的輔助函式
    def yahoo_link(code, color, label=None):
        display = label or code
        url = f'https://tw.stock.yahoo.com/quote/{code}.TW'
        return (
            f'<a href="{url}" target="_blank" rel="noopener" '
            f'style="color:{color};font-weight:700;text-decoration:none;'
            f'display:inline-flex;align-items:center;gap:3px;transition:opacity .15s;" '
            f'onmouseover="this.style.opacity=\'0.75\'" '
            f'onmouseout="this.style.opacity=\'1\'">'
            f'{display}'
            f'<svg width="10" height="10" viewBox="0 0 10 10" fill="none" '
            f'xmlns="http://www.w3.org/2000/svg" style="opacity:.6">'
            f'<path d="M2 8L8 2M8 2H4M8 2V6" stroke="currentColor" '
            f'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
            f'</svg></a>'
        )

    def build_chart_html(charts,df_ref,scol='total_score'):
        h=''
        for sid,b64 in charts.items():
            name=nm.get(sid,sid); row=df_ref[df_ref['stock_id']==sid] if not df_ref.empty else pd.DataFrame()
            cap=f'{sid} {name}' if row.empty else f'#{int(row.iloc[0]["rank"])} {sid} {name} | {fn(row.iloc[0].get(scol,0))}'
            h+=f'<div class="chart-wrap"><div class="chart-caption">{cap}</div><img src="data:image/png;base64,{b64}" style="width:90%;border-radius:6px;margin:12px auto;display:block"/></div>'
        return h

    # ══ 強勢確認表格 ══
    sr=''
    if not sdf.empty:
        for _,r in sdf.head(TOP_STRONG).iterrows():
            rk=int(r['rank']); m=['🥇','🥈','🥉'][rk-1] if rk<=3 else f'#{rk}'
            yr=fin.get(r['stock_id'],None)
            sr+=f'<tr><td style="text-align:center">{m}</td>'
            # ★ 改這裡：代碼欄位改為 Yahoo 連結（紅色）
            sr+=f'<td>{yahoo_link(r["stock_id"], "#fb7185")}</td>'
            sr+=f'<td style="font-weight:600">{r["name"]}</td>'
            sr+=f'<td><span class="badge-score" style="background:linear-gradient(135deg,#3a1520,#6b2535)">{fn(r["total_score"])}</span></td>'
            sr+=f'<td style="font-size:.82em">{r.get("signal_b","")}<br><span style="color:#7c7894">{r.get("signal_c","")}</span></td>'
            sr+=f'<td style="font-weight:600">{fn(r["close"],1)}</td><td>{ft(r["turnover_today"])}</td><td>{fn(r["vol_ratio"])}x</td>'
            sr+=f'<td>{pc(r["ma28_bias"])}</td><td>{pc(r["daily_return_pct"])}</td><td>{rc(r["rsi14"])}</td>'
            sr+=f'<td>{r["inst_consec"]}天</td><td>{sb(r["strength"])}</td><td>{fy(yr)}</td></tr>'
    else: sr='<tr><td colspan="14" style="text-align:center;color:#7c7894;padding:24px">今日無符合條件個股</td></tr>'

    # ══ 起漲預警表格 ══
    er=''
    if not edf.empty:
        for _,r in edf.head(TOP_EARLY).iterrows():
            rk=int(r['rank']); m=['🌱','🌿','🍃'][rk-1] if rk<=3 else f'#{rk}'
            er+=f'<tr><td style="text-align:center">{m}</td>'
            # ★ 改這裡：代碼欄位改為 Yahoo 連結（青色）
            er+=f'<td>{yahoo_link(r["stock_id"], "#2dd4bf")}</td>'
            er+=f'<td>{r["name"]}</td>'
            er+=f'<td><span class="badge-score" style="background:linear-gradient(135deg,#0a2520,#1a5545)">{fn(r["total_ew_score"])}</span></td>'
            er+=f'<td>{fn(r["close"],1)}</td><td>{ft(r["turnover_today"])}</td><td>{fn(r["vol_ratio"])}x</td>'
            er+=f'<td>{pc(r["ma28_bias"])}</td><td>{pc(r["daily_return_pct"])}</td><td>{rc(r["rsi14"])}</td>'
            er+=f'<td>{fn(r["consol_ratio"])}</td><td>{fy(r["yoy_revenue_pct"])}</td><td>{r["inst_consec_days"]}天</td></tr>'
    else: er='<tr><td colspan="13" style="text-align:center;color:#7c7894;padding:24px">今日無起漲預警</td></tr>'

    # ══ 綜合轉強表格 ══
    comp_df=full_out[full_out['composite_score']>0].sort_values('composite_score',ascending=False).head(TOP_COMPOSITE).reset_index(drop=True)
    cr=''
    if not comp_df.empty:
        medals=['🏅','🎖️','⭐','✨','💫']
        for i,(_,r) in enumerate(comp_df.iterrows()):
            cr+=f'<tr><td style="text-align:center">{medals[i] if i<5 else "▪️"}</td>'
            # ★ 改這裡：代碼欄位改為 Yahoo 連結（紫色）
            cr+=f'<td>{yahoo_link(r["stock_id"], "#c084fc")}</td>'
            cr+=f'<td style="font-weight:600">{r["name"]}</td>'
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
    comp_df2 = comp_df.copy()
    if not comp_df2.empty:
        comp_df2 = comp_df2.drop(columns=['rank'], errors='ignore')
        comp_df2.insert(0, 'rank', range(1, len(comp_df2) + 1))
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
<div class="legend"><div><span class="dot" style="background:var(--accent)"></span> 綜合分 = early×{COMPOSITE_EARLY_W} + total×{COMPOSITE_TOTAL_W}</div><div style="color:var(--accent)">↗ 點擊代碼直接開 Yahoo 股市走勢圖</div></div></div>

<div class="section" id="early-section"><div class="section-header early"><div class="section-icon">🌱</div><div class="section-title"><h2>即將起漲潛力股 Top {TOP_EARLY}</h2><p>硬條件過濾+加分排名</p></div><a href="#early-charts" class="chart-link">K線圖 ↓</a></div>
<div class="table-wrap"><table><thead><tr><th>排名</th><th>代碼</th><th>名稱</th><th>總分</th><th>收盤價</th><th>成交值</th><th>量比</th><th>MA28乖離</th><th>漲幅%</th><th>RSI14</th><th>收斂比</th><th>營收YoY</th><th>法人連買</th></tr></thead><tbody>{er}</tbody></table></div>
<div class="legend"><div><span class="dot" style="background:var(--teal)"></span> YoY>{EW_BONUS_YOY_THRESHOLD:.0f}%→+{EW_BONUS_YOY:.0f} ｜ 法人連買≥2天→+{EW_BONUS_INST:.0f} ｜ 60日<25%→+{EW_BONUS_60D:.0f}</div><div style="color:var(--teal)">↗ 點擊代碼直接開 Yahoo 股市走勢圖</div></div></div>

<div class="section" id="strong-section"><div class="section-header strong"><div class="section-icon">🔥</div><div class="section-title"><h2>強勢確認股 Top {TOP_STRONG}</h2><p>量價齊揚+法人認同+技術突破</p></div><a href="#strong-charts" class="chart-link">K線圖 ↓</a></div>
<div class="table-wrap"><table><thead><tr><th>排名</th><th>代碼</th><th>名稱</th><th>總分</th><th>訊號</th><th>收盤價</th><th>成交值</th><th>量比</th><th>MA28乖離</th><th>漲幅%</th><th>RSI14</th><th>連買天</th><th>強弱</th><th>營收YoY</th></tr></thead><tbody>{sr}</tbody></table></div>
<div class="legend"><div><span class="dot" style="background:var(--coral)"></span> 量價子分數×1.1 + 連買天數×{INST_CONSEC_WEIGHT} + Z-score</div><div><span style="color:var(--red)">RSI⚠️</span> ≥78 追高需謹慎</div><div style="color:var(--coral)">↗ 點擊代碼直接開 Yahoo 股市走勢圖</div></div></div>

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
