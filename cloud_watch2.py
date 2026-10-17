#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cloud_watch2.py —— 云端统一监测（在已验证的 cloud_watch.py 基础上扩展）
====================================================================
三种任务（由 --task 选择，可被 GitHub Actions 定时触发）：

  sell   持仓卖出提醒（沿用 v1 的 analyze 逻辑 + 24h 去重 + 邮件）
  buy    开盘/盘中买点监测：全市场打分 → 取综合分前5 → 对前5名逐只
         判定买点（回踩企稳/放量突破/超卖反弹）→ 命中发邮件；
         开盘时段(9:30-10:29)每 10 分钟一次，其余交易时段每 30 分钟，
         由 workflow 的两个 cron 分档实现，脚本内不再自行定时。
  brief  每交易日 9:10 盘前简报：Top5 候选 + 隔夜新闻摘要，一封邮件。

用法：
  python cloud_watch2.py --task sell
  python cloud_watch2.py --task buy
  python cloud_watch2.py --task brief
  加 --dry-run 只打印不发送。

依赖：仅 Python3 标准库。环境变量（Secrets）：
  SMTP_USER / SMTP_PASS / MAIL_TO   —— 同 v1
"""

import os, sys, json, time, re, smtplib, urllib.request, urllib.parse
from email.mime.text import MIMEText
from email.header import Header
from datetime import datetime, timedelta, timezone

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.qq.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
MAIL_TO   = os.environ.get("MAIL_TO", SMTP_USER or "")
CST = timezone(timedelta(hours=8))
DEDUP_HOURS = float(os.environ.get("DEDUP_HOURS", "24"))

WATCHLIST = "watchlist.json"
STATE     = "_state.json"

# 卖出信号关键词（与网页/云端 v1 完全一致）——上行保持不动
SELL_KEY  = ("止损", "止盈", "破位", "超买", "弱势")
# 买点类型（与网页 buyPointOf 一致）
BUY_KINDS = ("放量突破", "回踩企稳", "超卖反弹")

# 全市场精选（约1000只）：剔除科创板(688/689)、北交所(43/83/87/92)与零成交个股，与网页端一致
DROP_BOARDS = ("688", "689", "43", "83", "87", "92")


# 云端精选池（与网页 RAW_POOL 对齐的人工整理清单；剔除 688/北交）
MARKET_BASE = [
    "600519","601318","600036","600030","601899","600900","601857","600028","601088","601398",
    "601939","601288","601166","600000","601998","601818","600016","601328","601169","601229",
    "600919","600926","601128","002142","601628","601336","601319","601601","600999","601211",
    "601066","601688","601881","600918","601788","600570","300059","600276","600809","600887",
    "603288","600196","600085","600422","600521","603259","600763","600779","600132","600702",
    "603369","603589","603345","600298","600690","600872","600859","600637","600839","601989",
    "601985","600795","600886","600674","600011","601991","600362","600111","603993","600547",
    "600988","600219","600010","600019","600663","600104","601633","601238","601127","600438",
    "601012","600585","600031","601766","600150","601390","601668","601800","601186","601618",
    "601669","601138","603501","600941","601728","600018","600009","601111","600029","601006",
    "601816","600487","600498","600486","603260","600989","600096","600141","600309","600346",
    "000001","000002","000333","000651","000858","000568","000725","002415","002594","002475",
    "002714","002304","002371","002230","002027","002352","300750","300760","300015","300122",
    "300274","300124","300308","300502","300033","300454","300014","300408","300628","300661",
    "300782","300347","300003","300529","300633","300896","300999","300979","301269","300413",
    "300433","301235","002460","002466","000338","000625","000977","002241","000063","000100",
    "000157","002008","002049","000538","000596","000799","000895","000983","002493","002648",
    "001979","000049","002444","002507","002568","002812","002821","000661","000963","000999",
    "002007","002311","000708","000932","000898","000550","000876","002405","002555","002726",
]


def market_top(n=100):
    """成交额榜（腾讯源+内置精选池，已剔科创/北交/零成交/ST/退市），返回前 n 个 {code,name,q}"""
    try:
        qs = batch_quotes(MARKET_BASE)
    except Exception:
        qs = {}
    pool = []
    for c, q in qs.items():
        name = str(q.get("f14") or "")
        if not c or c.startswith(DROP_BOARDS):
            continue
        if not (q.get("f6") or 0):
            continue
        if name.startswith("退") or "ST" in name:
            continue
        pool.append({"code": c, "name": name, "q": q})
    pool.sort(key=lambda t: -(t["q"].get("f6") or 0))
    return pool[:n]


# ---------------- 基础工具 ----------------
def _get(url, timeout=15, tries=4):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    raise last


def secid_of(code):
    return ("1." if code[0] == "6" else "0.") + code


def load_watchlist():
    with open(WATCHLIST, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def load_state():
    if os.path.exists(STATE):
        try:
            with open(STATE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(st):
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


def batch_quotes(codes):
    """腾讯 qt.gtimg.cn 批量快照（主源）：{code: q}
    字段映射 → 与东财 ulist 同形：f2现价 f3涨跌% f6成交额(元) f8换手% f10量比 f14名称
    （2026-10-17 修正：量比取腾讯 f49；f31 是当日涨跌额，旧版误用 f31）"""
    if not codes:
        return {}
    out_all = {}
    for i in range(0, len(codes), 60):
        part = codes[i:i + 60]
        q = ",".join((("sh" if c[0] in "65" else "sz") + c) for c in part)
        try:
            req = urllib.request.Request("https://qt.gtimg.cn/q=" + q, headers={
                "User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
            with urllib.request.urlopen(req, timeout=15) as r:
                raw = r.read()
            txt = raw.decode("gbk", "ignore")
            for seg in txt.split(";"):
                m = re.search(r'v_\w{2}(\d{6})="([^"]*)"', seg)
                if not m:
                    continue
                f = (m.group(2) or "").split("~")
                if len(f) < 39:
                    continue
                try:
                    price = float(f[3]); prev = float(f[4])
                except Exception:
                    continue
                if not (price > 0):
                    continue
                out_all[m.group(1)] = {
                    "f12": m.group(1), "f14": f[1], "f2": price,
                    "f3": round((price - prev) / prev * 100, 2) if prev > 0 else 0.0,
                    "f10": float(f[49]) if (len(f) > 49 and f[49]) else 0.0,
                    "f8": float(f[38]) if f[38] else 0.0,
                    "f6": (float(f[37]) if f[37] else 0.0) * 1e4,
                }
        except Exception:
            continue
        time.sleep(0.12)
    # 腾讯为空时备源切东财 ulist
    if not out_all:
        return batch_quotes_em(codes)
    return out_all


def batch_quotes_em(codes):
    """备源：东财 ulist"""
    if not codes:
        return {}
    secids = ",".join(secid_of(c) for c in codes[:45])
    url = ("https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids="
           + urllib.parse.quote(secids) + "&fields=f2,f3,f4,f6,f8,f10,f12,f14")
    d = _get(url)
    diff = (d or {}).get("data", {}).get("diff") or []
    out = {}
    if isinstance(diff, list):
        for it in diff:
            if it and it.get("f12"):
                out[str(it["f12"])] = it
    return out


def kline_of(code, lmt=70):
    """K线：主源腾讯 mkline(m60聚合日K)，备源东财"""
    sym = ("sh" if code[0] in "65" else "sz") + code
    try:
        d = _get("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/mkline?param=%s,m60,,600" % sym)
        node = ((d or {}).get("data") or {}).get(sym) or {}
        mk = node.get("m60") or []
        by_date, order = {}, []
        for row in mk:
            dstr = (row[0] or "")[:10]
            if not dstr:
                continue
            if dstr not in by_date:
                by_date[dstr] = {"date": dstr, "o": float(row[1]), "h": float(row[3]),
                                 "l": float(row[4]), "c": float(row[2]), "v": float(row[5])}
                order.append(dstr)
            else:
                b = by_date[dstr]
                b["h"] = max(b["h"], float(row[3])); b["l"] = min(b["l"], float(row[4]))
                b["c"] = float(row[2]); b["v"] += float(row[5])
        days = [by_date[k] for k in order]
        qt = node.get("qt") or []
        if days and qt and len(qt) > 3 and float(qt[3]) > 0:
            days[-1]["c"] = float(qt[3])
        if days:
            return days[-lmt:]
    except Exception:
        pass
    # 备源：东财 kline
    try:
        url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid="
               + secid_of(code)
               + "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57"
               + "&klt=101&fqt=1&end=20500101&lmt=%d" % lmt)
        d = _get(url)
        kls = (d or {}).get("data", {}).get("klines") or []
        return [{"date": k.split(",")[0], "o": float(k.split(",")[1]), "c": float(k.split(",")[2]),
                 "h": float(k.split(",")[3]), "l": float(k.split(",")[4]), "v": float(k.split(",")[5])}
                for k in kls]
    except Exception:
        return []


def score_of(q):
    """与网页 scoreOf 一致的基础分（用于 buy 任务挑选 Top5）"""
    s = 50.0
    zf = q.get("f3") or 0
    if zf > 0 and zf <= 7:
        s += zf * 2.5
    elif zf > 7:
        s += 8
    else:
        s += zf * 1.2
    lb = q.get("f10") or 1
    s += 8 if 1 <= lb <= 3 else (2 if 0 < lb < 1 else 0)
    hs = q.get("f8") or 0
    s += 6 if 2 <= hs <= 10 else (2 if hs > 10 else 0)
    amt = q.get("f6") or 0
    if amt > 0:
        import math
        s += min(12, max(0, math.log10(amt + 1) - 7))
    return round(s, 1)


# ---------------- 技术指标（与网页一致） ----------------
def ma_of(closes, n):
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def rsi_of(closes, n=14):
    if len(closes) < n + 1:
        return None
    up = dn = 0.0
    for i in range(len(closes) - n, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0:
            up += d
        else:
            dn -= d
    if dn == 0:
        return 100.0
    return 100 - 100 / (1 + up / dn)


def buy_point(q, kl):
    """镜像网页 buyPointOf：返回买点类型字符串或 None"""
    if not q or not kl or len(kl) < 25:
        return None
    closes = [k["c"] for k in kl]
    cur = closes[-1]
    chg = q.get("f3") or 0
    lb = q.get("f10") or 1
    ma5 = ma_of(closes, 5)
    ma10 = ma_of(closes, 10)
    ma20 = ma_of(closes, 20)
    r = rsi_of(closes)
    if ma5 is None or ma10 is None or ma20 is None or r is None:
        return None
    bull = ma5 > ma10 > ma20
    recent_high = max((k["h"] or 0) for k in kl[-6:-1]) if len(kl) >= 6 else None
    if recent_high and 1 <= chg <= 7 and cur > recent_high and lb >= 1.5 and r < 85 and bull:
        return "放量突破"
    if -1 <= chg <= 5 and bull and ma5 <= cur <= ma5 * 1.02 and 0.8 <= lb <= 3 and 45 <= r < 75:
        return "回踩企稳"
    if r < 35 and chg > 0 and lb >= 1:
        return "超卖反弹"
    return None


def sell_analyze(h, q, kl):
    """镜像 v1 analyze（卖出信号） → (tag, pnl, reasons)"""
    cost = float(h.get("cost") or 0)
    cur = float(q.get("f2") or 0)
    pnl = (cur - cost) / cost * 100 if cost > 0 else 0.0
    closes = [k["c"] for k in kl]
    ma20 = ma_of(closes, 20)
    ma10 = ma_of(closes, 10)
    r = rsi_of(closes)
    fall2 = len(closes) >= 2 and closes[-1] < closes[-2]
    take = float(h.get("take") or 20)
    stop = float(h.get("stop") or -10)
    tag, reasons = "持有", []
    if pnl <= stop:
        tag, reasons = "止损卖出", ["已跌破止损线 %.0f%%" % stop]
    elif pnl >= take:
        tag, reasons = "止盈卖出", ["已达止盈目标 +%.0f%%" % take]
    elif ma20 is not None and cur < ma20 and fall2:
        tag, reasons = "破位减仓", ["跌破20日线且重心下移，趋势走弱"]
    elif r is not None and r > 80:
        tag, reasons = "超买减仓", ["RSI %.0f 超买，逢高减仓" % r]
    elif ma10 is not None and cur < ma10 and fall2:
        tag, reasons = "弱势减仓", ["跌破10日线且重心下移"]
    elif r is not None and r < 55:
        tag, reasons = "持有观望", ["RSI %.0f 偏弱，等待企稳" % r]
    if ma20 is not None:
        reasons.append("MA20=%.2f" % ma20)
    return tag, pnl, reasons


# ---------------- 新闻（新浪滚动，与网页同源） ----------------
def fetch_news(limit=15):
    try:
        d = _get("https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&k=&num=%d&page=1" % limit)
        items = ((d or {}).get("result") or {}).get("data") or []
        return [it.get("title", "").strip() for it in items if it.get("title")]
    except Exception:
        return []


# ---------------- 邮件 ----------------
def send_mail(title, body):
    if not (SMTP_USER and SMTP_PASS):
        return "未配置 SMTP"
    m = MIMEText(body, "plain", "utf-8")
    m["Subject"] = Header(title, "utf-8")
    m["From"] = SMTP_USER
    m["To"] = MAIL_TO
    try:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20)
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, [MAIL_TO], m.as_string())
        s.quit()
        return "已发送"
    except Exception as e:
        return "发送失败: %s" % e


def dedup_key(kind, code, tag):
    return "%s:%s:%s" % (kind, code, tag)


# ---------------- 任务 ----------------
def task_sell(st):
    qs = batch_quotes(load_watchlist_codes())
    hits = []
    for h in load_watchlist():
        code = h.get("code", "")
        q = qs.get(code)
        if not q:
            print("[跳过] %s 未取到行情" % code)
            continue
        kl = kline_of(code, 70)
        tag, pnl, reasons = sell_analyze(h, q, kl)
        if tag == "持有" and not any(s in tag for s in SELL_KEY):
            continue
        if not any(s in tag for s in SELL_KEY):
            continue
        key = dedup_key("sell", code, tag)
        if st.get(key) and time.time() - st[key] < DEDUP_HOURS * 3600:
            print("[去重] %s %s" % (h.get("name"), tag))
            continue
        name = h.get("name", code)
        title = "卖出提醒：%s %s" % (name, tag)
        body = ("%s（%s）触发「%s」信号\n"
                "现价 %.2f  当日涨跌 %s%%\n"
                "盈亏 %+.2f%%\n"
                "%s\n—— 云端监测（技术面参考，不构成投资建议）"
                % (name, code, tag, q.get("f2") or 0, q.get("f3") or 0, pnl, "；".join(reasons)))
        res = send_mail(title, body)
        print("[卖出] %s → %s（邮件%s）" % (name, tag, res))
        if "已发送" in res:
            st[key] = time.time()
            hits.append(code)
    return hits


def task_buy(st):
    """全市场打分 → Top5 → 买点判定 → 邮件（含开盘时段说明）"""
    codes = load_watchlist_codes()
    # 买入监测针对综合分靠前的股票；云端用「成交额榜单」近似：拉全市场前 100 页太多，
    # 这里改为：watchlist 内所有持仓 + 说明；若 watchlist 为空则直接用东财涨幅榜 Top100。
    top = market_top(100)
    # market_top 已剔除科创板(688/689)、北交所(43/83/87/92)与零成交个股（约1000只池）
    # 补上持仓里的票
    for h in load_watchlist():
        code = h.get("code", "")
        if code and not any(t["code"] == code for t in top):
            q = batch_quotes([code]).get(code)
            if q:
                top.append({"code": code, "name": h.get("name", code), "q": q})
    if not top:
        print("[buy] 无可监测标的")
        return []
    hits = []
    for t in top[:5]:
        q = t.get("q") or {}
        code = t["code"]
        name = t.get("name") or code
        kl = kline_of(code, 70)
        bp = buy_point(q, kl)
        if not bp:
            continue
        key = dedup_key("buy", code, bp)
        if st.get(key) and time.time() - st[key] < DEDUP_HOURS * 3600:
            print("[去重] %s %s" % (name, bp))
            continue
        title = "买入提醒：%s %s（%s）" % (name, bp, datetime.now(CST).strftime("%m-%d %H:%M"))
        body = ("%s（%s）出现「%s」买点\n"
                "现价 %s   涨幅 %s%%   量比 %s\n"
                "提示：开盘时段每 10 分钟自动检测一次；同一买点 24 小时只提醒一次。\n"
                "—— 技术面参考，不构成投资建议" %
                (name, code, bp, q.get("f2") or kl[-1]["c"] if kl else "-",
                 q.get("f3") or 0, q.get("f10") or 1))
        res = send_mail(title, body)
        print("[买入] %s %s → %s" % (name, bp, res))
        if "已发送" in res:
            st[key] = time.time()
            hits.append(code)
    return hits


def task_brief(st):
    """盘前简报：Top5（全市场精选池，剔除科创/北交/零成交） + 新闻"""
    top = market_top(100)
    qs = batch_quotes(load_watchlist_codes())
    for h in load_watchlist():
        code = h.get("code", "")
        if code and not any(t["code"] == code for t in top):
            q = qs.get(code)
            if q:
                top.append({"code": code, "name": h.get("name", code), "q": q})
    ranked = sorted(top, key=lambda t: -((t.get("q") or {}).get("f6") or 0))[:5]
    news = fetch_news(15)
    lines = ["【盘前简报】%s" % datetime.now(CST).strftime("%Y-%m-%d %H:%M"), ""]
    lines.append("一、Top5 候选（按成交额，腾讯源，仅供技术面参考）：")
    for t in ranked:
        code = t.get("code")
        q = t.get("q") or {}
        kl = kline_of(code, 70)
        bp = buy_point(q, kl)
        tagstr = ("出现买点「%s」" % bp) if bp else "暂无买点信号"
        lines.append("  · %s(%s) 现价 %.2f  涨幅 %.2f%%  —— %s"
                     % (q.get("f14") or code, code, q.get("f2") or 0, q.get("f3") or 0, tagstr))
    lines.append("")
    if news:
        lines.append("二、隔夜要闻（新浪滚动）：")
        for n in news:
            lines.append("  · " + n)
    else:
        lines.append("二、隔夜要闻：接口暂不可用，已跳过")
    title = "盘前简报 %s（Top5+新闻）" % datetime.now(CST).strftime("%m-%d")
    body = "\n".join(lines)
    if "--dry-run" in sys.argv:
        print("[简报·dry] 标题：%s\n%s" % (title, body[:600]))
        return []
    res = send_mail(title, body)
    print("[简报] 邮件%s" % res)
    if "已发送" in res:
        st["brief:%s" % datetime.now(CST).strftime("%Y%m%d")] = time.time()
        # 返回非空 hits，让 main() 落盘 _state.json（brief:YYYYMMDD 日去重 + state 可追溯）
        return ["brief"]
    return []


def load_watchlist_codes():
    return [h.get("code", "") for h in load_watchlist()]


# ---------------- 入口 ----------------
def main():
    task = "sell"
    for i, a in enumerate(sys.argv):
        if a == "--task" and i + 1 < len(sys.argv):
            task = sys.argv[i + 1]
    dry = "--dry-run" in sys.argv
    st = load_state()
    if task == "sell":
        hits = task_sell(st)
    elif task == "buy":
        hits = task_buy(st)
    elif task == "brief":
        hits = task_brief(st)
    else:
        print("未知 task:", task)
        return
    if hits and not dry:
        save_state(st)
        print("任务完成，命中：", hits)


if __name__ == "__main__":
    main()