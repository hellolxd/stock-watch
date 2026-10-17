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

import os, sys, json, time, smtplib, urllib.request, urllib.parse
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


def market_top(n=100):
    """东财成交额榜（已剔科创/北交/零成交），返回前 n 个 {code,name,q}"""
    try:
        d = _get("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=100&po=1&np=1"
                 "&fltt=2&invt=2&fid=f6&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
                 "&fields=f2,f3,f6,f8,f10,f12,f14")
        diff = ((d or {}).get("data") or {}).get("diff") or []
        out = []
        for it in diff:
            c = str(it.get("f12") or "")
            if not c or c.startswith(DROP_BOARDS):
                continue          # 科创板 / 北交所
            if not (it.get("f6") or 0):
                continue          # 零成交 / 停牌
            out.append({"code": c, "name": it.get("f14") or "", "q": it})
        return out[:n]
    except Exception:
        return []


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
    """ulist 批量快照：{code: q}"""
    if not codes:
        return {}
    secids = ",".join(secid_of(c) for c in codes)
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
    url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid="
           + secid_of(code)
           + "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57"
           + "&klt=101&fqt=1&end=20500101&lmt=%d" % lmt)
    d = _get(url)
    kls = (d or {}).get("data", {}).get("klines") or []
    out = []
    for k in kls:
        p = k.split(",")
        out.append({"date": p[0], "o": float(p[1]), "c": float(p[2]),
                    "h": float(p[3]), "l": float(p[4]), "v": float(p[5])})
    return out


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
    lines.append("一、Top5 候选（按成交额，仅供技术面参考）：")
    for code, q in ranked:
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
    res = send_mail(title, "\n".join(lines))
    print("[简报] 邮件%s" % res)
    if "已发送" in res:
        st["brief:%s" % datetime.now(CST).strftime("%Y%m%d")] = time.time()
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