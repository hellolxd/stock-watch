#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cloud_watch.py —— 云端持仓监测 + 邮件提醒
================================================
作用：部署在 GitHub Actions 上定时运行。读取 watchlist.json 里的持仓，
实时拉取行情与 K 线，判断每一只是否出现「卖出/减仓」信号，命中则发邮件。

用法：
  python cloud_watch.py --config watchlist.json --state _state.json
  python cloud_watch.py --dry-run --config watchlist.json   # 只打印信号，不发邮件（本地自测）

依赖：Python 3 标准库（urllib / smtplib / json），无需安装第三方包。

环境变量（GitHub Actions 里用 Secrets 配置，见部署手册）：
  SMTP_USER  发件邮箱，如 1176431829@qq.com
  SMTP_PASS  QQ 邮箱授权码（不是登录密码，需开通 SMTP 后获取）
  MAIL_TO    收件邮箱（默认同发件人）
"""

import os
import json
import sys
import time
import smtplib
import urllib.request
import urllib.parse
from email.mime.text import MIMEText
from email.header import Header
from datetime import timezone, timedelta

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.qq.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
MAIL_TO = os.environ.get("MAIL_TO", SMTP_USER or "")
DEDUP_HOURS = float(os.environ.get("DEDUP_HOURS", "24"))  # 同一信号 24 小时内只推一次
CST = timezone(timedelta(hours=8))                        # 北京时间


# ---------- 行情获取 ----------
def _get(url, timeout=15, tries=4):
    """带重试的 JSON 请求，东财接口偶发断开/限流"""
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*",
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    raise last


def secid_of(code):
    return ("1." if code[0] == "6" else "0.") + code


def fetch_quotes(codes):
    """返回 {code: quote}，quote: f2现价 f3涨跌幅 f8换手 f10量比 f6成交额 f12代码 f14名称"""
    if not codes:
        return {}
    secids = ",".join(secid_of(c) for c in codes)
    url = ("https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids="
           + urllib.parse.quote(secids) + "&fields=f2,f3,f4,f6,f8,f10,f12,f14")
    try:
        d = _get(url)
    except Exception:
        return {}
    diff = (d or {}).get("data", {}).get("diff")
    out = {}
    if isinstance(diff, list):
        for it in diff:
            if it and it.get("f12"):
                out[str(it["f12"])] = it
    return out


def fetch_kline(code, lmt=60):
    """返回 [{date,o,c,h,l,v}]，失败返回空列表"""
    for att in range(3):
        try:
            url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid="
                   + secid_of(code) + "&fields1=f1,f2,f3,f4,f5,f6"
                   + "&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
                   + "&klt=101&fqt=1&end=20500101&lmt=" + str(lmt))
            d = _get(url)
            kls = (d or {}).get("data", {}).get("klines")
            if isinstance(kls, list) and kls:
                out = []
                for k in kls:
                    p = k.split(",")
                    out.append({"date": p[0], "o": float(p[1]), "c": float(p[2]),
                                "h": float(p[3]), "l": float(p[4]), "v": float(p[5])})
                return out
        except Exception:
            pass
        time.sleep(0.4 * (att + 1))
    return []


# ---------- 技术指标 ----------
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


# ---------- 卖出信号判断（与网页版保持一致） ----------
def analyze(h, kl, cur):
    cost = float(h.get("cost", 0) or 0)
    pnl = (cur - cost) / cost * 100 if cost > 0 else 0.0
    closes = [k["c"] for k in kl]
    ma5, ma10, ma20 = ma_of(closes, 5), ma_of(closes, 10), ma_of(closes, 20)
    r = rsi_of(closes)
    take = float(h.get("take") or 20)
    stop = float(h.get("stop") or -10)
    tag, reasons = "持有", []
    fall2 = len(closes) >= 2 and closes[-1] < closes[-2]
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
    else:
        tag, reasons = "持有", ["趋势健康，继续持有"]
    if ma20 is not None:
        reasons.append("MA20=%.2f" % ma20)
    return tag, pnl, reasons


SELL_KEY = ("止损", "止盈", "破位", "超买", "弱势")


# ---------- 邮件推送 ----------
def send_mail(title, body):
    if not (SMTP_USER and SMTP_PASS):
        return "未配置 SMTP_USER/SMTP_PASS，跳过发送"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(title, "utf-8")
    msg["From"] = SMTP_USER
    msg["To"] = MAIL_TO
    try:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20)
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, [MAIL_TO], msg.as_string())
        s.quit()
        return "已发送"
    except Exception as e:
        return "发送失败: %s" % e


# ---------- 主流程 ----------
def main():
    dry = "--dry-run" in sys.argv
    config = "watchlist.json"
    state_path = "_state.json"
    args = sys.argv
    for i, a in enumerate(args):
        if a == "--config" and i + 1 < len(args):
            config = args[i + 1]
        if a == "--state" and i + 1 < len(args):
            state_path = args[i + 1]

    with open(config, "r", encoding="utf-8") as f:
        items = json.load(f)
    if not isinstance(items, list) or not items:
        print("watchlist 为空，退出")
        return

    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    quotes = fetch_quotes([h.get("code", "") for h in items])
    changed = False

    for h in items:
        code = h.get("code", "")
        q = quotes.get(code)
        if not q or q.get("f2") is None:
            print("[跳过] %s 未取到行情" % code)
            continue
        cur = q["f2"]
        kl = fetch_kline(code, 60)
        tag, pnl, reasons = analyze(h, kl, cur)
        if not any(s in tag for s in SELL_KEY):
            print("[持有] %s %s %s" % (h.get("name"), tag, "；".join(reasons)))
            continue
        key = "%s:%s" % (code, tag)
        last = state.get(key, 0)
        if last and (time.time() - last) < DEDUP_HOURS * 3600:
            print("[去重] %s %s 在去重周期内，不重复推送" % (h.get("name"), tag))
            continue
        name = h.get("name", code)
        title = "卖出提醒：%s %s" % (name, tag)
        body = ("%s（%s）出现「%s」信号\n"
                "现价 %.2f    当日涨跌 %s\n"
                "持仓盈亏 %+.2f%%\n"
                "%s\n"
                "—— 来自云端股票监测（仅供技术面参考）"
                % (name, code, tag, cur,
                   ("+%.2f%%" % q["f3"]) if q.get("f3") else "-",
                   pnl, "；".join(reasons)))
        print("[信号] %s → %s" % (name, tag))
        if dry:
            print("  (dry-run) 主题：%s\n  正文：%s" % (title, body.replace("\n", " | ")))
            continue
        res = send_mail(title, body)
        print("  邮件%s" % res)
        if "已发送" in res:
            state[key] = time.time()
            changed = True

    if changed:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        print("已更新状态文件", state_path)


if __name__ == "__main__":
    main()