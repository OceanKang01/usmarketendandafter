import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import yfinance as yf

KR_NAME = {
    "MU": "마이크론",
    "NVDA": "엔비디아",
}


KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")

STATE_PATH = "state.json"

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_USER_ID = int(os.environ["TELEGRAM_ALLOWED_USER_ID"])

# optional: 특정 chat_id만 허용하고 싶다면
FORCE_CHAT_ID = os.environ.get("TELEGRAM_FORCE_CHAT_ID")
FORCE_CHAT_ID = int(FORCE_CHAT_ID) if FORCE_CHAT_ID else None

TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"


# ---------------------------
# State I/O
# ---------------------------
def load_state() -> dict:
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------
# Telegram
# ---------------------------
def tg_get_updates(offset: int):
    r = requests.get(
        f"{TG_BASE}/getUpdates",
        params={"timeout": 20, "offset": offset},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("result", [])


def tg_send(chat_id: int, text: str):
    r = requests.post(
        f"{TG_BASE}/sendMessage",
        data={"chat_id": chat_id, "text": text},
        timeout=20,
    )
    r.raise_for_status()


def normalize_ticker(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9\.\-\_]", "", s.strip()).upper()


def parse_cmd(text: str):
    """
    지원:
      /start
      /list
      /add MU
      /add MU,NVDA,TSLA
      /del NVDA
      /del NVDA,TSLA
    """
    text = (text or "").strip()
    if not text.startswith("/"):
        return None, None

    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    # 콤마/공백 혼합 입력 처리
    tickers = []
    if arg:
        raw = re.split(r"[,\s]+", arg.strip())
        tickers = [normalize_ticker(x) for x in raw if normalize_ticker(x)]

    return cmd, tickers


def handle_updates(state: dict) -> bool:
    """
    텔레그램 업데이트를 폴링하여 state 반영.
    - allowed_user_id 외는 무시
    - chat_id 자동 저장
    - /add /del /list 처리
    - last_update_id 갱신
    반환: state가 변경되었는지
    """
    changed = False
    offset = int(state.get("last_update_id", 0)) + 1

    updates = tg_get_updates(offset=offset)
    if not updates:
        return False

    for u in updates:
        state["last_update_id"] = max(state.get("last_update_id", 0), u.get("update_id", 0))
        changed = True

        msg = u.get("message") or u.get("edited_message")
        if not msg:
            continue

        from_user = msg.get("from", {})
        user_id = from_user.get("id")
        chat_id = (msg.get("chat") or {}).get("id")
        text = msg.get("text", "")

        # 보안: 허용 유저만
        if user_id != ALLOWED_USER_ID:
            continue

        # optional: 특정 채팅방만 허용
        if FORCE_CHAT_ID and chat_id != FORCE_CHAT_ID:
            continue

        # chat_id 저장(처음 1회)
        if state.get("chat_id") != chat_id:
            state["chat_id"] = chat_id
            changed = True

        cmd, tickers = parse_cmd(text)

        if cmd in ("/start",):
            tg_send(chat_id, "OK. /list, /add TICKER, /del TICKER 를 사용할 수 있어요.")
            continue

        if cmd == "/list":
            cur = state.get("tickers", [])
            tg_send(chat_id, "Tickers: " + (", ".join(cur) if cur else "(empty)"))
            continue

        if cmd == "/add":
            cur = set(state.get("tickers", []))
            for t in tickers:
                cur.add(t)
            new_list = sorted(cur)
            if new_list != state.get("tickers", []):
                state["tickers"] = new_list
                changed = True
            tg_send(chat_id, "Updated: " + ", ".join(state["tickers"]))
            continue

        if cmd == "/del":
            cur = set(state.get("tickers", []))
            for t in tickers:
                cur.discard(t)
            new_list = sorted(cur)
            if new_list != state.get("tickers", []):
                state["tickers"] = new_list
                changed = True
            tg_send(chat_id, "Updated: " + (", ".join(state["tickers"]) if state["tickers"] else "(empty)"))
            continue

    return changed


# ---------------------------
# Yahoo Finance prices (yfinance + fallback quote endpoint)
# ---------------------------
def yahoo_quote(symbols: list[str]) -> dict[str, dict]:
    """
    폴백용: Yahoo quote endpoint.
    여러 종목 한 번에 조회.
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    r = requests.get(url, params={"symbols": ",".join(symbols)}, timeout=20)
    r.raise_for_status()
    rows = r.json().get("quoteResponse", {}).get("result", [])
    out = {}
    for row in rows:
        sym = row.get("symbol")
        if sym:
            out[sym.upper()] = row
    return out


def get_regular_close_yfinance(symbol: str):
    """
    정규장 종가: 최근 거래일 일봉 Close
    """
    t = yf.Ticker(symbol)
    df = t.history(period="10d", interval="1d", auto_adjust=False)
    if df is None or df.empty:
        raise RuntimeError("empty daily")
    last_idx = df.index[-1]
    close = float(df["Close"].iloc[-1])
    day = last_idx.date().isoformat()
    return day, close


def get_extended_last_yfinance(symbol: str):
    """
    extended last: 1분봉 + prepost=True 마지막 bar close
    (yfinance history의 prepost 파라미터로 extended 세션 포함) :contentReference[oaicite:3]{index=3}
    """
    t = yf.Ticker(symbol)
    df = t.history(period="1d", interval="1m", prepost=True, auto_adjust=False)
    if df is None or df.empty:
        raise RuntimeError("empty intraday")
    ts = df.index[-1]
    # tz 방어
    if getattr(ts, "tzinfo", None) is not None:
        ts_et = ts.astimezone(ET)
    else:
        ts_et = ts.replace(tzinfo=ET)
    px = float(df["Close"].iloc[-1])
    return ts_et, px


def build_report(tickers: list[str]) -> str:
    """
    각 ticker에 대해:
      - regular close (yfinance 우선, 실패 시 quote의 regularMarketPreviousClose)
      - extended last (yfinance 우선, 실패 시 quote의 postMarketPrice 우선)
    """
    tickers = [normalize_ticker(t) for t in tickers if normalize_ticker(t)]
    if not tickers:
        return "No tickers."

    # 폴백 데이터(한 번에)
    quote_map = {}
    try:
        quote_map = yahoo_quote(tickers)
    except Exception:
        quote_map = {}

    lines = []
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    lines.append(f"[US Close + After-hours] {now_kst}")
    lines.append("")

    for sym in tickers:
        # 1) regular close
        trading_day = None
        close = None
        close_src = None
        try:
            trading_day, close = get_regular_close_yfinance(sym)
            close_src = "yfinance(daily)"
        except Exception:
            q = quote_map.get(sym, {})
            # 보통 가장 최근 종가에 해당 (상황에 따라 필드가 없을 수 있음)
            v = q.get("regularMarketPreviousClose")
            if v is not None:
                close = float(v)
                trading_day = "last_trading_day"
                close_src = "yahoo_quote(prev_close)"

        # 2) extended last
        ext_px = None
        ext_ts_et = None
        ext_src = None
        try:
            ext_ts_et, ext_px = get_extended_last_yfinance(sym)
            ext_src = "yfinance(1m_prepost)"
        except Exception:
            q = quote_map.get(sym, {})
            # after-hours 우선
            v = q.get("postMarketPrice")
            if v is None:
                v = q.get("regularMarketPrice")
            if v is not None:
                ext_px = float(v)
                ext_src = "yahoo_quote(post/regular)"
                # 시간도 있으면 표기
                tsec = q.get("postMarketTime") or q.get("regularMarketTime")
                if tsec:
                    ext_ts_et = datetime.fromtimestamp(int(tsec), tz=ET)

        if close is None and ext_px is None:
            lines.append(f"{sym}: (no data)")
            continue

        if close is not None and ext_px is not None:
            chg_pct = (ext_px / close - 1.0) * 100.0 if close != 0 else 0.0
            ts_txt = f" @ {ext_ts_et.strftime('%Y-%m-%d %H:%M ET')}" if ext_ts_et else ""
            day_txt = trading_day if trading_day else ""
            lines.append(
                f"{sym}: close {close:.2f} ({day_txt}) | ext {ext_px:.2f} ({chg_pct:+.2f}%)"
                f"{ts_txt}"
            )
        elif close is not None:
            lines.append(f"{sym}: close {close:.2f} ({trading_day})")
        else:
            ts_txt = f" @ {ext_ts_et.strftime('%Y-%m-%d %H:%M ET')}" if ext_ts_et else ""
            lines.append(f"{sym}: ext {ext_px:.2f}{ts_txt}")

        # 디버그성 출처(원하면 지워도 됨)
        # lines.append(f"   src: close={close_src}, ext={ext_src}")

    return "\n".join(lines)


# ---------------------------
# Trigger window & main
# ---------------------------
def in_send_window_kst(now: datetime) -> bool:
    """
    KST 06:30 ~ 06:45 (inclusive) 허용
    5분 주기면 06:30/35/40/45에서 걸림
    """
    if now.hour != 6:
        return False
    return 30 <= now.minute <= 45


def main():
    state = load_state()
    state_changed = False

    # 1) 텔레그램 업데이트 처리(/add,/del,/list)
    try:
        if handle_updates(state):
            state_changed = True
    except Exception as e:
        # 업데이트 처리 실패해도 리포트 로직은 계속 진행
        print(f"[WARN] Telegram update handling error: {e}")

    now_kst = datetime.now(KST)
    today_kst = now_kst.date().isoformat()

    # 2) 06:30~06:45 사이 && 오늘 미발송이면 발송
    if in_send_window_kst(now_kst) and state.get("last_sent_kst_date") != today_kst:
        chat_id = state.get("chat_id")
        if chat_id is None:
            print("[INFO] chat_id is null. Send /start to the bot once.")
        else:
            tickers = state.get("tickers", [])
            msg = build_report(tickers)
            try:
                tg_send(chat_id, msg)
                # 3) 발송 후 last_sent_kst_date 저장
                state["last_sent_kst_date"] = today_kst
                state_changed = True
                print("[INFO] Report sent.")
            except Exception as e:
                print(f"[ERROR] Send failed: {e}")

    if state_changed:
        save_state(state)
        print("[INFO] state.json updated.")
    else:
        print("[INFO] No state change.")


if __name__ == "__main__":
    main()
