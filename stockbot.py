import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import yfinance as yf




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
      /del NVDA
      /name MU 마이크론
      /unname MU
      /names
    반환:
      (cmd, args_str, tickers_list)
    """
    text = (text or "").strip()
    if not text.startswith("/"):
        return None, "", []

    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    args = parts[1].strip() if len(parts) > 1 else ""

    # add/del용 ticker list (콤마/공백 혼합 지원)
    tickers = []
    if args:
        raw = re.split(r"[,\s]+", args)
        tickers = [normalize_ticker(x) for x in raw if normalize_ticker(x)]

    return cmd, args, tickers



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

        cmd, args, tickers = parse_cmd(text)

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

        if cmd == "/names":
            names = state.get("names", {})
            if not names:
                tg_send(chat_id, "Names: (empty)")
            else:
                # 보기 좋게 정렬 출력
                items = [f"{k}={v}" for k, v in sorted(names.items())]
                tg_send(chat_id, "Names: " + ", ".join(items))
            continue

        if cmd == "/name":
            # 형식: /name MU 마이크론
            # args에서 첫 토큰이 ticker, 나머지 전체가 name
            if not args:
                tg_send(chat_id, "Usage: /name TICKER 한국명 (예: /name MU 마이크론)")
                continue

            parts2 = args.split(maxsplit=1)
            if len(parts2) < 2:
                tg_send(chat_id, "Usage: /name TICKER 한국명 (예: /name MU 마이크론)")
                continue

            t = normalize_ticker(parts2[0])
            name = parts2[1].strip()

            if not t or not name:
                tg_send(chat_id, "Usage: /name TICKER 한국명 (예: /name MU 마이크론)")
                continue

            names = state.get("names")
            if not isinstance(names, dict):
                names = {}
                state["names"] = names
                changed = True

            # 저장
            prev = names.get(t)
            if prev != name:
                names[t] = name
                changed = True

            tg_send(chat_id, f"OK: {t} -> {name}")
            continue

        if cmd == "/unname":
            # 형식: /unname MU (여러개도 허용: /unname MU NVDA)
            if not tickers:
                tg_send(chat_id, "Usage: /unname TICKER (예: /unname MU)")
                continue

            names = state.get("names")
            if not isinstance(names, dict) or not names:
                tg_send(chat_id, "Names: (empty)")
                continue

            removed = []
            for t in tickers:
                if t in names:
                    del names[t]
                    removed.append(t)
                    changed = True

            if removed:
                tg_send(chat_id, "Removed: " + ", ".join(removed))
            else:
                tg_send(chat_id, "No matches.")
            continue
            
         if cmd == "/test":
            # 즉시 리포트 1회 발송 트리거
            state["force_report"] = True
            changed = True
            tg_send(chat_id, "OK. 다음 실행에서 리포트를 즉시 생성해 보낼게요.")
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


def get_close_and_prev_close_yfinance(symbol: str):
    """
    당일 종가 + 전일 종가 (변화율 계산용)
    """
    t = yf.Ticker(symbol)
    df = t.history(period="15d", interval="1d", auto_adjust=False)
    if df is None or df.empty or len(df) < 2:
        raise RuntimeError("not enough daily data")

    close = float(df["Close"].iloc[-1])
    prev_close = float(df["Close"].iloc[-2])
    day = df.index[-1].date().isoformat()
    return day, close, prev_close



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


def build_report(state: dict) -> str:
    tickers = [normalize_ticker(t) for t in state.get("tickers", []) if normalize_ticker(t)]
    if not tickers:
        return "No tickers."

    names_map = state.get("names", {})
    if not isinstance(names_map, dict):
        names_map = {}

    # 폴백 데이터(한 번에)
    quote_map = {}
    try:
        quote_map = yahoo_quote(tickers)
    except Exception:
        quote_map = {}

    now_kst = datetime.now(KST)
    wk_kr = ["월", "화", "수", "목", "금", "토", "일"][now_kst.weekday()]
    header = f"[{now_kst.month}월{now_kst.day}일 {wk_kr}요일 미국 주식 마감]"
    lines = [header]

    for sym in tickers:
        name = names_map.get(sym, sym)
        # 이하 로직은 기존 “마감/애프터 변화율” 계산 그대로 두되,
        # 출력에서 name만 사용하면 됩니다.
        close = prev_close = None
        try:
            _, close, prev_close = get_close_and_prev_close_yfinance(sym)
        except Exception:
            q = quote_map.get(sym, {})
            # 폴백: regularMarketPreviousClose(전일종가), regularMarketPrice(현재/마감 근접)
            pc = q.get("regularMarketPreviousClose")
            rp = q.get("regularMarketPrice")
            if pc is not None and rp is not None:
                prev_close = float(pc)
                close = float(rp)

        # 2) 애프터마켓 가격 가져오기 (yfinance 우선, 실패 시 quote)
        ext_px = None
        try:
            _, ext_px = get_extended_last_yfinance(sym)  # (ts_et, px)
        except Exception:
            q = quote_map.get(sym, {})
            v = q.get("postMarketPrice")
            if v is None:
                # after-hours가 없을 때는 프리마켓/정규로 대체하지 않는 게 깔끔함
                ext_px = None
            else:
                ext_px = float(v)

        # 3) 출력 구성
        if close is None or prev_close is None or prev_close == 0:
            # 최소한 after-hours만이라도 있으면 표시
            if ext_px is not None and close is not None and close != 0:
                after_pct = (ext_px / close - 1.0) * 100.0
                lines.append(f"{name} 마감 (N/A), 애프터 {after_pct:+.1f}%")
            else:
                lines.append(f"{name} (데이터 부족)")
            continue

        close_pct = (close / prev_close - 1.0) * 100.0

        if ext_px is not None and close != 0:
            after_pct = (ext_px / close - 1.0) * 100.0
            lines.append(f"{name} {close_pct:+.1f}% 마감, 애프터 {after_pct:+.1f}%")
        else:
            lines.append(f"{name} {close_pct:+.1f}% 마감")

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
    force_send = os.environ.get("FORCE_SEND", "0") == "1"
    if (force_send or in_send_window_kst(now_kst)) and state.get("last_sent_kst_date") != today_kst:    
        chat_id = state.get("chat_id")
        if chat_id is None:
            print("[INFO] chat_id is null. Send /start to the bot once.")
        else:
            msg = build_report(state)            
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
