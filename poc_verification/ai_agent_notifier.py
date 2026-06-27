


"""
ai_agent_notifier.py
=====================
LangGraph 기반 AI 에이전트 연동 모듈.
매매 체결 이벤트 발생 시:
  1. Gemini(기본) 또는 OpenAI가 매매 이유를 분석
  2. 텔레그램 메시지 전송
  3. 이메일 전송 (Gmail SMTP 사용)
"""

import os
import sys
import re
import time
import asyncio
import smtplib
import threading
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import TypedDict, Dict, Any

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from telegram import Bot
from telegram.request import HTTPXRequest


_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _configure_stdio() -> None:
    """Windows cp949 콘솔에서 한글·특수문자 print 오류 방지."""
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _log(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", None) or "utf-8"
        print(msg.encode(enc, errors="replace").decode(enc, errors="replace"))


def _load_env_file() -> None:
    """루트 .env 로드 (단독 실행·모듈 import 공통)."""
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip().strip('"').strip("'")


def _env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


def _telegram_configured() -> bool:
    tok = _env("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    cid = _env("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
    return bool(tok and cid and tok != "YOUR_TELEGRAM_BOT_TOKEN" and cid != "YOUR_TELEGRAM_CHAT_ID")


def _gemini_configured() -> bool:
    return bool(_env("GEMINI_API_KEY") or _env("GOOGLE_API_KEY"))


def _email_configured() -> bool:
    sender = _env("EMAIL_SENDER")
    receiver = _env("EMAIL_RECEIVER")
    password = _env("EMAIL_PASSWORD")
    if not password or "앱_비밀번호" in password or password.startswith("gmail_"):
        return False
    return bool(_EMAIL_RE.match(sender or "") and _EMAIL_RE.match(receiver or ""))


def _ai_provider() -> str:
    return _env("AI_PROVIDER", "gemini").lower() or "gemini"


_configure_stdio()
_load_env_file()

# ==============================================================================
# 1. 환경 설정 (.env 파일에 아래 항목들을 추가하세요)
# ==============================================================================


def _format_timestamp(ts) -> str:
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, OverflowError, ValueError):
            pass
    return str(ts or "-")


def _fallback_analysis(data: dict, error: str = "") -> str:
    signal = data.get("signal", "HOLD")
    ticker = data.get("ticker", "?")
    reason = data.get("risk_reason", "전략 시그널")
    suffix = "AI 분석 API 오류로 요약만 전송"
    if "RESOURCE_EXHAUSTED" in error or "429" in error:
        suffix = "Gemini 할당량 초과로 요약만 전송"
    elif "미설정" in error:
        suffix = "AI 키 미설정으로 요약만 전송"
    return f"{ticker} {signal} 체결 — {reason}. ({suffix})"


def _build_llm():
    provider = _ai_provider()
    if provider == "openai":
        from langchain_openai import ChatOpenAI
        api_key = _env("OPENAI_API_KEY")
        if not api_key or api_key.startswith("sk-..."):
            raise ValueError("OPENAI_API_KEY 미설정")
        return ChatOpenAI(
            model=_env("OPENAI_MODEL", "gpt-4o-mini"),
            api_key=api_key,
            base_url=_env("OPENAI_API_BASE", "https://api.openai.com/v1"),
            temperature=0.3,
        )
    from langchain_google_genai import ChatGoogleGenerativeAI
    api_key = _env("GEMINI_API_KEY") or _env("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY 또는 GOOGLE_API_KEY 미설정")
    return ChatGoogleGenerativeAI(
        model=_env("GEMINI_MODEL", "gemini-2.0-flash"),
        google_api_key=api_key,
        temperature=0.3,
    )

# ==============================================================================
# 2. LangGraph 상태 정의
# ==============================================================================
class AgentState(TypedDict):
    trade_data:          Dict[str, Any]  # 매매 이벤트 원본 데이터
    analysis_result:     str             # AI 분석 결과 텍스트
    telegram_status:     str             # 텔레그램 전송 결과
    email_status:        str             # 이메일 전송 결과

# ==============================================================================
# 3. 노드 1: AI 매매 분석
# ==============================================================================
def analyze_trade(state: AgentState) -> AgentState:
    """Gemini(기본) 또는 OpenAI로 매매 이유를 2~3줄 분석."""
    data = state["trade_data"]
    ts = _format_timestamp(data.get("timestamp"))

    system_prompt = (
        "당신은 전문 가상자산 퀀트 트레이더입니다. "
        "주어진 매매 데이터를 바탕으로 왜 이런 매매 결정이 내려졌는지 "
        "2~3줄로 명확하고 간결하게 한국어로 분석해주세요."
    )
    user_prompt = f"""
[매매 이벤트 발생]
- 종목: {data.get('ticker')}
- 방향: {data.get('signal')} (BUY=매수 / SELL=매도)
- 체결가: {float(data.get('price') or 0):,.0f} KRW
- 리스크 관리 이유: {data.get('risk_reason', '일반 지표 시그널')}
- 시간: {ts}

위 데이터를 바탕으로 매매 이유를 분석해 주세요.
"""

    try:
        llm = _build_llm()
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ])
        text = getattr(response, "content", str(response))
        return {"analysis_result": text}
    except Exception as e:
        err = str(e)
        _log(f"[AI Agent] {_ai_provider()} 분석 실패 — 폴백 사용: {err}")
        return {"analysis_result": _fallback_analysis(data, err)}

# ==============================================================================
# 4. 노드 2: 텔레그램 알림 전송
# ==============================================================================
async def _send_telegram_async(message: str) -> bool:
    """비동기 텔레그램 메시지 전송."""
    if not _telegram_configured():
        _log("[Telegram] 토큰 미설정 — 콘솔 출력만 수행합니다.")
        return False
    token = _env("TELEGRAM_BOT_TOKEN")
    chat_id = _env("TELEGRAM_CHAT_ID")
    request = HTTPXRequest(connect_timeout=20.0, read_timeout=20.0, write_timeout=20.0)
    try:
        bot = Bot(token=token, request=request)
        await bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
        return True
    except Exception as e:
        _log(f"[Telegram Error] {e}")
        try:
            bot = Bot(token=token, request=request)
            await bot.send_message(chat_id=chat_id, text=message)
            return True
        except Exception as e2:
            _log(f"[Telegram Error] plain 재시도 실패: {e2}")
            return False

def notify_telegram(state: AgentState) -> AgentState:
    """분석 결과를 텔레그램으로 전송합니다. (로컬 라벨 표시)"""
    data     = state["trade_data"]
    analysis = state["analysis_result"]

    signal_emoji = "🟢" if data.get("signal") == "BUY" else "🔴"
    ts = _format_timestamp(data.get("timestamp"))
    message = (
        f"{signal_emoji} *자동매매 체결 알림 (로컬)* {signal_emoji}\n\n"
        f"📌 *종목*: {data.get('ticker')}\n"
        f"⚖️ *구분*: {data.get('signal')}\n"
        f"💰 *단가*: {float(data.get('price') or 0):,.0f} 원\n"
        f"🕐 *시간*: {ts}\n\n"
        f"🧠 *AI 에이전트 분석 (Local Engine)*:\n{analysis}"
    )

    _log(f"\n[Telegram 메시지 미리보기]\n{message}\n")

    try:
        success = asyncio.run(_send_telegram_async(message))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        success = loop.run_until_complete(_send_telegram_async(message))
        loop.close()

    status = "Success" if success else "Failed (토큰/네트워크 확인 필요)"
    return {"telegram_status": status}

# ==============================================================================
# 5. 노드 3: 이메일 알림 전송
# ==============================================================================
def notify_email(state: AgentState) -> AgentState:
    """분석 결과를 이메일로 전송합니다. Gmail SMTP(포트 587) 사용. (로컬 라벨 표시)"""
    data     = state["trade_data"]
    analysis = state["analysis_result"]

    if not _email_configured():
        _log("[Email] 이메일 계정 미설정 — 이메일 전송을 생략합니다.")
        return {"email_status": "Skipped (계정 미설정)"}

    email_sender = _env("EMAIL_SENDER")
    email_password = _env("EMAIL_PASSWORD")
    email_receiver = _env("EMAIL_RECEIVER")

    signal_label = "매수 (BUY)" if data.get("signal") == "BUY" else "매도 (SELL)"
    from email.header import Header
    subject = Header(
        f"[자동매매 알림 (로컬)] {data.get('ticker')} {signal_label} 체결 — {data.get('timestamp')}",
        "utf-8",
    ).encode()

    # HTML 이메일 본문 구성
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: auto;">
        <h2 style="color: {'#27ae60' if data.get('signal') == 'BUY' else '#e74c3c'};">
            {'🟢 로컬 엔진 매수 체결' if data.get('signal') == 'BUY' else '🔴 로컬 엔진 매도 체결'} — {data.get('ticker')}
        </h2>
        <table style="width:100%; border-collapse:collapse;">
            <tr style="background:#f2f2f2;">
                <td style="padding:8px; border:1px solid #ddd;"><b>종목</b></td>
                <td style="padding:8px; border:1px solid #ddd;">{data.get('ticker')}</td>
            </tr>
            <tr>
                <td style="padding:8px; border:1px solid #ddd;"><b>구분</b></td>
                <td style="padding:8px; border:1px solid #ddd;">{signal_label}</td>
            </tr>
            <tr style="background:#f2f2f2;">
                <td style="padding:8px; border:1px solid #ddd;"><b>체결 단가</b></td>
                <td style="padding:8px; border:1px solid #ddd;">{data.get('price'):,.0f} 원</td>
            </tr>
            <tr>
                <td style="padding:8px; border:1px solid #ddd;"><b>리스크 이유</b></td>
                <td style="padding:8px; border:1px solid #ddd;">{data.get('risk_reason', '일반 지표 시그널')}</td>
            </tr>
            <tr style="background:#f2f2f2;">
                <td style="padding:8px; border:1px solid #ddd;"><b>시간</b></td>
                <td style="padding:8px; border:1px solid #ddd;">{data.get('timestamp')}</td>
            </tr>
        </table>
        <br>
        <div style="background:#eaf4fb; padding:16px; border-left:4px solid #3498db; border-radius:4px;">
            <b>🧠 AI 에이전트 분석</b><br><br>
            {analysis.replace(chr(10), '<br>')}
        </div>
        <br>
        <p style="font-size:12px; color:#999;">
            본 메일은 로컬 파이썬 자동매매 시스템(Local Engine)에 의해 자동 발송되었습니다.
        </p>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = email_sender
    msg["To"]      = email_receiver
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.login(email_sender, email_password)
            server.sendmail(email_sender, email_receiver, msg.as_bytes())
        _log(f"[Email] 이메일 전송 성공 → {email_receiver}")
        return {"email_status": "Success"}
    except smtplib.SMTPAuthenticationError:
        _log("[Email Error] 인증 실패 — Gmail 앱 비밀번호를 확인하세요.")
        return {"email_status": "Failed (인증 오류)"}
    except Exception as e:
        _log(f"[Email Error] {e}")
        return {"email_status": f"Failed ({e})"}

# ==============================================================================
# 6. LangGraph 그래프 구성
# ==============================================================================
def build_trade_agent():
    """AI 분석 → 텔레그램 → 이메일 순서의 에이전트 그래프를 생성합니다."""
    workflow = StateGraph(AgentState)

    workflow.add_node("analyze",         analyze_trade)
    workflow.add_node("notify_telegram", notify_telegram)
    workflow.add_node("notify_email",    notify_email)

    workflow.set_entry_point("analyze")
    workflow.add_edge("analyze",         "notify_telegram")
    workflow.add_edge("notify_telegram", "notify_email")
    workflow.add_edge("notify_email",    END)

    return workflow.compile()

# ==============================================================================
# 7. 외부 호출 인터페이스
# ==============================================================================
trade_agent_app = build_trade_agent()

def process_trade_event(trade_data: dict):
    """
    risk_management_poc.py의 _execute_order에서 호출되는 엔트리 포인트.
    별도 스레드에서 실행하여 매매 엔진을 블로킹하지 않습니다.
    """
    def run_agent():
        _load_env_file()
        initial_state = {
            "trade_data":      trade_data,
            "analysis_result": "",
            "telegram_status": "",
            "email_status":    ""
        }
        _log(f"[AI Agent] {trade_data.get('ticker')} {trade_data.get('signal')} 이벤트 분석 시작 ({_ai_provider()})...")
        _log(
            f"[AI Agent] 설정 — telegram={_telegram_configured()} gemini={_gemini_configured()}"
        )
        result = trade_agent_app.invoke(initial_state)
        _log(f"[AI Agent] 텔레그램: {result['telegram_status']} | 이메일: {result['email_status']}")

    threading.Thread(target=run_agent, daemon=True).start()


# ==============================================================================
# 8. 단독 테스트 실행
# ==============================================================================
if __name__ == "__main__":
    test_event = {
        "ticker":      "BTC",
        "signal":      "BUY",
        "price":       85_000_000,
        "risk_reason": "RSI 과매도 구간 진입 및 MACD 골든크로스 발생",
        "timestamp":   time.strftime("%Y-%m-%d %H:%M:%S")
    }
    done = threading.Event()

    def run_test():
        _load_env_file()
        initial_state = {
            "trade_data":      test_event,
            "analysis_result": "",
            "telegram_status": "",
            "email_status":    ""
        }
        _log(f"[AI Agent] {test_event.get('ticker')} {test_event.get('signal')} 이벤트 분석 시작 ({_ai_provider()})...")
        _log(
            f"[AI Agent] 설정 — telegram={_telegram_configured()} gemini={_gemini_configured()}"
        )
        result = trade_agent_app.invoke(initial_state)
        _log(f"[AI Agent] 텔레그램: {result['telegram_status']} | 이메일: {result['email_status']}")
        done.set()

    threading.Thread(target=run_test, daemon=True).start()
    done.wait(timeout=90)
