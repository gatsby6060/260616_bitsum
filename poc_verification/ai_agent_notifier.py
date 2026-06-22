


"""
ai_agent_notifier.py
=====================
LangGraph 기반 AI 에이전트 연동 모듈.
매매 체결 이벤트 발생 시:
  1. GPT가 매매 이유를 분석
  2. 텔레그램 메시지 전송
  3. 이메일 전송 (Gmail SMTP 사용)
"""

import os
import time
import asyncio
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import TypedDict, Dict, Any

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from telegram import Bot

# ==============================================================================
# 1. 환경 설정 (.env 파일에 아래 항목들을 추가하세요)
# ==============================================================================

# --- 텔레그램 설정 ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "YOUR_TELEGRAM_CHAT_ID")

# --- 이메일 설정 (Gmail 기준) ---
EMAIL_SENDER    = os.getenv("EMAIL_SENDER",   "your_gmail@gmail.com")   # 보내는 계정
EMAIL_PASSWORD  = os.getenv("EMAIL_PASSWORD", "YOUR_APP_PASSWORD")       # Gmail 앱 비밀번호
EMAIL_RECEIVER  = os.getenv("EMAIL_RECEIVER", "your_email@example.com")  # 받는 이메일

# --- OpenAI 설정 ---
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

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
    """GPT를 사용하여 매매 이유를 2~3줄로 분석합니다."""
    data = state["trade_data"]

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_API_BASE,
        temperature=0.3
    )

    system_prompt = (
        "당신은 전문 가상자산 퀀트 트레이더입니다. "
        "주어진 매매 데이터를 바탕으로 왜 이런 매매 결정이 내려졌는지 "
        "2~3줄로 명확하고 간결하게 한국어로 분석해주세요."
    )
    user_prompt = f"""
[매매 이벤트 발생]
- 종목: {data.get('ticker')}
- 방향: {data.get('signal')} (BUY=매수 / SELL=매도)
- 체결가: {data.get('price'):,.0f} KRW
- 리스크 관리 이유: {data.get('risk_reason', '일반 지표 시그널')}
- 시간: {data.get('timestamp')}

위 데이터를 바탕으로 매매 이유를 분석해 주세요.
"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt)
    ]

    response = llm.invoke(messages)
    return {"analysis_result": response.content}

# ==============================================================================
# 4. 노드 2: 텔레그램 알림 전송
# ==============================================================================
async def _send_telegram_async(message: str) -> bool:
    """비동기 텔레그램 메시지 전송."""
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print("[Telegram] 토큰 미설정 — 콘솔 출력만 수행합니다.")
        return False
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")
        return True
    except Exception as e:
        print(f"[Telegram Error] {e}")
        return False

def notify_telegram(state: AgentState) -> AgentState:
    """분석 결과를 텔레그램으로 전송합니다."""
    data     = state["trade_data"]
    analysis = state["analysis_result"]

    signal_emoji = "🟢" if data.get("signal") == "BUY" else "🔴"
    message = (
        f"{signal_emoji} *자동매매 체결 알림* {signal_emoji}\n\n"
        f"📌 *종목*: {data.get('ticker')}\n"
        f"⚖️ *구분*: {data.get('signal')}\n"
        f"💰 *단가*: {data.get('price'):,.0f} 원\n"
        f"🕐 *시간*: {data.get('timestamp')}\n\n"
        f"🧠 *AI 에이전트 분석*:\n{analysis}"
    )

    print(f"\n[Telegram 메시지 미리보기]\n{message}\n")

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
    """분석 결과를 이메일로 전송합니다. Gmail SMTP(포트 587) 사용."""
    data     = state["trade_data"]
    analysis = state["analysis_result"]

    if EMAIL_SENDER == "your_gmail@gmail.com":
        print("[Email] 이메일 계정 미설정 — 이메일 전송을 생략합니다.")
        return {"email_status": "Skipped (계정 미설정)"}

    signal_label = "매수 (BUY)" if data.get("signal") == "BUY" else "매도 (SELL)"
    subject = f"[자동매매 알림] {data.get('ticker')} {signal_label} 체결 — {data.get('timestamp')}"

    # HTML 이메일 본문 구성
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: auto;">
        <h2 style="color: {'#27ae60' if data.get('signal') == 'BUY' else '#e74c3c'};">
            {'🟢 매수 체결' if data.get('signal') == 'BUY' else '🔴 매도 체결'} — {data.get('ticker')}
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
            본 메일은 자동매매 시스템에 의해 자동 발송되었습니다.
        </p>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECEIVER
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"[Email] 이메일 전송 성공 → {EMAIL_RECEIVER}")
        return {"email_status": "Success"}
    except smtplib.SMTPAuthenticationError:
        print("[Email Error] 인증 실패 — Gmail 앱 비밀번호를 확인하세요.")
        return {"email_status": "Failed (인증 오류)"}
    except Exception as e:
        print(f"[Email Error] {e}")
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
        initial_state = {
            "trade_data":      trade_data,
            "analysis_result": "",
            "telegram_status": "",
            "email_status":    ""
        }
        print(f"[AI Agent] {trade_data.get('ticker')} {trade_data.get('signal')} 이벤트 분석 시작...")
        result = trade_agent_app.invoke(initial_state)
        print(f"[AI Agent] 텔레그램: {result['telegram_status']} | 이메일: {result['email_status']}")

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
    process_trade_event(test_event)
    time.sleep(10)  # 백그라운드 스레드 완료 대기
