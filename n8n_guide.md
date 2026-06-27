# 260616_bitsum 프로젝트를 위한 n8n 입문 및 활용 가이드

본 가이드는 사용자의 **260616_bitsum** 프로젝트(BTC & ETH 다중 타임프레임 및 시장 국면 감지 자동매매 시스템) 분석을 바탕으로, n8n을 어떻게 입문하고 프로젝트에 실질적으로 활용할 수 있는지 안내하기 위해 작성되었습니다.

---

## 1. n8n이란 무엇인가?

n8n은 **노드 기반의 워크플로우 자동화 도구**로, 다양한 애플리케이션과 API를 연결하여 반복적인 작업을 자동화할 수 있게 해줍니다. 코딩 지식이 없어도 시각적인 인터페이스를 통해 복잡한 로직을 구현할 수 있으며, 개발자라면 HTTP 요청, Webhook, 커스텀 코드(JavaScript/Python) 노드를 활용하여 무한한 확장이 가능합니다.

특히 **암호화폐 트레이딩, 봇 알림, 데이터 수집** 등의 백엔드 파이프라인 구축에 널리 사용됩니다. [1]

---

## 2. n8n 입문 및 학습 가이드

n8n을 처음 접하시는 분들을 위한 단계별 학습 방법입니다.

### 2.1. 시작하기 (설치 및 클라우드)
n8n은 두 가지 주요 방식으로 사용할 수 있습니다. [2]
*   **n8n Cloud**: 가장 빠르고 쉬운 방법입니다. 인프라 관리가 필요 없으며 가입 즉시 브라우저에서 워크플로우를 만들 수 있습니다. (초보자 권장)
*   **Self-hosted (Docker/npm)**: 본인의 서버에 직접 설치하여 무료로 대부분의 기능을 제한 없이 사용할 수 있습니다. 개발 지식이 있다면 Docker를 이용한 로컬 설치를 추천합니다.

### 2.2. 핵심 개념 이해
n8n 워크플로우는 **노드(Node)**들의 연결로 이루어집니다.
*   **Trigger Node**: 워크플로우를 시작하는 노드입니다. (예: 특정 시간에 실행하는 *Schedule Trigger*, 외부에서 데이터를 받을 때 실행하는 *Webhook* 등)
*   **Action Node**: 실제 작업을 수행하는 노드입니다. (예: HTTP 요청을 보내는 *HTTP Request*, 텔레그램 메시지를 보내는 *Telegram*, 조건문을 처리하는 *If* 등)
*   **Connection**: 노드와 노드를 연결하여 데이터를 전달합니다. 이전 노드의 출력 데이터(JSON 형태)가 다음 노드의 입력 데이터가 됩니다.

### 2.3. 추천 학습 리소스
*   **n8n Academy** (공식 무료 강의): [learn.n8n.io](https://learn.n8n.io/) 에서 제공하는 **N8N101 (초급)** 코스를 수강하시면 기본 개념을 완벽하게 잡을 수 있습니다. [3]
*   **공식 유튜브 튜토리얼**: n8n 공식 채널의 "Build Your First Workflow" 영상을 따라 해보며 감을 익히는 것을 추천합니다. [4]
*   **템플릿 활용**: n8n은 수천 개의 공식 템플릿을 제공합니다. 원하는 기능의 템플릿을 복사하여 본인의 환경에 맞게 수정해보는 것이 가장 빠른 학습 방법입니다.

---

## 3. 260616_bitsum 프로젝트 분석

현재 `260616_bitsum` 프로젝트는 다음과 같은 특징을 가진 훌륭한 자동매매 시스템입니다.
*   **주요 기능**: BTC/ETH 다중 타임프레임 데이터 분석, 시장 국면(Bull/Bear/Range) 감지, 리스크 관리(손절/익절), FastAPI 기반 웹 서버 및 웹소켓 실시간 데이터 전송.
*   **알림 시스템**: `ai_agent_notifier.py`를 통해 거래 발생 시 LangGraph 기반 AI(Gemini/OpenAI)가 매매 이유를 분석하고 Telegram 및 Email로 알림을 전송하고 있습니다.
*   **API 연동**: 빗썸 REST API 및 WebSocket API를 적극적으로 활용하고 있습니다.

---

## 4. 260616_bitsum 프로젝트에 n8n 활용하기

현재 Python 코드로 하드코딩된 일부 백엔드 작업과 알림 시스템을 n8n으로 분리하면, 시스템의 유지보수성을 높이고 더 유연한 알림 파이프라인을 구축할 수 있습니다.

### 💡 활용 아이디어 1: AI 매매 분석 및 텔레그램/이메일 알림 자동화 (현재 코드 대체)
현재 `ai_agent_notifier.py`에서 수행하는 텔레그램 및 이메일 발송 로직을 n8n으로 이관할 수 있습니다.
*   **구현 방식**:
    1. Python 백엔드(FastAPI 또는 매매 엔진)에서는 매매 체결 시 n8n의 Webhook URL로 매매 데이터(종목, 가격, 수량 등)만 JSON 형태로 POST 전송합니다.
    2. n8n의 **Webhook Node**가 이를 수신합니다.
    3. n8n 내의 **OpenAI / Gemini Node**를 연결하여 매매 데이터를 바탕으로 분석 코멘트를 생성합니다.
    4. n8n의 **Telegram Node**와 **Email Node** (또는 Gmail Node)를 통해 생성된 메시지를 발송합니다.
*   **장점**: 알림 메시지 포맷을 변경하거나 새로운 알림 채널(예: Slack, Discord)을 추가할 때 Python 코드를 수정하고 서버를 재시작할 필요 없이 n8n 웹 UI에서 노드만 추가하면 됩니다. [5]

### 💡 활용 아이디어 2: 매매 내역 구글 스프레드시트 자동 기록
자동매매 시스템의 성과를 분석하기 위해 체결 내역을 엑셀로 관리하는 것은 매우 유용합니다.
*   **구현 방식**: 매매 발생 시 호출되는 n8n Webhook 워크플로우에 **Google Sheets Node**를 추가합니다. 체결 시간, 종목, 포지션(Buy/Sell), 가격, 수량 등을 지정된 스프레드시트에 자동으로 행(Row) 추가하도록 설정합니다. [6]

### 💡 활용 아이디어 3: 주간/일간 트레이딩 리포트 자동 생성
*   **구현 방식**: n8n의 **Schedule Trigger Node**를 사용하여 매주 월요일 아침 9시에 워크플로우가 실행되도록 합니다.
*   **흐름**:
    1. Schedule Trigger 작동
    2. Google Sheets Node에서 지난 1주일간의 매매 기록 읽기
    3. Code Node(JavaScript) 또는 AI Node를 통해 승률, 총 수익금, 최대 손실폭 등 요약 계산
    4. Telegram Node로 주간 요약 리포트 전송 [6]

### 💡 활용 아이디어 4: 외부 트레이딩 시그널 수신 및 FastAPI 연동
만약 TradingView의 웹훅 알림이나 다른 텔레그램 방의 시그널을 받아서 현재 프로젝트의 전략에 참고하고 싶다면 n8n이 훌륭한 브릿지 역할을 합니다.
*   **구현 방식**: TradingView 시그널 $\rightarrow$ n8n Webhook 수신 $\rightarrow$ 데이터 파싱 및 필터링 $\rightarrow$ 프로젝트의 FastAPI 엔드포인트(POST `/api/tickers/control` 등)로 **HTTP Request Node**를 통해 제어 명령 전송.

---

## 5. 결론 및 다음 단계

`260616_bitsum` 프로젝트는 이미 FastAPI와 다양한 모듈로 잘 구조화되어 있습니다. n8n을 도입한다면 핵심 트레이딩 엔진(지표 계산, 주문 실행)은 현재처럼 Python에 유지하고, **"알림 발송, 매매 일지 기록, 외부 시그널 수집, 정기 리포트 생성"**과 같은 부가적인 파이프라인을 n8n으로 분리하는 것을 강력히 추천합니다.

### 지금 바로 해볼 수 있는 액션:
1. n8n Cloud 무료 평가판에 가입하거나 로컬에 n8n을 설치합니다.
2. Webhook 노드와 Telegram 노드를 연결하는 아주 간단한 워크플로우를 만들어 봅니다.
3. 프로젝트의 FastAPI 코드에서 `requests.post()`를 이용해 해당 Webhook URL로 테스트 메시지를 보내보고 텔레그램으로 알림이 오는지 확인해 봅니다.

---

### References
* [1] n8n Docs - Welcome to n8n: https://docs.n8n.io/
* [2] n8n Docs - Choose how to use n8n: https://docs.n8n.io/choose-how-to-use-n8n.md
* [3] n8n Academy: https://learn.n8n.io/
* [4] n8n Docs - Build your first workflow: https://docs.n8n.io/build-your-first-workflow.md
* [5] n8n Workflows - Automated cryptocurrency trading bot: https://n8n.io/workflows/8453-automated-cryptocurrency-trading-bot-with-ict-methodology-gpt-4o-and-coinbase/
* [6] n8n Community - Automate Crypto Exchange Trade Alerts: https://community.n8n.io/t/automate-crypto-exchange-trade-alerts-weekly-reports-using-webhook-telegram-google-sheets/278575
