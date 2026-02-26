# Boxer - 사내 CS RAG 챗봇

마미박스 CS 팀 질문에 S3 로그 + Notion + RDS를 RAG 방식으로 조회하고, Ollama LLM이 자동 답변하는 Slack 챗봇.

## 현재 구현 원칙 (Phase 1)

- Slack Bolt는 **Socket Mode**로 먼저 검증 (초기 HTTPS 인바운드 구성 불필요)
- Python은 **3.11+** 사용
- 패키지 관리는 전역 설치보다 **`venv` 권장**
- 로컬 토큰 관리는 **`.env`** 사용
- 운영 배포 시 토큰 관리는 **Secrets Manager**로 전환

## Phase 1 - Slack Bolt ping-pong

- [x] boxer-role IAM Role EC2에 연결
- [x] GitHub 레포 생성 (firstquarter-J/rag-bot)
- [x] EC2에 Python 3.11+ 설치
- [x] slack-bolt 패키지 설치 (로컬/EC2)
- [ ] Secrets Manager에 Slack 토큰 저장
- [x] app.py 작성 (스레드 pong-ec2 응답)
- [x] 특정 사용자 규칙 응답 (DD/Mark)
- [x] Slack에서 @Boxer ping -> pong-local 확인 (로컬)
- [x] Slack에서 @Boxer ping -> pong-ec2 확인 (EC2)

### Phase 1 빠른 실행 (.env)

1. Python 3.11+ 확인

```bash
python3.11 --version
```

2. 가상환경 및 패키지 설치

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. 토큰 설정

```bash
cp .env.example .env
```

- [.env](/Users/firstquarter/workspace/rag-bot/.env) 파일에 아래 값 입력
  - `SLACK_BOT_TOKEN=xoxb-...`
  - `SLACK_APP_TOKEN=xapp-...`
  - `SLACK_SIGNING_SECRET=...`

4. 서버 실행

```bash
python app.py
```

5. Slack 테스트

- 채널에서 `@Boxer ping` 입력
- 봇이 스레드에 `pong-ec2` 응답하면 성공
- DD(`U0A079J3L9M`)가 멘션하면 스레드에 `간식 통제` 응답
- Mark(`U02LBHACKEU`)가 멘션하면 스레드에 `득남 축하` 응답

### EC2 배포 현황 (2026-02-26 UTC)

- EC2 접속 확인: `ec2-user@43.203.174.230`
- 런타임 설치 완료: `git`, `python3.11`, `python3.11-pip`
- 배포 경로: `/home/ec2-user/rag-bot`
- `python3.11 -m venv .venv` 생성 및 `pip install -r requirements.txt` 완료
- `.env` 업로드 및 권한 `600` 적용
- `systemd` 서비스 `boxer.service` 등록/활성화
- `journalctl -u boxer` 기준 Socket Mode 연결 로그 확인 (`Bolt app is running`)

## Phase 2 - LLM 우선 연동 (Provider 분리)

RAG 데이터 소스를 붙이기 전에 LLM 응답 파이프라인을 먼저 완성한다.

- [ ] `LLM_PROVIDER` 기반 라우팅 구현 (`ollama` / `openai` / `claude`)
- [ ] 공통 인터페이스 구현 (`generate_answer(prompt, context)`)
- [ ] Slack 멘션 -> LLM 답변 스레드 응답 E2E
- [ ] 답변 가드레일 추가 (근거 부족 시 추정 금지/모르면 모른다고 답변)
- [ ] provider별 설정을 `.env`로 분리

## Phase 3 - 단일 소스 RAG 파일럿

먼저 한 소스만 붙여 retrieval 품질과 프롬프트 구조를 검증한다.

- [ ] 우선 소스 선택 (Notion 또는 S3)
- [ ] 문서 수집/정규화
- [ ] retrieval 결과를 LLM context에 주입
- [ ] 근거 포함 답변 형식 검증

## Phase 4 - 멀티 소스 확장 (S3 + Notion + RDS)

- [ ] S3 로그 fetch + 날짜/키워드 필터링
- [ ] Notion 페이지 검색 + 본문 추출
- [ ] RDS 조회 연결 (보안그룹/계정 포함)
- [ ] 소스별 우선순위/충돌 규칙 정의
- [ ] 통합 retrieval 결과를 단일 context로 구성

## Phase 5 - 모델 운영 전략 고도화

- [ ] 모델 실행 위치 결정
- [ ] Ollama 전용 EC2 운영안 확정 (권장)
- [ ] 대안 provider 연동 테스트 (온프레미스/Claude/OpenAI API)
- [ ] 비용/지연/품질 기준으로 provider 선택 정책 수립
- [ ] 성능 튜닝 (인스턴스 타입, 캐시, 동시성)

## Phase 6 - box-admin-client 채팅 UI 연동

Slack 외에 box-admin-client 웹 앱에 채팅 UI를 추가한다. 핵심 RAG 엔진을 공통 서비스로 분리하고 Slack과 웹 인터페이스가 각각 붙는 구조를 목표로 한다.

- [ ] RAG 엔진 코어 모듈화 (Slack Bolt와 분리)
- [ ] FastAPI REST/WebSocket 엔드포인트 구현
- [ ] box-admin-client 인증 방식 확인
- [ ] 네트워크 구성 확인 (같은 VPC 여부)
- [ ] box-admin-client 채팅 컴포넌트 구현
- [ ] LLM 스트리밍 응답 연동 (SSE or WebSocket)

## 아키텍처

```text
Slack            <-> Slack Bolt Layer \
                                      RAG Orchestrator (Retrieval + Prompt + Policy)
box-admin-client <-> FastAPI / WS API /
                                      LLM Gateway (Provider Adapter)
                                      |- Ollama on Dedicated EC2 (권장)
                                      |- On-prem Model Server
                                      |- Claude/OpenAI API
```

## 모델 배포 전략

권장: 모델은 별도 서버(전용 EC2 또는 온프레미스)로 분리하고, 봇 서버는 provider adapter만 둔다.

이유:

- 모델 추론 부하를 봇 처리와 분리해 장애 전파를 줄일 수 있음
- 모델 교체(Ollama <-> 외부 API <-> 온프레미스)가 코드 변경 없이 가능
- 비용과 성능을 provider별로 유연하게 운영 가능
- 보안/네트워크 정책을 모델 서버 단위로 분리 가능

초기 PoC는 같은 EC2에서 시작해도 되지만, 운영 전환 시 분리를 권장한다.
