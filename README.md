# Boxer - 사내 CS RAG 챗봇

마미박스 CS 팀 질문에 S3 로그 + Notion + RDS를 RAG 방식으로 조회하고, Ollama LLM이 자동 답변하는 Slack 챗봇.

## 현재 구현 원칙 (Phase 1)

- Slack Bolt는 **Socket Mode**로 먼저 검증 (초기 HTTPS 인바운드 구성 불필요)
- Python은 **3.11+** 사용
- 패키지 관리는 전역 설치보다 **`venv` 권장**
- 로컬 토큰 관리는 **`.env`** 사용
- 기본 LLM provider는 **`ollama`** (`boxer-llm`, `OLLAMA_BASE_URL` 사용)
- 공통 시스템 정책 프롬프트는 provider와 무관하게 동일 적용 (영어 규칙문 + 언어/톤 정책)
- 사용자 제한은 `claude` 호출에만 적용 (`ollama`는 제한 없음)
- DB 조회는 `.env`의 `BOX_DB_*` 값으로 연결하고 읽기 전용 쿼리만 허용
- 자연어 자동 해석은 튜닝이 아니라 `의도/슬롯 추출 + 템플릿 조회` 방식으로 구현
- 운영 배포 시 토큰 관리는 **Secrets Manager**로 전환

## Phase 1 - Slack Bolt ping-pong

- [x] boxer-role IAM Role EC2에 연결
- [x] GitHub 레포 생성 (firstquarter-J/rag-bot)
- [x] EC2에 Python 3.11+ 설치
- [x] slack-bolt 패키지 설치 (로컬/EC2)
- [ ] Secrets Manager에 Slack 토큰 저장
- [x] app.py 작성 (스레드 pong-ec2 응답)
- [x] 멘션 사용자 태그 응답
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
  - `SLACK_BOT_TOKEN=<YOUR_VALUE>`
  - `SLACK_APP_TOKEN=<YOUR_VALUE>`
  - `SLACK_SIGNING_SECRET=<YOUR_VALUE>`
  - `LLM_PROVIDER=<ollama|claude>`
  - `OLLAMA_BASE_URL=<OLLAMA_BASE_URL>`
  - `OLLAMA_MODEL=<OLLAMA_MODEL>`
  - `OLLAMA_TIMEOUT_SEC=<SECONDS>`
  - `OLLAMA_TEMPERATURE=<0.0~1.0>`
  - `THREAD_CONTEXT_FETCH_LIMIT=<1~200>`
  - `THREAD_CONTEXT_MAX_MESSAGES=<N>`
  - `THREAD_CONTEXT_MAX_CHARS=<N>`
  - Claude 사용 시에만 `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`, `ANTHROPIC_MAX_TOKENS` 설정
  - 바코드 조회 API: `APP_USER_API_URL`, `APP_USER_API_TIMEOUT_SEC`
  - DB 조회 사용 시 `DB_QUERY_ENABLED=true`, `BOX_DB_HOST`, `BOX_DB_PORT`, `BOX_DB_USERNAME`, `BOX_DB_PASSWORD`, `BOX_DB_DATABASE` 설정
  - DB 조회 제어값: `DB_QUERY_TIMEOUT_SEC`, `DB_QUERY_MAX_ROWS`, `DB_QUERY_MAX_SQL_CHARS`, `DB_QUERY_MAX_RESULT_CHARS`

4. 서버 실행

```bash
python app.py
```

5. Slack 테스트

- 채널에서 `@Boxer ping` 입력
- 봇이 스레드에 `pong-ec2` 응답하면 성공
- 멘션한 사용자 태그(`<@user_id>`)를 포함해 스레드 응답하면 성공
- 11자리 바코드 입력 시 app-user API 조회 결과(산모/아이 정보) 응답하면 성공
- DB 조회 테스트: `@Boxer db 조회` (기본 쿼리) 또는 `@Boxer db 조회 SELECT NOW() AS now_time`

### EC2 배포 현황 (2026-02-26 UTC)

- EC2 접속 확인: `ec2-user@43.203.174.230`
- 런타임 설치 완료: `git`, `python3.11`, `python3.11-pip`
- 배포 경로: `/home/ec2-user/rag-bot`
- `python3.11 -m venv .venv` 생성 및 `pip install -r requirements.txt` 완료
- `.env` 업로드 및 권한 `600` 적용
- `systemd` 서비스 `boxer.service` 등록/활성화
- `journalctl -u boxer` 기준 Socket Mode 연결 로그 확인 (`Bolt app is running`)

### LLM EC2 연동 현황 (2026-02-27 KST)

- LLM 전용 인스턴스: `boxer-llm` (`m7i.large`, Amazon Linux 2023)
- 네트워크: 같은 VPC 내부에서 Private IP로 통신
- Ollama: `0.17.1`, 모델 `qwen2.5:1.5b` pull 및 로컬 실행 확인
- Ollama 바인딩: `OLLAMA_HOST=0.0.0.0:11434` 적용
- 보안그룹: `Boxer-LLM (sg-0ec551157bcc20e83)` 인바운드 `11434/TCP` 허용
- 내부 통신 검증: Bolt 서버에서 `http://<LLM_PRIVATE_IP>:11434/api/generate` 호출 성공

### 스레드 문맥 반영 (2026-02-27 KST)

- 스레드 멘션 시 `conversations.replies`로 같은 스레드 메시지를 읽어 모델 입력에 포함
- 기본 동작
  - `THREAD_CONTEXT_FETCH_LIMIT=100`까지 조회
  - 최근 `THREAD_CONTEXT_MAX_MESSAGES=12`개만 사용
  - 총 `THREAD_CONTEXT_MAX_CHARS=5000`자 이내로 절단
- Ollama는 `OLLAMA_TEMPERATURE=0.0` 기본값으로 설정해 답변 흔들림을 완화
- 나열형 질문은 스레드 원문 기준 누락 없이 순서대로 답변하도록 공통 시스템 프롬프트 강화

### 조사 모드 구현 계획 (2026-02-27 KST)

- 트리거 문구: 스레드에서 `@Boxer 스레드 맥락 분석`
- 트리거 감지 시 일반 Q&A 모드가 아닌 조사 모드로 라우팅
- 조사 모드 기본 흐름
  - 스레드 전체 문맥 요약 + 핵심 키 추출 (`고객`, `시간대`, `session_id`, `request_id`, `에러코드`)
  - 키 누락 시 질문 1회로 보강
  - 바코드가 있으면 사용자 식별 API를 먼저 조회해 컨텍스트 보강
  - 필요 소스 판단 후 retrieval 실행 (`Notion`, `S3`, `RDS`)
  - 근거 병합/중복 제거 후 LLM이 결론 작성
- 조사 모드 응답 형식
  - `결론`
  - `확인한 근거` (출처 링크/로그 경로/조회 시각)
  - `다음 조치`
  - `추가 확인 필요 항목` (있을 때만)

### 자연어 자동 해석 방식 (튜닝 아님)

- 목표: LLM이 SQL을 직접 생성하지 않고, 자연어를 구조화된 조회 요청으로 변환
- 처리 순서
  - `intent 분류`: 예) `video_shoot_dates_by_barcode`
  - `slot 추출`: 예) `barcode`, `date_from`, `date_to`, `limit`
  - `slot 검증`: 누락/포맷 오류 시 보강 질문 1회
  - `조회 실행`: 의도별 allowlist SQL/API 템플릿에 파라미터 바인딩
  - `응답 생성`: 조회 결과를 요약하고 근거를 함께 제시
- 원칙
  - 모델 파인튜닝 없이 프롬프트 + 라우팅/검증 코드로 구현
  - DB는 읽기 전용 쿼리만 허용
  - PII(이름/전화번호)는 기본 마스킹 후 필요한 경우에만 노출

### 바코드 유저 조회 API (Lambda)

- 엔드포인트: `GET https://bh63r1dl09.execute-api.ap-northeast-2.amazonaws.com/prod/app-user?barcode=<BARCODE>`
- 제공 값: `산모 이름`, `산모 전화번호`, `userSeq`, `babySeq`, `babyNickname`
- 활용 방식
  - 조사 모드에서 바코드가 감지되면 1차로 호출
  - 조회된 `userSeq/babySeq`를 DB/로그/문서 조회 키로 재사용
  - 최종 응답에는 필요한 범위만 노출하고 민감정보는 마스킹

### 공통 프롬프트 정책 (2026-02-27 KST)

- 시스템 프롬프트는 혼합형으로 운영: 규칙/제약은 영어 문장으로 명시
- 언어 정책: 기본 한국어, 영어 질문에는 영어 답변
- 톤 정책: 반말 고정, 존댓말(요/습니다체) 금지
- 답변 정책: 핵심 우선, 간결 답변(기본 3~6문장), 근거 부족 시 추측 금지

## Phase 2 - LLM 우선 연동 (Provider 분리)

RAG 데이터 소스를 붙이기 전에 LLM 응답 파이프라인을 먼저 완성한다.

- [x] `LLM_PROVIDER` 기반 라우팅 구현 (`ollama` / `claude`)
- [ ] `LLM_PROVIDER` 확장 (`openai`)
- [ ] 공통 인터페이스 구현 (`generate_answer(prompt, context)`)
- [x] Slack 멘션 -> LLM 답변 스레드 응답 E2E (`ollama` / `claude`)
- [ ] 답변 가드레일 추가 (근거 부족 시 추정 금지/모르면 모른다고 답변)
- [x] provider별 설정을 `.env`로 분리

## Phase 3 - 단일 소스 RAG + 조사 모드 기본기

먼저 한 소스만 붙여 retrieval 품질과 프롬프트 구조를 검증한다.

- [ ] 조사 모드 트리거 인식 (`스레드 맥락 분석`)
- [ ] intent/slot JSON 스키마 정의 (`video_shoot_dates_by_barcode` 포함)
- [ ] 스레드 키 정보 추출기 구현 (`시간대`, `세션/요청 ID`, `에러 키워드`)
- [ ] 키 누락 시 보강 질문 1회 정책 적용
- [ ] 바코드 감지 시 app-user API 선조회 라우팅
- [ ] 우선 소스 선택 (Notion 또는 S3)
- [ ] 문서 수집/정규화
- [ ] retrieval 결과를 LLM context에 주입
- [ ] 근거 포함 답변 형식 검증

## Phase 4 - 멀티 소스 확장 (S3 + Notion + RDS)

- [ ] 조사 모드에서 소스별 병렬 조회 실행기 구현
- [ ] Lambda app-user API 연동 (실패/timeout/retry 정책 포함)
- [ ] S3 로그 fetch + 날짜/키워드/ID 필터링
- [ ] Notion 페이지 검색 + 본문 추출
- [ ] RDS 조회 연결 (보안그룹/계정 포함)
- [ ] 소스별 우선순위/충돌 규칙 정의
- [ ] 통합 retrieval 결과를 단일 context로 구성
- [ ] 출처/조회시각을 포함한 근거 블록 표준화
- [ ] PII 마스킹 정책 적용 (전화번호/이름)

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
