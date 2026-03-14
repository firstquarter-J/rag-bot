# Boxer: Retrieval-Grounded Assistant Bot

Boxer는 오픈소스로 재사용 가능한 `Retrieval-Grounded Assistant (RGA)` LLM bot을 목표로 하는 프로젝트다.
질문을 받으면 승인된 데이터 소스(DB/S3/API/Notion)를 먼저 조회하고, 그 근거를 바탕으로 답변한다.

핵심 원칙:

- 추측보다 조회 결과
- LLM fallback보다 라우터 우선
- 서버가 실제 조회를 수행하고, LLM은 수집된 근거를 바탕으로만 문장화
- 근거가 없으면 아는 척하지 않고 `없음`, `확인 필요`로 답변

## 이 저장소가 담고 있는 것

- 재사용 가능한 open core
- Slack 기반 sample adapter
- 더 많은 기능이 들어간 company adapter reference implementation
- DB/S3/Notion/request log 같은 retrieval 경로
- Ollama / Claude 기반 retrieval synthesis

지향점은 누구나 open core를 가져다 쓰고, 도메인별 adapter만 바꿔서 자기 조직용 bot을 만들 수 있게 하는 거다.
현재는 그 방향으로 정리 중이고, 이 저장소 안에 있는 `company adapter`가 가장 완성도 높은 레퍼런스 역할을 한다.

## Open Core 와 Domain Adapter

### Open Core

- `boxer/core`
- `boxer/adapters/common`
- `boxer/adapters/sample`
- `boxer/routers/common`

공통 설정, Slack 공통 래퍼, LLM 호출, request log, 저수준 DB/S3/Notion helper 같은 재사용 가능한 기반을 둔다.

### Domain Adapter

- `boxer/company`
- `boxer/adapters/company`
- `boxer/routers/company`

도메인 규칙, 권한 정책, prompt, env, 구체적인 라우터와 connector 조합을 둔다.

이 저장소의 기본 경계 원칙은 단순하다.

- open core에는 회사 고유 규칙을 넣지 않는다
- 회사 전용 정책과 도메인 로직은 adapter/domain 모듈에만 둔다

## 요청 처리 흐름

1. Slack 이벤트와 질문을 정규화한다
2. 라우터와 정책 가드가 직접 처리 가능한 질문을 먼저 잡는다
3. 승인된 DB/S3/API/Notion 조회만 서버가 실행한다
4. 수집된 근거를 합치고 필요하면 LLM이 근거 기반으로 최종 문장을 만든다
5. 응답과 요청 메타데이터를 남긴다

즉, `LLM이 임의로 조회를 상상하는 구조`가 아니라 `서버가 먼저 근거를 확보하고 LLM은 그 근거를 바탕으로만 답하는 구조`다.

## 환경 파일

- `.env.example`: open core / 공통 key만 기록
- `.env.company.example`: company adapter에서 참고할 회사 전용 key만 기록
- `.env`: 실제 실행 값만 기록

실제 비밀값은 `.env`에만 두고 커밋하지 않는다.

## 빠른 시작

### 1) Sample Adapter

`sample adapter`는 open core 동작 확인용 최소 구현이다.
회사 전용 설정 없이 Slack 응답 흐름만 smoke test할 수 있다.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

최소 환경 변수:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SLACK_SIGNING_SECRET`

선택:

- `ADAPTER_ENTRYPOINT=boxer.adapters.sample.slack:create_app`
- LLM 기능을 실험할 때만 `LLM_PROVIDER`와 provider별 env 추가

실행:

```bash
scripts/smoke_sample_adapter.sh
python app.py
```

예시:

- `@Bot ping`

### 2) Company Adapter

`company adapter`는 더 많은 retrieval 라우터가 붙은 reference implementation이다.

```bash
cp .env.example .env
```

그 다음 `.env.company.example`를 참고해서 필요한 key를 같은 `.env`에 추가한다.

필수:

- `ADAPTER_ENTRYPOINT=boxer.adapters.company.slack:create_app`

기능별 설정 예시:

- DB / 바코드 / 구조화 조회: `DB_QUERY_ENABLED`, `DB_*`
- S3 explicit 조회: `S3_QUERY_ENABLED`, `AWS_REGION`, `S3_*`
- Notion 문서 질의: `NOTION_*`
- 요청 로그 저장: `REQUEST_LOG_*`
- 회사 전용 정책/권한/장비 기능: `.env.company.example`의 관련 key
- retrieval synthesis / 일반 질문: `LLM_PROVIDER`와 provider별 key

실행:

```bash
python app.py
```

## Company Adapter 사용 예시

최신 예시는 Slack에서 `@Bot 사용법`이라고 멘션하면 바로 볼 수 있다.
README에는 대표 예시만 정리한다.

### 기본

- `@Bot 사용법`
- `@Bot ping`

### 바코드 녹화 기록 조회

- `@Bot 12345678910 영상 개수`
- `@Bot 12345678910 영상 목록`
- `@Bot 12345678910 영상 정보`
- `@Bot 12345678910 마지막 녹화일`
- `@Bot 12345678910 전체 녹화 날짜 목록`
- `@Bot 12345678910 2026-03-06 녹화 기록`

### 로그 / 원인 분석

- `@Bot 12345678910 로그 분석`
- `@Bot 12345678910 2026-03-06 로그 분석`
- `@Bot 12345678910 2026-03-06 로그 에러 분석`
- `@Bot 12345678910 2026-03-06 녹화 실패 원인 분석`

### 장비 파일

- `@Bot 12345678910 2026-03-06 파일 있나`
- `@Bot 12345678910 2026-03-06 fileid`
- `@Bot 12345678910 2026-03-06 파일 다운로드`

### 구조화 조회

- `@Bot 2026년 병원 개수`
- `@Bot 병원명 서울병원 병실 목록`
- `@Bot 장비명 MB-200 장비 상태`
- `@Bot 2026-03-06 캡처 개수`
- `@Bot 병원명 서울병원 2026-03-06 영상 개수`

### 운영 조회

- `@Bot s3 영상 12345678910`
- `@Bot s3 로그 MB-200 2026-03-06`
- `@Bot db 조회 select seq, fullBarcode from recordings limit 3`
- `@Bot 요청 로그 최근 20`
- `@Bot 요청 로그 사용자 오늘`
- `@Bot 요청 로그 라우트 어제`
- `@Bot 요청 통계 오늘`

### 문서 / 일반 질문

- `@Bot 마미박스 동기화 안 될 때 조치`

## 조회 기준 차이

비슷해 보여도 조회 기준이 다른 명령이 있다.

- `12345678910 ...` 형태의 바코드 녹화 질문은 주로 `recordings` DB 기준으로 본다
- `s3 영상 <바코드>`는 S3 버킷의 raw object 존재 여부를 직접 본다
- `s3 로그 <장비명> <YYYY-MM-DD>`는 S3의 raw log file을 직접 읽는다

예를 들어 `바코드 녹화 기록`은 DB에 반영된 서비스 기준 상태를 보는 용도고,
`s3 영상`은 원본 파일이 실제 저장소에 있는지 확인하는 용도다.

## 내 adapter를 붙이는 방법

새 도메인을 붙일 때는 open core를 수정하기보다 adapter를 추가하는 쪽을 권장한다.

1. `boxer.adapters.sample.slack`를 시작점으로 삼는다
2. 도메인별 정책, prompt, env, 라우터를 별도 모듈로 둔다
3. `ADAPTER_ENTRYPOINT=<your_module>:create_app` 으로 연결한다
4. 공통으로 재사용 가능한 코드는 `boxer/core`, `boxer/adapters/common`, `boxer/routers/common`에만 올린다

이 저장소의 `company adapter`는 그 구조를 보여주는 reference implementation이다.

## 검증 스크립트

```bash
scripts/smoke_sample_adapter.sh
```

- sample adapter smoke test

```bash
scripts/verify_open_core_boundary.sh
```

- open core / company 경계가 깨지지 않았는지 확인

```bash
.venv/bin/python scripts/verify_usage_examples.py
```

- company adapter 사용 예시가 실제 라우터 기준으로 동작하는지 검증
- live DB/S3/Notion 접근 권한과 env가 있어야 한다

## 보안 / 운영 원칙

- DB 조회는 read-only만 허용
- 민감 조회는 정책 가드에서 차단
- 장비 파일 다운로드 링크는 공개 채널이 아니라 DM으로만 전달
- request log는 SQLite 기반으로 저장하고, 필요하면 S3 snapshot backup을 붙일 수 있다
- 비밀값은 `.env`에만 두고 예제 파일에는 key만 남긴다
