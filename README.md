# Boxer - 사내 CS RAG 챗봇

마미박스 CS 팀 질문에 S3 로그 + Notion + RDS를 RAG 방식으로 조회하고, Ollama LLM이 자동 답변하는 Slack 챗봇.

## 현재 구현 원칙 (Phase 1)

- Slack Bolt는 **Socket Mode**로 먼저 검증 (초기 HTTPS 인바운드 구성 불필요)
- Python은 **3.11+** 사용
- 패키지 관리는 전역 설치보다 **`venv` 권장**
- 토큰 관리는 최종적으로 Secrets Manager를 사용하되, 초기 ping-pong 검증은 **임시 하드코딩**으로 빠르게 확인 가능
- 하드코딩 사용 시 절대 커밋 금지, 검증 후 Secrets Manager 방식으로 전환

## Phase 1 - Slack Bolt ping-pong

- [x] boxer-role IAM Role EC2에 연결
- [x] GitHub 레포 생성 (firstquarter-J/rag-bot)
- [ ] EC2에 Python 3.11+ 설치
- [ ] slack-bolt 패키지 설치
- [ ] Secrets Manager에 Slack 토큰 저장
- [ ] app.py 작성 (pong 응답)
- [ ] Slack에서 @Boxer ping -> pong 확인

## Phase 2 - S3 로그 읽기

- [ ] S3 로그 버킷 구조 파악
- [ ] 날짜 파싱 로직 구현
- [ ] boto3로 로그 파일 fetch + 필터링
- [ ] Slack 출력 확인

## Phase 3 - Notion 페이지 읽기

- [ ] Notion API 키 Secrets Manager 저장
- [ ] 키워드 기반 페이지 검색 구현
- [ ] 페이지 본문 텍스트 추출 로직
- [ ] Slack 출력 확인

## Phase 4 - RDS 조회

- [ ] Secrets Manager에 DB 계정 저장
- [ ] RDS 보안그룹 인바운드 3306 EC2 SG 허용
- [ ] pymysql 연결 + SELECT 쿼리 구현
- [ ] Slack 출력 확인

## Phase 5 - LLM 연동

- [ ] t3.medium -> t3.large 업그레이드
- [ ] Ollama 설치 + 모델 로드
- [ ] Phase 2~4 context 주입 로직 구현
- [ ] 자연어 질문 -> AI 답변 E2E 테스트

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
Slack            <-> Slack Bolt Layer  \
                                        RAG Engine (S3 + Notion + RDS + LLM)
box-admin-client <-> FastAPI / WS API  /
```
