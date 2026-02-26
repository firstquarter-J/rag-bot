# Commit Convention

## Format

타입: 이모지 제목 (최대 50자, 한글 가능, 명령문, 마침표 금지)

본문 (무엇을, 왜 - 어떻게 X)

꼬리말 (optional)

첫 커밋은 `initial commit`

## Type & Emoji

| 타입     | 이모지 | 용도                             |
| -------- | ------ | -------------------------------- |
| feat     | ✨     | 새 기능                          |
| add      | ➕     | 부수적 코드/파일/라이브러리 추가 |
| fix      | 🐛     | 버그 수정                        |
| refactor | ♻️     | 로직 변경 없이 코드 개선         |
| docs     | 📝     | 문서 추가/수정                   |
| chore    | 🔧     | 빌드, 패키지 등 기타             |
| remove   | 🗑️     | 코드/파일 삭제                   |
| style    | 💄     | 포맷, 세미콜론 등                |
| test     | ✅     | 테스트 코드                      |

## Example

feat: ✨ Slack Bolt ping-pong 응답 구현

@Boxer 멘션 수신 시 pong 응답
Socket Mode 연결 확인용 기본 구조

Resolve: #1

## Deployment Rule

- 기본 작업 범위는 `코드 수정 + 로컬 테스트`까지만 수행
- `커밋`은 사용자의 명시적 지시가 있을 때만 수행
- `푸시`는 사용자의 명시적 지시가 있을 때만 수행
- `EC2 반영`은 사용자의 명시적 지시가 있을 때만 수행
- 배포 요청 시 작업 순서는 반드시 `커밋 -> 푸시 -> EC2 반영`
