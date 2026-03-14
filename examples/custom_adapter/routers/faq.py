_FAQ_ENTRIES = {
    "what is boxer": (
        "Boxer는 retrieval-grounded assistant bot을 만들기 위한 open core야. "
        "질문 라우팅과 정책은 adapter가 붙여서 완성해."
    ),
    "how does routing work": (
        "라우팅은 core가 자동으로 정하지 않아. adapter가 질문을 분기하고, "
        "필요할 때만 DB/S3/API/Notion helper를 호출해."
    ),
    "what is rga": (
        "RGA는 retrieval-grounded assistant의 줄임말이야. "
        "먼저 근거를 조회하고 그 근거로만 답하게 만드는 방식이야."
    ),
}

_SENSITIVE_HINTS = (
    "customer email",
    "email",
    "phone",
    "ssn",
    "password",
    "token",
    "secret",
)


def _normalize_question(question: str) -> str:
    return " ".join(str(question or "").strip().lower().split())


def find_faq_answer(question: str) -> str | None:
    normalized_question = _normalize_question(question)
    if not normalized_question:
        return None
    return _FAQ_ENTRIES.get(normalized_question)


def is_sensitive_question(question: str) -> bool:
    normalized_question = _normalize_question(question)
    if not normalized_question:
        return False
    return any(token in normalized_question for token in _SENSITIVE_HINTS)
