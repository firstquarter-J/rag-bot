from boxer.company import settings as cs


def _extract_barcode(text: str) -> str | None:
    match = cs.BARCODE_PATTERN.search(text)
    if not match:
        return None
    return match.group(1)
