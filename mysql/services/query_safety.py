import re


READ_ONLY_START_KEYWORDS = ("select", "with")
BLOCKED_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "replace",
    "alter",
    "drop",
    "create",
    "truncate",
    "rename",
    "grant",
    "revoke",
    "call",
    "do",
    "set",
    "use",
    "load",
    "handler",
    "lock",
    "unlock",
    "begin",
    "start transaction",
    "commit",
    "rollback",
)


def validate_read_only_query(query: str) -> None:
    """Raise ValueError unless the query is a single read-only statement."""
    if not query or not query.strip():
        raise ValueError("쿼리를 입력해주세요.")

    normalized = _strip_sql_comments(query).strip()
    if not normalized:
        raise ValueError("쿼리를 입력해주세요.")

    normalized = normalized.rstrip().rstrip(";").strip()
    lowered = normalized.lower()

    if ";" in lowered:
        raise ValueError("여러 SQL 문장은 허용되지 않습니다.")

    if not lowered.startswith(READ_ONLY_START_KEYWORDS):
        raise ValueError("사용자 정의 쿼리는 SELECT 또는 WITH로 시작하는 읽기 전용 쿼리만 허용됩니다.")

    blocked_pattern = r"\b(" + "|".join(re.escape(keyword) for keyword in BLOCKED_KEYWORDS) + r")\b"
    blocked_matches = re.findall(blocked_pattern, lowered)
    blocked_matches = [match for match in blocked_matches if match not in READ_ONLY_START_KEYWORDS]

    if blocked_matches:
        unique_keywords = ", ".join(sorted(set(blocked_matches)))
        raise ValueError(f"읽기 전용이 아닌 SQL 키워드가 포함되어 있습니다: {unique_keywords}")


def _strip_sql_comments(query: str) -> str:
    without_block_comments = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    without_line_comments = re.sub(r"(?m)--.*?$", " ", without_block_comments)
    without_hash_comments = re.sub(r"(?m)#.*?$", " ", without_line_comments)
    return without_hash_comments
