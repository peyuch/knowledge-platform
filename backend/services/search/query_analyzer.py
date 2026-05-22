"""Query analysis and routing — keyword rules + LLM fallback."""

RELATION_KEYWORDS = ["谁", "负责", "审批", "汇报", "归属", "属于", "参与", "流程", "怎么", "如何"]
FACT_KEYWORDS = ["什么是", "定义", "规定", "标准", "办法", "制度", "多少", "金额", "日期", "时间"]
SHORT_QUERY_THRESHOLD = 8


def analyze_query(query: str) -> dict:
    """Classify query into fact/concept/rel + short/long."""
    is_short = len(query) < SHORT_QUERY_THRESHOLD

    rel_score = sum(1 for kw in RELATION_KEYWORDS if kw in query)
    fact_score = sum(1 for kw in FACT_KEYWORDS if kw in query)

    if rel_score > fact_score:
        query_type = "rel"
    elif fact_score > 0:
        query_type = "fact"
    else:
        query_type = "concept"

    return {"query_type": query_type, "is_short": is_short, "rewritten_query": None}
