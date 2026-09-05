"""지출 엑셀 업로드 자동분류 규칙 매칭 로직 단위 테스트 (DB 불필요)."""
import pandas as pd

from modules.finance.finance_expense_upload import match_rule


def _rules(rows):
    return pd.DataFrame(rows, columns=["id", "keyword", "action", "expense_category_id", "priority"])


def test_priority_then_longer_keyword_wins():
    rules = _rules([
        (1, "해외송금", "exclude", None, 10),
        (2, "망포역플래티넘", "assign", 2, 50),
        (3, "상가비", "assign", 9, 50),
    ])
    m = match_rule("망포역플래티넘 상가비", rules)
    assert m.action == "assign" and m.expense_category_id == 2 and m.keyword == "망포역플래티넘"


def test_exclude_rule():
    rules = _rules([(1, "해외송금", "exclude", None, 10)])
    m = match_rule("해외송금 대체", rules)
    assert m.action == "exclude" and m.expense_category_id is None


def test_case_insensitive_and_no_match():
    rules = _rules([(1, "kwe", "assign", 5, 50)])
    assert match_rule("KWE운임비등 BZ뱅크", rules).expense_category_id == 5
    assert match_rule("박진환 BZ뱅크", rules).action == "none"
    assert match_rule("", rules).action == "none"
    assert match_rule("아무거나", _rules([])).action == "none"
