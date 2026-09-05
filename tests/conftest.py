"""
pytest 공통 설정.

- 실제 cigar.db 를 임시 디렉터리에 복사해 그 사본으로 테스트한다 (원본 무변경 보장).
- 모듈들이 상대경로 "cigar.db" 와 환경변수 DAILYCIGAR_DB_PATH 두 방식으로 DB 를 찾으므로
  둘 다 사본을 가리키게 맞춘다 (cwd 를 임시 디렉터리로, 환경변수도 설정).

실행:  cd <repo>  &&  python -m pytest tests -q
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

SOURCE_DB = REPO_ROOT / "cigar.db"


@pytest.fixture(scope="session")
def db_path(tmp_path_factory) -> Path:
    if not SOURCE_DB.exists():
        pytest.skip("cigar.db 가 없어 통합 테스트를 건너뜁니다.")
    work = tmp_path_factory.mktemp("dbcopy")
    dst = work / "cigar.db"
    shutil.copy2(SOURCE_DB, dst)

    os.environ["DAILYCIGAR_DB_PATH"] = str(dst)
    os.chdir(work)  # 상대경로 "cigar.db" 를 쓰는 모듈(db.py 등)도 사본을 보게 함

    import db as dbmod  # noqa: E402
    dbmod.DB_PATH = str(dst)
    return dst


@pytest.fixture()
def conn(db_path):
    c = sqlite3.connect(str(db_path))
    try:
        yield c
    finally:
        c.close()


def _months_with_data(path: Path) -> list[tuple[int, int]]:
    c = sqlite3.connect(str(path))
    try:
        rows = c.execute(
            """
            SELECT DISTINCT substr(sale_date, 1, 7) AS ym FROM (
                SELECT sale_date FROM retail_sales
                UNION ALL
                SELECT sale_date FROM wholesale_sales
            ) WHERE ym IS NOT NULL AND ym <> '' ORDER BY ym
            """
        ).fetchall()
    finally:
        c.close()
    out = []
    for (ym,) in rows:
        try:
            y, m = ym.split("-")
            out.append((int(y), int(m)))
        except Exception:
            continue
    return out


@pytest.fixture(scope="session")
def months(db_path) -> list[tuple[int, int]]:
    ms = _months_with_data(db_path)
    if not ms:
        pytest.skip("판매 데이터가 있는 월이 없습니다.")
    return ms
