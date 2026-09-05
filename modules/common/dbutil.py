"""
DB 접근 공통 유틸 (정본).

기존에는 거의 모든 화면 모듈이 get_conn / table_exists / view_exists / get_table_columns /
pick_col 을 각자 복사해 두고 있었고(19곳), DB_PATH 정의 방식도 파일마다 달랐습니다.
이 모듈 하나로 통일합니다.

DB 경로 우선순위:
  1) 환경변수 DAILYCIGAR_DB_PATH
  2) 기본값 "cigar.db" (앱 실행 위치 기준 상대경로 — 기존 동작과 동일)
"""
from __future__ import annotations

import os
import sqlite3
from typing import Iterable, Optional, Sequence

DEFAULT_DB_PATH = "cigar.db"


def get_db_path() -> str:
    """현재 시점의 DB 경로. 환경변수를 매 호출마다 다시 읽어 테스트에서 경로를 바꿀 수 있게 한다."""
    return os.getenv("DAILYCIGAR_DB_PATH", DEFAULT_DB_PATH)


def get_conn(row_factory: bool = False) -> sqlite3.Connection:
    """
    sqlite3 커넥션 생성.
    - 화면 모듈들의 기존 get_conn 과 동일하게 check_same_thread=False.
    - row_factory=True 를 주면 sqlite3.Row 를 사용(db.py의 get_conn 과 동일 동작).
    """
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    if row_factory:
        conn.row_factory = sqlite3.Row
    return conn


def object_exists(conn: sqlite3.Connection, name: str, obj_type: Optional[str] = None) -> bool:
    """sqlite_master 에서 이름으로 존재 여부 확인. obj_type 은 'table' / 'view' / None(둘 다)."""
    try:
        if obj_type:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ? LIMIT 1",
                (obj_type, name),
            )
        else:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1",
                (name,),
            )
        return cur.fetchone() is not None
    except Exception:
        return False


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return object_exists(conn, table_name, "table")


def view_exists(conn: sqlite3.Connection, view_name: str) -> bool:
    return object_exists(conn, view_name, "view")


def table_or_view_exists(conn: sqlite3.Connection, name: str) -> bool:
    return object_exists(conn, name, None)


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """PRAGMA table_info 기반 컬럼명 목록. 테이블/뷰 모두 사용 가능. 실패 시 빈 리스트."""
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cur.fetchall()]
    except Exception:
        return []


def pick_col(cols: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    """candidates 순서대로 cols 에 존재하는 첫 컬럼명을 반환. 없으면 None."""
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


# 일부 모듈에서 쓰던 별칭 (의미 동일)
find_existing_column = pick_col
has_table = table_exists
