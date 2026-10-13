"""
공통 UI 테마/사이드바 (정본).

모든 화면 파일에 복사되어 있던 사이드바 네비게이션 블록과, 화면을 꾸미는
전역 CSS를 이 모듈 하나로 통일한다. 화면 파일은 아래 두 함수만 호출한다.

    from modules.common.theme import inject_global_theme, render_sidebar
    st.set_page_config(page_title="...", layout="wide")
    inject_global_theme()
    render_sidebar(active="소매관리")
"""
from __future__ import annotations

import streamlit as st

# (사이드바 표시 라벨, 페이지 경로, 구분선 뒤에 올지 여부)
NAV_ITEMS = [
    ("HOME", "DAILY_CIGAR.py"),
    ("대시보드", "pages/1_대시보드.py"),
    ("기준정보", "pages/2_기준정보.py"),
    None,  # divider
    ("수입관리", "pages/3_수입관리.py"),
    ("소매관리", "pages/4_소매관리.py"),
    ("도매관리", "pages/4_도매관리.py"),
    ("재무관리", "pages/5_재무관리.py"),
    ("분석", "pages/6_분석.py"),
    None,
    ("문서출력", "pages/7_문서출력.py"),
    ("매장운영", "pages/8_매장운영.py"),
    ("재고관리", "pages/9_재고관리.py"),
]


def render_sidebar(active: str | None = None) -> None:
    """공통 사이드바 네비게이션. active 는 NAV_ITEMS 의 라벨과 (이모지 제외하고) 일치하면 강조 표시된다."""
    with st.sidebar:
        st.markdown('<div class="dc-sidebar-logo">🍂 DAILY CIGAR</div>', unsafe_allow_html=True)
        for item in NAV_ITEMS:
            if item is None:
                st.divider()
                continue
            label, path = item
            is_active = active is not None and label.startswith(active)
            st.page_link(path, label=label, icon="🔸" if is_active else None)


def inject_global_theme() -> None:
    """앱 전체에 적용되는 전역 CSS(사이드바/카드/버튼/탭/표 등). 화면당 1회 호출."""
    st.markdown(
        """
        <style>
        :root {
            --dc-maroon: #6e2a34;
            --dc-maroon-dark: #4a1c24;
            --dc-gold: #c9a227;
            --dc-gold-light: #e8c766;
            --dc-cream: #faf6f0;
            --dc-espresso: #241712;
            --dc-espresso-light: #32221b;
            --dc-text: #2b2420;
        }

        /* ---------- 전체 배경/타이포 ---------- */
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, var(--dc-cream) 0%, #ffffff 420px);
        }
        html, body, [class*="css"] {
            color: var(--dc-text);
        }
        h1 {
            font-weight: 800 !important;
            letter-spacing: -0.02em;
            padding-bottom: 0.35rem;
            border-bottom: 3px solid transparent;
            border-image: linear-gradient(90deg, var(--dc-gold), transparent) 1;
        }
        h2, h3 {
            font-weight: 700 !important;
            letter-spacing: -0.01em;
        }

        /* ---------- 사이드바 ---------- */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, var(--dc-espresso) 0%, var(--dc-espresso-light) 100%);
        }
        [data-testid="stSidebar"] * {
            color: #f1e7d8 !important;
        }
        .dc-sidebar-logo {
            font-size: 1.3rem;
            font-weight: 800;
            letter-spacing: 0.02em;
            color: var(--dc-gold-light) !important;
            padding: 0.4rem 0 1rem 0;
            border-bottom: 1px solid rgba(201, 162, 39, 0.35);
            margin-bottom: 0.6rem;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] {
            border-radius: 8px;
            margin: 2px 0;
            transition: background 0.15s ease, transform 0.1s ease;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"]:hover {
            background: rgba(201, 162, 39, 0.18);
            transform: translateX(2px);
        }
        [data-testid="stSidebar"] hr {
            border-color: rgba(201, 162, 39, 0.25) !important;
        }

        /* ---------- 카드형 지표(st.metric) ---------- */
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid rgba(110, 42, 52, 0.08);
            border-top: 3px solid var(--dc-gold);
            border-radius: 12px;
            padding: 0.9rem 1rem 0.7rem 1rem;
            box-shadow: 0 2px 10px rgba(110, 42, 52, 0.06);
            transition: box-shadow 0.15s ease, transform 0.15s ease;
        }
        [data-testid="stMetric"]:hover {
            box-shadow: 0 6px 18px rgba(110, 42, 52, 0.12);
            transform: translateY(-1px);
        }
        [data-testid="stMetricLabel"] {
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            font-size: 0.72rem !important;
            color: #8a7a6a !important;
        }
        [data-testid="stMetricValue"] {
            font-weight: 800 !important;
            color: var(--dc-maroon-dark) !important;
        }
        [data-testid="stMetricValue"] [data-testid="stMarkdownContainer"],
        [data-testid="stMetricValue"] [data-testid="stMarkdownContainer"] p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: unset !important;
            font-size: 1.65rem !important;
        }

        /* ---------- 버튼 ---------- */
        .stButton > button, .stFormSubmitButton > button, .stDownloadButton > button {
            background: linear-gradient(135deg, var(--dc-maroon) 0%, var(--dc-maroon-dark) 100%);
            color: #fff;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            box-shadow: 0 2px 6px rgba(110, 42, 52, 0.25);
            transition: box-shadow 0.15s ease, transform 0.1s ease;
        }
        .stButton > button:hover, .stFormSubmitButton > button:hover, .stDownloadButton > button:hover {
            box-shadow: 0 4px 12px rgba(110, 42, 52, 0.35);
            transform: translateY(-1px);
            color: #fff;
        }

        /* ---------- 탭 ---------- */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
            border-bottom: 1px solid rgba(110, 42, 52, 0.12);
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            font-weight: 600;
            color: #8a7a6a;
        }
        .stTabs [aria-selected="true"] {
            color: var(--dc-maroon-dark) !important;
            border-bottom: 3px solid var(--dc-gold) !important;
        }

        /* ---------- 표/데이터프레임 ---------- */
        [data-testid="stDataFrame"], [data-testid="stTable"] {
            border-radius: 10px;
            overflow: hidden;
            border: 1px solid rgba(110, 42, 52, 0.08);
        }

        /* ---------- 알림 박스 ---------- */
        div[data-testid="stAlert"] {
            border-radius: 10px;
        }

        /* ---------- 구분선 ---------- */
        [data-testid="stAppViewContainer"] hr {
            border: none;
            height: 2px;
            background: linear-gradient(90deg, var(--dc-gold) 0%, transparent 70%);
            opacity: 0.5;
        }

        /* ---------- 진행률 바 ---------- */
        [data-testid="stProgress"] > div > div > div {
            background: linear-gradient(90deg, var(--dc-gold) 0%, var(--dc-maroon) 100%) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
