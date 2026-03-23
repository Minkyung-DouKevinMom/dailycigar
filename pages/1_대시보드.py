import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from modules.dashboard.dashboard_finance_summary import render

render()
