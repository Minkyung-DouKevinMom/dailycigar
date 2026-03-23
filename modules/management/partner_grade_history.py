import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from modules.management.partner_grade_history_management import render

render()
