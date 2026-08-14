"""
Root entry point wrapper for Streamlit Web UI.
Provides 100% backward compatibility for both `streamlit run app.py` and `streamlit run web_ui/app.py`.
"""

import os
import sys

# Ensure root directory is in python path
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from web_ui.app import main

if __name__ == "__main__":
    main()
