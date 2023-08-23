import sys
import os
from dotenv import dotenv_values

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ
