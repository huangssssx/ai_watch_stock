import os
import sys
import builtins
try:
    getattr(sys.stdout, 'flush', lambda: None)()
except Exception:
    try:
        sys.stdout = open(os.devnull, 'w')
    except Exception:
        pass
try:
    getattr(sys.stderr, 'flush', lambda: None)()
except Exception:
    try:
        sys.stderr = open(os.devnull, 'w')
    except Exception:
        pass
_orig_print = builtins.print
def print(*args, **kwargs):
    try:
        _orig_print(*args, **kwargs)
    except OSError:
        pass
os.environ.setdefault('TQDM_DISABLE', '1')
_backend_dir = os.path.dirname(os.path.abspath(__file__))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
from pymr_compat import ensure_py_mini_racer
ensure_py_mini_racer()
import akshare as ak

## 求每个股票的PE值
df = ak.stock_financial_analysis_indicator(symbol="000001",start_year="2026")