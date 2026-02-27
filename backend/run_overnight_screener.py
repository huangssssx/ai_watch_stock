#!/usr/bin/env python3
"""运行隔夜套利选股脚本，使用策略2（平衡型）"""

import sys
import os

backend_dir = os.path.dirname(os.path.abspath(__file__))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from scripts.一夜持股法_实盘 import main

if __name__ == "__main__":
    # 使用策略2（平衡型）
    main(profile_key="2")
