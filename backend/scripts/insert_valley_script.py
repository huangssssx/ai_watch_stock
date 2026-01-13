#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将山谷狙击选股脚本注入数据库
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from database import SessionLocal
from models import StockScreener

def insert_valley_sniper_script():
    """将山谷狙击选股脚本插入数据库"""
    
    # 读取脚本内容
    script_path = os.path.join(os.path.dirname(__file__), '选股策略', '山谷狙击选股策略.py')
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    db = SessionLocal()
    try:
        # 检查是否已存在
        existing = db.query(StockScreener).filter(
            StockScreener.name == "山谷狙击选股"
        ).first()
        
        if existing:
            print(f"⚠️  策略 '山谷狙击选股' 已存在 (ID: {existing.id})")
            print("是否要更新脚本内容? (y/n): ", end='')
            choice = input().lower()
            if choice == 'y':
                existing.script_content = script_content
                existing.description = "基于缩量、均线支撑、MACD/RSI底背离的山谷买点策略，避免追高买在半山腰"
                db.commit()
                print(f"✅ 已更新策略脚本 (ID: {existing.id})")
            else:
                print("❌ 取消更新")
            return
        
        # 创建新策略
        screener = StockScreener(
            name="山谷狙击选股",
            description="基于缩量、均线支撑、MACD/RSI底背离的山谷买点策略，避免追高买在半山腰",
            script_content=script_content,
            cron_expression="0 15 * * *",  # 每天15:00执行（收盘后）
            is_active=False  # 默认不激活，用户手动激活
        )
        
        db.add(screener)
        db.commit()
        db.refresh(screener)
        
        print(f"✅ 成功插入选股策略到数据库")
        print(f"   策略ID: {screener.id}")
        print(f"   策略名称: {screener.name}")
        print(f"   Cron表达式: {screener.cron_expression}")
        print(f"   状态: {'激活' if screener.is_active else '未激活'}")
        print(f"\n💡 提示：")
        print(f"   - 在Web界面中可以查看和运行此策略")
        print(f"   - 点击 'Run Now' 立即执行")
        print(f"   - 激活后将按Cron表达式定时执行")
        
    except Exception as e:
        print(f"❌ 插入失败: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    insert_valley_sniper_script()
