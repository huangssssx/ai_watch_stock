
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将【洗盘终结与拉升启动】选股脚本注入数据库
"""

import sys
import os
import argparse

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from database import SessionLocal
from models import StockScreener

def insert_wash_markup_script(script_path: str, target_id: int, force_update: bool):
    """将选股脚本插入数据库"""
    
    # 读取脚本内容
    if not os.path.exists(script_path):
        print(f"❌ 错误：找不到脚本文件 {script_path}")
        return

    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    db = SessionLocal()
    try:
        existing = None
        if target_id is not None:
            existing = db.query(StockScreener).filter(StockScreener.id == int(target_id)).first()
        if existing is None:
            existing = db.query(StockScreener).filter(StockScreener.name == "洗盘拉升突破").first()
        
        if existing:
            print(f"⚠️  策略 '洗盘拉升突破' 已存在 (ID: {existing.id})")
            do_update = bool(force_update)
            if not do_update:
                print("是否要更新脚本内容? (y/n): ", end='')
                choice = input().lower()
                do_update = choice == 'y'
            if do_update:
                existing.script_content = script_content
                existing.description = "基于'洗盘终结研究'：寻找长期缩量洗盘后，突然放量突破均线压制的个股。"
                db.commit()
                print(f"✅ 已更新策略脚本 (ID: {existing.id})")
            else:
                print("❌ 取消更新")
            return
        
        # 创建新策略
        screener = StockScreener(
            name="洗盘拉升突破",
            description="基于'洗盘终结研究'：寻找长期缩量洗盘后，突然放量突破均线压制的个股。",
            script_content=script_content,
            cron_expression="0 10 * * *",  # 每天10:00执行（早盘确认）
            is_active=False  # 默认不激活
        )
        
        db.add(screener)
        db.commit()
        db.refresh(screener)
        
        print(f"✅ 成功插入选股策略到数据库")
        print(f"   策略ID: {screener.id}")
        print(f"   策略名称: {screener.name}")
        print(f"   Cron表达式: {screener.cron_expression}")
        print(f"   状态: {'激活' if screener.is_active else '未激活'}")
        
    except Exception as e:
        print(f"❌ 插入失败: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    # 默认指向刚刚创建的脚本
    default_script = os.path.join(os.path.dirname(__file__), "选股策略", "洗盘_拉升_策略.py")
    p.add_argument("--file", type=str, default=default_script)
    p.add_argument("--id", type=int, default=None)
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    script_path = os.path.abspath(args.file)
    insert_wash_markup_script(script_path=script_path, target_id=args.id, force_update=bool(args.force))
