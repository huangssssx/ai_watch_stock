#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将触底反弹监控脚本注入数据库 (RuleScript表)
"""

import sys
import os
import argparse

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from database import SessionLocal
from models import RuleScript

def insert_rebound_rule(script_path: str, force_update: bool):
    """将触底反弹监控脚本插入数据库"""
    
    # 读取脚本内容
    if not os.path.exists(script_path):
        print(f"❌ 脚本文件不存在: {script_path}")
        return

    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    db = SessionLocal()
    try:
        rule_name = "触底反弹监控"
        existing = db.query(RuleScript).filter(RuleScript.name == rule_name).first()
        
        if existing:
            print(f"⚠️  规则 '{rule_name}' 已存在 (ID: {existing.id})")
            do_update = bool(force_update)
            if not do_update:
                # 自动模式下默认更新，或者提示用户
                print("是否要更新脚本内容? (y/n): ", end='')
                choice = input().lower()
                do_update = choice == 'y'
            
            if do_update:
                existing.code = script_content
                existing.description = "捕捉处于下跌趋势或低位盘整中，出现MA5突破+MACD/RSI共振反转信号的股票。"
                db.commit()
                print(f"✅ 已更新规则脚本 (ID: {existing.id})")
            else:
                print("❌ 取消更新")
            return
        
        # 创建新规则
        rule = RuleScript(
            name=rule_name,
            description="捕捉处于下跌趋势或低位盘整中，出现MA5突破+MACD/RSI共振反转信号的股票。",
            code=script_content
        )
        
        db.add(rule)
        db.commit()
        db.refresh(rule)
        
        print(f"✅ 成功插入规则脚本到数据库")
        print(f"   规则ID: {rule.id}")
        print(f"   规则名称: {rule.name}")
        
    except Exception as e:
        print(f"❌ 插入失败: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    default_path = os.path.join(os.path.dirname(__file__), "跟踪策略", "触底反弹监控.py")
    p.add_argument("--file", type=str, default=default_path)
    p.add_argument("--force", action="store_true", help="Force update if exists")
    args = p.parse_args()

    script_path = os.path.abspath(args.file)
    insert_rebound_rule(script_path=script_path, force_update=bool(args.force))
