import sqlite3
import json
import time
import sys
import os
from openai import OpenAI

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "backend"))

# Configuration
LOG_ID = 108
DB_PATH = "backend/stock_watch.db"

def reproduce():
    print(f"=== Reproducing Monitor Flow for Log ID {LOG_ID} ===")
    
    # 1. Load Data from DB
    if not os.path.exists(DB_PATH):
        print(f"Error: DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Fetch Log
    log = cur.execute("SELECT * FROM logs WHERE id=?", (LOG_ID,)).fetchone()
    if not log:
        print(f"Error: Log {LOG_ID} not found")
        conn.close()
        return
        
    raw_data = log['raw_data']
    
    # Fetch AI Config (assuming Gemini, id=2 based on previous check)
    ai_config = cur.execute("SELECT * FROM ai_configs WHERE id=2").fetchone()
    conn.close()
    
    if not ai_config:
        print("Error: AI Config (ID 2) not found")
        return

    print(f"AI Provider: {ai_config['name']} ({ai_config['base_url']})")

    # 2. Extract Payload
    marker = "AI Request Payload:\n"
    idx = raw_data.find(marker)
    if idx < 0:
        print("Error: Payload marker not found in raw_data")
        return
        
    payload_text = raw_data[idx + len(marker):].strip()
    try:
        payload = json.loads(payload_text)
    except Exception as e:
        print(f"Error parsing payload JSON: {e}")
        return

    messages = payload.get("messages", [])
    if not messages:
        print("Error: No messages in payload")
        return

    # 3. Analyze Prompts
    system_msg = next((m for m in messages if m['role'] == 'system'), None)
    user_msg = next((m for m in messages if m['role'] == 'user'), None)

    if system_msg:
        print("\n--- System Prompt Analysis ---")
        content = system_msg['content']
        print(f"Length: {len(content)} chars")
        if "//" in content:
            print("WARNING: System prompt contains comments ('//'), which might confuse some models.")
        print("Preview:")
        print(content[:500] + "..." if len(content) > 500 else content)

    if user_msg:
        print("\n--- User Prompt Analysis ---")
        content = user_msg['content']
        print(f"Original Length: {len(content)} chars")
        
        # VERIFY FIX: FULL PAYLOAD
        print(">>> VERIFYING FIX: FULL PAYLOAD (NO TRUNCATION) <<<")
        # user_msg['content'] = content # It is already full content
    
    # VERIFY FIX: NEW SYSTEM PROMPT
    if system_msg:
        print(">>> VERIFYING FIX: NEW SYSTEM PROMPT <<<")
        system_msg['content'] = (
            "你是一位拥有20年经验的资深量化基金经理，擅长短线博弈和趋势跟踪。\n"
            "你的任务是根据提供的股票实时数据和技术指标，给出当前时间点明确的、可执行的交易指令。\n\n"
            "【分析原则】\n"
            "1. 客观：只基于提供的数据说话，不要幻想未提供的新闻。\n"
            "2. 果断：必须给出明确的方向（买入/卖出/观望），禁止模棱两可。\n"
            "3. 风控：任何开仓建议必须包含止损位。\n\n"
            "【输出要求】\n"
            "请严格只输出一个合法的 JSON 对象，不要包含 Markdown 代码块标记（如 ```json），格式如下：\n"
            "{\n"
            "  \"type\": \"info\",\n"
            "  \"signal\": \"STRONG_BUY\",\n"
            "  \"action_advice\": \"现价25.5元立即买入，目标27元\",\n"
            "  \"suggested_position\": \"3成仓\",\n"
            "  \"duration\": \"短线T+1\",\n"
            "  \"support_pressure\": {\"support\": 24.0, \"pressure\": 28.0},\n"
            "  \"stop_loss_price\": 23.5,\n"
            "  \"message\": \"详细逻辑分析...\"\n"
            "}\n"
            "注意：type可选 info/warning/error；signal可选 STRONG_BUY/BUY/WAIT/SELL/STRONG_SELL。"
        )

    # 4. Call AI
    print("\n--- Calling AI ---")
    client = OpenAI(
        api_key=ai_config['api_key'],
        base_url=ai_config['base_url'],
        timeout=300.0
    )
    
    start_time = time.time()
    try:
        response = client.chat.completions.create(
            model=ai_config['model_name'],
            messages=messages,
            response_format={"type": "json_object"},
            temperature=ai_config['temperature']
        )
        duration = time.time() - start_time
        content = response.choices[0].message.content
        
        print(f"AI Call Finished in {duration:.2f}s")
        print("\n--- Raw AI Response ---")
        print(repr(content))
        
        # 5. Parse Result
        print("\n--- Parsing Result ---")
        clean_content = content.replace("```json", "").replace("```", "").strip()
        try:
            parsed = json.loads(clean_content)
            print("✅ JSON Parse Success")
            print(json.dumps(parsed, indent=2, ensure_ascii=False))
            
            if "signal" not in parsed:
                print("❌ Missing 'signal' field")
            elif parsed["signal"] not in ["STRONG_BUY", "BUY", "WAIT", "SELL", "STRONG_SELL"]:
                print(f"⚠️ Unknown signal value: {parsed['signal']}")
                
        except json.JSONDecodeError as e:
            print(f"❌ JSON Parse Failed: {e}")
            
    except Exception as e:
        print(f"❌ AI Call Error: {e}")

if __name__ == "__main__":
    reproduce()
