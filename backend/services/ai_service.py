from openai import OpenAI
import json
from typing import Dict, Any, Optional

class AIService:
    def _should_embed_system_prompt_in_user(self, ai_config: Dict[str, Any]) -> bool:
        base_url = (ai_config.get("base_url") or "").strip().lower()
        if not base_url:
            return False
        if "api.openai.com" in base_url:
            return False
        if "openai.com" in base_url:
            return False
        return True

    def analyze(self, data_context: str, prompt_template: str, ai_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send data to AI and get analysis result.
        ai_config: {api_key, base_url, model_name}
        """
        try:
            client = OpenAI(
                api_key=ai_config["api_key"],
                base_url=ai_config["base_url"]
            )
            
            system_prompt = (
                "你是一位拥有20年经验的资深量化基金经理，擅长短线博弈和趋势跟踪。"
                "你的任务是根据提供的股票实时数据和技术指标，给出当前时间点明确的、可执行的交易指令。"
                "\n\n"
                "【分析原则】\n"
                "1. 客观：只基于提供的数据说话，不要幻想未提供的新闻。\n"
                "2. 果断：必须给出明确的方向（买入/卖出/观望），禁止模棱两可。\n"
                "3. 风控：任何开仓建议必须包含止损位。\n"
                "\n\n"
                "【输出要求】\n"
                "请严格只输出一个合法的 JSON 对象，不要包含 Markdown 代码块标记（如 ```json），格式如下：\n"
                "{\n"
                "  \"type\": \"info\" | \"warning\" | \"error\",  // info=正常分析, warning=数据不足或风险极高, error=无法分析\n"
                "  \"signal\": \"STRONG_BUY\" | \"BUY\" | \"WAIT\" | \"SELL\" | \"STRONG_SELL\", // 明确的信号\n"
                "  \"action_advice\": \"...\", // 一句话的大白话操作建议，例如：'现价25.5元立即买入，目标27元'\n"
                "  \"suggested_position\": \"...\", // 建议仓位，例如：'3成仓' 或 '空仓观望'\n"
                "  \"duration\": \"...\", // 建议持仓时间，例如：'短线T+1' 或 '中线持股2周'\n"
                "  \"support_pressure\": {\"support\": 价格, \"pressure\": 价格}, // 支撑压力位\n"
                "  \"stop_loss_price\": 价格, // 严格的止损价格\n"
                "  \"message\": \"...\" // 详细的逻辑分析摘要，解释为什么这么做，不超过100字\n"
                "}"
            )
            
            import datetime
            current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            user_content = f"""
            Current Time: {current_time_str}
            
            Task: Analyze the following market data and generate an investment decision JSON.
            
            Analysis Instructions (Strategy):
            {prompt_template}
            
            Real-time Indicators Data:
            {data_context}
            
            Remember: Be decisive. If the signal is strictly strictly strictly unclear, allow 'WAIT'. Otherwise, give a direction.
            Return strictly JSON format.
            """
            
            response = client.chat.completions.create(
                model=ai_config["model_name"],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                response_format={"type": "json_object"}, # Force JSON if model supports it, else prompt engineering
                temperature=ai_config.get("temperature", 0.1)
            )
            
            content = response.choices[0].message.content
            
            try:
                # Clean markdown code blocks if present
                clean_content = content.replace("```json", "").replace("```", "").strip()
                result = json.loads(clean_content)
                
                # Ensure signal field exists
                if "signal" not in result:
                    result["signal"] = "WAIT"
                    
                return result, content
            except json.JSONDecodeError:
                return {
                    "type": "error", 
                    "message": "AI returned invalid JSON",
                    "signal": "WAIT"
                }, content
                
        except Exception as e:
            return {"type": "error", "message": f"AI Error: {str(e)}"}, str(e)

    def chat(self, message: str, ai_config: Dict[str, Any], system_prompt: Optional[str] = None) -> str:
        client = OpenAI(
            api_key=ai_config["api_key"],
            base_url=ai_config["base_url"]
        )
        normalized_system_prompt = (system_prompt or "").strip()
        normalized_message = (message or "").strip()

        if normalized_system_prompt:
            if self._should_embed_system_prompt_in_user(ai_config):
                user_content = (
                    "系统指令：\n"
                    f"{normalized_system_prompt}\n\n"
                    "用户输入：\n"
                    f"{normalized_message}"
                )
                messages = [{"role": "user", "content": user_content}]
            else:
                messages = [
                    {"role": "system", "content": normalized_system_prompt},
                    {"role": "user", "content": normalized_message},
                ]
        else:
            messages = [{"role": "user", "content": normalized_message}]
        response = client.chat.completions.create(
            model=ai_config["model_name"],
            messages=messages,
            temperature=ai_config.get("temperature", 0.7)
        )
        return response.choices[0].message.content or ""

ai_service = AIService()
