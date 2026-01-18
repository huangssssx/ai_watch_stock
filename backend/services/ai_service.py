from openai import OpenAI
import json
from typing import Dict, Any, Optional, Tuple

class AIService:
    def _truncate_text(self, text: str, limit: int = 2000) -> str:
        normalized = text or ""
        if len(normalized) <= limit:
            return normalized
        return normalized[:limit]

    def _should_embed_system_prompt_in_user(self, ai_config: Dict[str, Any]) -> bool:
        base_url = (ai_config.get("base_url") or "").strip().lower()
        if not base_url:
            return False
        if "api.openai.com" in base_url:
            return False
        if "openai.com" in base_url:
            return False
        return True

    def _build_system_prompt(self) -> str:
        return (
            "你是一位拥有20年经验的资深量化基金经理，擅长短线博弈和趋势跟踪。"
            "你的任务是根据提供的股票实时数据和技术指标，给出当前时间点明确的、可执行的交易指令。"
            "\n\n"
            "【分析原则】\n"
            "1. 客观：只基于提供的数据说话，不要幻想未提供的新闻。\n"
            "2. 果断：必须给出明确的方向（买入/卖出/观望），禁止模棱两可。\n"
            "3. 风控：任何开仓建议必须包含止损位。\n"
            "\n\n"
            "【输出要求】\n"
            "你必须只输出一个合法的 JSON 对象（RFC8259）。输出必须以 { 开始、以 } 结束。\n"
            "禁止输出任何非 JSON 内容：禁止 Markdown、禁止代码块标记、禁止解释、禁止前后缀、禁止多余换行或多段输出。\n"
            "字符串必须使用双引号，禁止单引号；禁止尾随逗号；禁止 NaN/Infinity。\n"
            "必须包含且仅包含以下顶层字段：type, signal, action_advice, suggested_position, duration, support_pressure, stop_loss_price, message。\n"
            "support_pressure 必须是对象，且只包含 support 与 pressure。\n"
            "\n"
            "如果你无法严格按要求输出（例如信息不足/计算失败/格式不确定），也必须输出合法 JSON，且按下面的兜底模板返回，不允许输出其他任何内容。\n"
            "\n"
            "兜底模板（请原样输出字段名，仅替换内容）：\n"
            "{\n"
            "  \"type\": \"error\",\n"
            "  \"signal\": \"WAIT\",\n"
            "  \"action_advice\": \"观望\",\n"
            "  \"suggested_position\": \"0成仓\",\n"
            "  \"duration\": \"-\",\n"
            "  \"support_pressure\": {\"support\": \"-\", \"pressure\": \"-\"},\n"
            "  \"stop_loss_price\": \"-\",\n"
            "  \"message\": \"输出受限：请仅返回合法 JSON 对象\"\n"
            "}\n"
            "\n"
            "正常输出格式如下（示例）：\n"
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

    def _build_user_content(self, data_context: str, prompt_template: str, current_time_str: Optional[str] = None) -> str:
        import datetime

        now_str = current_time_str or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return (
            "Current Time: "
            + now_str
            + "\n\n"
            + "Task: Analyze the following market data and generate an investment decision JSON.\n\n"
            + "Analysis Instructions (Strategy):\n"
            + (prompt_template or "")
            + "\n\n"
            + "Real-time Indicators Data:\n"
            + (data_context or "")
            + "\n\n"
            + "Remember: Be decisive. If the signal is strictly strictly strictly unclear, allow 'WAIT'. Otherwise, give a direction.\n"
            + "Return strictly JSON format."
        )

    def analyze(self, data_context: str, prompt_template: str, ai_config: Dict[str, Any], current_time_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Send data to AI and get analysis result.
        ai_config: {api_key, base_url, model_name}
        """
        try:
            client = OpenAI(
                api_key=ai_config["api_key"],
                base_url=ai_config["base_url"],
                timeout=300.0,
            )
            
            system_prompt = self._build_system_prompt()
            user_content = self._build_user_content(data_context, prompt_template, current_time_str=current_time_str)
            
            response = client.chat.completions.create(
                model=ai_config["model_name"],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                response_format={"type": "json_object"}, # Force JSON if model supports it, else prompt engineering
                temperature=ai_config.get("temperature", 0.1),
                
            )
            
            content = response.choices[0].message.content or ""
            
            try:
                # Clean markdown code blocks if present
                clean_content = content.replace("```json", "").replace("```", "").strip()
                result = json.loads(clean_content)
                
                # Ensure signal field exists
                if "signal" not in result:
                    result["signal"] = "WAIT"
                    
                return result, content
            except json.JSONDecodeError as e:
                clean_content = (content or "").replace("```json", "").replace("```", "").strip()
                raw_head = self._truncate_text(clean_content, 500)
                msg = f"AI returned invalid JSON ({str(e)}). raw_head={raw_head}"
                return {
                    "type": "error", 
                    "message": msg,
                    "signal": "WAIT",
                    "parse_error": str(e),
                    "raw_response": clean_content,
                }, content
                
        except Exception as e:
            return {"type": "error", "message": f"AI Error: {str(e)}"}, str(e)

    def analyze_debug(self, data_context: str, prompt_template: str, ai_config: Dict[str, Any], current_time_str: Optional[str] = None) -> Tuple[Dict[str, Any], str, Dict[str, str]]:
        try:
            client = OpenAI(
                api_key=ai_config["api_key"],
                base_url=ai_config["base_url"],
                timeout=300.0,
            )
            
            system_prompt = self._build_system_prompt()
            user_content = self._build_user_content(data_context, prompt_template, current_time_str=current_time_str)

            response = client.chat.completions.create(
                model=ai_config["model_name"],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                response_format={"type": "json_object"},
                temperature=ai_config.get("temperature", 0.1),
            )
            
            content = response.choices[0].message.content or ""
            
            try:
                clean_content = content.replace("```json", "").replace("```", "").strip()
                result = json.loads(clean_content)
                
                if "signal" not in result:
                    result["signal"] = "WAIT"
                    
                return result, content, {"system_prompt": system_prompt, "user_prompt": user_content}
            except json.JSONDecodeError as e:
                clean_content = (content or "").replace("```json", "").replace("```", "").strip()
                raw_head = self._truncate_text(clean_content, 500)
                msg = f"AI returned invalid JSON ({str(e)}). raw_head={raw_head}"
                return {
                    "type": "error", 
                    "message": msg,
                    "signal": "WAIT",
                    "parse_error": str(e),
                    "raw_response": clean_content,
                }, content, {"system_prompt": system_prompt, "user_prompt": user_content}
                
        except Exception as e:
            return {"type": "error", "message": f"AI Error: {str(e)}"}, str(e), {"system_prompt": "", "user_prompt": ""}

    def chat(self, message: str, ai_config: Dict[str, Any], system_prompt: Optional[str] = None) -> str:
        client = OpenAI(
            api_key=ai_config["api_key"],
            base_url=ai_config["base_url"],
            timeout=120.0,
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

    def analyze_raw(self, data_context: str, custom_prompt: str, ai_config: Dict[str, Any]) -> str:
        """
        Send data to AI with only the custom prompt - no system prompts, no JSON format requirement.
        Returns the raw AI response as-is.

        This is for the AI Watch feature where the user provides their own prompt.
        """
        try:
            client = OpenAI(
                api_key=ai_config["api_key"],
                base_url=ai_config["base_url"],
                timeout=300.0,
            )

            # Build user content: data + custom prompt only, no extra formatting
            user_content = f"{data_context}\n\n{custom_prompt}"

            response = client.chat.completions.create(
                model=ai_config["model_name"],
                messages=[
                    {"role": "user", "content": user_content}
                ],
                temperature=ai_config.get("temperature", 0.7),
            )

            return response.choices[0].message.content or ""

        except Exception as e:
            return f"AI Error: {str(e)}"

ai_service = AIService()
