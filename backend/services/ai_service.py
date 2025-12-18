from openai import OpenAI
import json
from typing import Dict, Any, Optional

class AIService:
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
            
            system_prompt = "You are a professional stock analyst. Analyze the provided data and return the result in strictly formatted JSON. The JSON must contain 'type' (string: 'info', 'warning', 'error') and 'message' (string)."
            
            user_content = f"""
            Task: Analyze the following stock data based on the instructions.
            
            Instructions:
            {prompt_template}
            
            Data:
            {data_context}
            
            Return strictly JSON format: {{"type": "...", "message": "..."}}
            """
            
            response = client.chat.completions.create(
                model=ai_config["model_name"],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                response_format={"type": "json_object"} # Force JSON if model supports it, else prompt engineering
            )
            
            content = response.choices[0].message.content
            
            try:
                result = json.loads(content)
                return result, content
            except json.JSONDecodeError:
                return {"type": "error", "message": "AI returned invalid JSON"}, content
                
        except Exception as e:
            return {"type": "error", "message": f"AI Error: {str(e)}"}, str(e)

    def chat(self, message: str, ai_config: Dict[str, Any], system_prompt: Optional[str] = None) -> str:
        client = OpenAI(
            api_key=ai_config["api_key"],
            base_url=ai_config["base_url"]
        )
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": message})
        response = client.chat.completions.create(
            model=ai_config["model_name"],
            messages=messages,
        )
        return response.choices[0].message.content or ""

ai_service = AIService()
