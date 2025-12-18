export interface Stock {
  id: number;
  symbol: string;
  name: string;
  is_monitoring: boolean;
  interval_seconds: number;
  prompt_template?: string;
  ai_provider_id?: number;
  indicators: IndicatorDefinition[];
}

export interface IndicatorDefinition {
  id: number;
  name: string;
  akshare_api: string;
  params_json: string;
}

export interface AIConfig {
  id: number;
  name: string;
  provider: string;
  base_url: string;
  api_key: string;
  model_name: string;
  is_active: boolean;
}

export interface AIConfigTestRequest {
  prompt_template?: string;
  data_context?: string;
}

export interface AIConfigTestResponse {
  ok: boolean;
  parsed: {
    type: string;
    message: string;
    [key: string]: unknown;
  };
  raw: string;
}

export interface Log {
  id: number;
  stock_id: number;
  timestamp: string;
  raw_data: string;
  ai_response: string;
  ai_analysis: {
    type: string;
    message: string;
  };
  is_alert: boolean;
}

export interface StockTestRunResponse {
  ok: boolean;
  stock_id: number;
  stock_symbol: string;
  model_name?: string;
  base_url?: string;
  system_prompt: string;
  user_prompt: string;
  ai_reply: string;
  data_truncated: boolean;
  data_char_limit?: number | null;
}
