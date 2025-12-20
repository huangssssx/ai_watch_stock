export interface Stock {
  id: number;
  symbol: string;
  name: string;
  is_monitoring: boolean;
  interval_seconds: number;
  monitoring_schedule?: string; // JSON string
  only_trade_days?: boolean;
  prompt_template?: string;
  ai_provider_id?: number;
  indicators: IndicatorDefinition[];
}

export interface IndicatorDefinition {
  id: number;
  name: string;
  akshare_api: string;
  params_json: string;
  post_process_json?: string;
}

export interface AIConfig {
  id: number;
  name: string;
  provider: string;
  base_url: string;
  api_key: string;
  model_name: string;
  temperature?: number;
  max_tokens?: number;
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

export interface AIAnalysisResult {
  type: 'info' | 'warning' | 'error';
  signal: 'STRONG_BUY' | 'BUY' | 'WAIT' | 'SELL' | 'STRONG_SELL';
  action_advice: string;
  suggested_position: string;
  duration: string;
  support_pressure?: {
    support: number | string;
    pressure: number | string;
  };
  stop_loss_price?: number | string;
  message: string;
  [key: string]: unknown;
}

export interface Log {
  id: number;
  stock_id: number;
  timestamp: string;
  raw_data: string;
  ai_response: string;
  ai_analysis: AIAnalysisResult;
  is_alert: boolean;
  stock?: Stock;
}

export interface StockTestRunResponse {
  ok: boolean;
  stock_id: number;
  stock_symbol: string;
  model_name?: string;
  base_url?: string;
  system_prompt: string;
  user_prompt: string;
  ai_reply: AIAnalysisResult;
  data_truncated: boolean;
  data_char_limit?: number | null;
}

export interface EmailConfig {
  smtp_server: string;
  smtp_port: number;
  sender_email: string;
  sender_password: string;
  receiver_email: string;
}

export interface GlobalPromptConfig {
  prompt_template: string;
  account_info?: string;
}

export interface AlertRateLimitConfig {
  enabled: boolean;
  max_per_hour_per_stock: number;
  allowed_signals?: string[];
  allowed_urgencies?: string[];
  suppress_duplicates?: boolean;
  bypass_rate_limit_for_strong_signals?: boolean;
}
