export interface Stock {
  id: number;
  symbol: string;
  name: string;
  is_monitoring: boolean;
  interval_seconds: number;
  monitoring_schedule?: string; // JSON string
  only_trade_days?: boolean;
  prompt_template?: string;
  remark?: string;
  ai_provider_id?: number;
  monitoring_mode?: 'ai_only' | 'script_only' | 'hybrid';
  rule_script_id?: number;
  indicators: IndicatorDefinition[];
  is_pinned?: boolean;
}

export interface StockPricePoint {
  open: number;
  close: number;
  high?: number;
  low?: number;
  volume?: number;
  amount?: number;
  date?: string;
  time?: string;
  [key: string]: unknown;
}

export interface RuleScript {
  id: number;
  name: string;
  description?: string;
  code: string;
  created_at?: string;
  updated_at?: string;
  is_pinned?: boolean;
}

export interface RuleTestPayload {
  symbol: string;
}

export interface RuleTestResponse {
  triggered: boolean;
  message: string;
  log: string;
}

export interface IndicatorDefinition {
  id: number;
  name: string;
  akshare_api?: string | null;
  params_json?: string | null;
  post_process_json?: string | null;
  python_code?: string | null;
  is_pinned?: boolean;
}

export interface IndicatorTestRequest {
  symbol: string;
  name?: string;
}

export interface IndicatorTestResponse {
  ok: boolean;
  indicator_id: number;
  indicator_name: string;
  symbol: string;
  raw: string;
  parsed?: unknown;
  error?: string | null;
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
  run_id?: string | null;
  stock_id: number;
  stock_symbol: string;
  monitoring_mode?: 'ai_only' | 'script_only' | 'hybrid' | string | null;
  skipped_reason?: string | null;
  error?: string | null;

  script_triggered?: boolean | null;
  script_message?: string | null;
  script_log?: string | null;

  model_name?: string | null;
  base_url?: string | null;
  system_prompt?: string | null;
  user_prompt?: string | null;
  ai_reply?: AIAnalysisResult | null;
  raw_response?: string | null;

  data_truncated?: boolean | null;
  data_char_limit?: number | null;

  fetch_ok?: number | null;
  fetch_error?: number | null;
  fetch_errors?: { indicator?: string; api?: string; duration_ms?: number; error?: string }[] | null;
  ai_duration_ms?: number | null;

  is_alert?: boolean | null;
  alert_attempted?: boolean | null;
  alert_suppressed?: boolean | null;
  alert_result?: unknown;
  prompt_source?: string | null;
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
  bypass_rate_limit_for_strong_signals?: boolean;
}

export interface ResearchScript {
  id: number;
  title: string;
  description?: string;
  script_content: string;
  last_run_at?: string;
  last_run_status?: string;
  created_at?: string;
  updated_at?: string;
  is_pinned?: boolean;
}

export interface ResearchRunResponse {
  success: boolean;
  log: string;
  result?: Record<string, unknown>[];
  chart?: Record<string, unknown>;
  error?: string;
}

export interface StockAIWatchConfig {
  id: number;
  stock_id: number;
  indicator_ids: string; // JSON string
  custom_prompt: string;
  ai_provider_id?: number;
  analysis_history: string; // JSON string
  updated_at?: string;
}

export interface AIWatchAnalyzeRequest {
  indicator_ids: number[];
  custom_prompt: string;
  ai_provider_id?: number;
}

export interface AIWatchAnalyzeResponse {
  ok: boolean;
  stock_symbol?: string;
  ai_reply?: AIAnalysisResult;
  raw_response?: string;
  data_truncated?: boolean;
  fetch_ok?: number;
  fetch_error?: number;
  system_prompt?: string;
  user_prompt?: string;
  error?: string;
}

export interface IndicatorPreviewResponse {
  ok: boolean;
  data?: Record<string, unknown>;
  error?: string;
}

export interface StockNews {
  id: number;
  title: string;
  content: string;
  source?: string | null;
  publish_time?: string | null;
  url?: string | null;
  related_stock_codes?: string | null;
  created_at?: string | null;
}

export interface SentimentAnalysis {
  id: number;
  target_type?: string;
  target_value?: string;
  sentiment_score: number;
  policy_orientation?: string | null;
  trading_signal: string;
  summary?: string | null;
  raw_response?: string | null;
  ai_provider_id?: number | null;
  timestamp?: string | null;
}
