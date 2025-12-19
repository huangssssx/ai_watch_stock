I will modify `backend/services/monitor_service.py` to address your requirements:

1. **Filter out "Wait" signals**:

   * I will modify the alert trigger logic to **strictly ignore** signals like "WAIT", "HOLD", or "OBSERVE", even if the AI marks them as a "warning".

   * This directly addresses your request to only be notified about "Buy" or "Sell" suggestions.

2. **Deduplicate Alerts**:

   * I will introduce a mechanism to store the content of the last sent alert for each stock.

   * Before sending a new alert, I will compare it with the previous one. If the signal and advice are identical, the alert will be suppressed.

   * This prevents repetitive notifications when the AI's advice hasn't changed.

**Implementation Details:**

* In `backend/services/monitor_service.py`:

  * Add `_last_alert_content_by_stock_id` dictionary to track alert history.

  * Update the `is_alert` condition to: `is_alert = signal.upper() not in ["WAIT", "HOLD"]`.

  * Inside the alert block, add a check: if `current_content == last_content`, skip sending.

This will effectively stop the flood of "Wait and see" (空仓观望) notifications while ensuring you still get important Buy/Sell signals.

\
日志还是正常发送和更新，日志发送没有任何限制需要限制的只是发邮件，其次每个股票监控日志只保留最近三日的数据，避免过度膨胀

