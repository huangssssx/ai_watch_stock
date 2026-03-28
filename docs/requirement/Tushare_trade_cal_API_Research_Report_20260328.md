# Tushare trade_cal API Research Report

**Date**: March 28, 2026  
**Researcher**: AI Watch Stock Team  
**Purpose**: Investigate trade_cal API returning empty results and document correct usage  
**Source**: Official Tushare Documentation + Community Issues Analysis  

## Executive Summary

The Tushare trade_cal API returns empty results primarily due to incorrect parameter formatting, date range issues, insufficient积分 (credits), and misunderstanding of optional parameter behavior. This report documents the official API specifications, common pitfalls, and solutions.

## Official API Documentation

**Source**: [Tushare Pro - Trading Calendar API](https://tushare.pro/document/2?doc_id=26)  
**Interface**: `trade_cal`  
**Description**: Get trading calendar data for major exchanges, defaults to Shanghai Stock Exchange  
**Required Credits**: 2000  

### Input Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| exchange | str | No | Exchange: SSE (上交所), SZSE (深交所), CFFEX (中金所), SHFE (上期所), CZCE (郑商所), DCE (大商所), INE (上能源) |
| start_date | str | No | Start date (Format: YYYYMMDD) |
| end_date | str | No | End date (Format: YYYYMMDD) |
| is_open | str | No | Trading status: '0' (holiday), '1' (trading) |

### Output Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| exchange | str | Exchange (SSE/SZSE) |
| cal_date | str | Calendar date |
| is_open | str | Trading status: 0 (closed), 1 (open) |
| pretrade_date | str | Previous trading day |

### Official Usage Examples

**Source**: [Tushare Pro Documentation](https://tushare.pro/document/2?doc_id=26)  

```python
import tushare as ts

# Method 1: Using pro_api
pro = ts.pro_api()
df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')

# Method 2: Using query
df = pro.query('trade_cal', start_date='20180101', end_date='20181231')
```

**Data Sample**:
```
   exchange  cal_date  is_open
0        SSE  20180101        0
1        SSE  20180102        1
2        SSE  20180103        1
```

## Critical Date Format Requirements

**⚠️ CRITICAL ISSUE**: Date format must be **YYYYMMDD** (8 digits, no separators)

### Correct Format Examples:
- ✅ `'20260101'` (January 1, 2026)
- ✅ `'20260328'` (March 28, 2026)
- ❌ `'2026-01-01'` (with dashes - WILL CAUSE EMPTY RESULTS)
- ❌ `'2026/01/01'` (with slashes - WILL CAUSE EMPTY RESULTS)
- ❌ `'01-01-2026'` (wrong order - WILL CAUSE EMPTY RESULTS)

## Common Causes of Empty Results

### 1. **Date Format Errors** (Most Common)
**Issue**: Using incorrect date formats like 'YYYY-MM-DD' or 'YYYY/MM/DD'  
**Evidence**: Multiple GitHub issues report empty results due to format errors  
**Solution**: Always use 'YYYYMMDD' format

### 2. **Insufficient Date Range**
**Issue**: Querying future dates or date ranges with no trading data  
**Example**: `start_date='20261201', end_date='20261231'` (future dates)  
**Solution**: Verify dates are in the past and within available data range

### 3. **Missing or Invalid积分 (Credits)**
**Issue**: API requires 2000 credits, insufficient credits return empty results  
**Solution**: Check account积分 balance and upgrade if necessary

### 4. **Incorrect Parameter Combinations**
**Issue**: Using `is_open` parameter incorrectly  
**Note**: According to GitHub issue #1141, `is_open` should be int type but documented as str  
**Solution**: Try both string ('0', '1') and integer (0, 1) formats

### 5. **Empty Date Range**
**Issue**: Not providing any date parameters returns limited data  
**Solution**: Always specify start_date and end_date for predictable results

## Getting the Last Trading Day - Recommended Approaches

### Approach 1: Query Recent Dates and Filter
```python
import tushare as ts
from datetime import datetime, timedelta

pro = ts.pro_api()

# Get recent dates (last 10 days)
today = datetime.now()
start_date = (today - timedelta(days=10)).strftime('%Y%m%d')
end_date = today.strftime('%Y%m%d')

df = pro.trade_cal(start_date=start_date, end_date=end_date)

# Filter for trading days and get the most recent
last_trading_day = df[df['is_open'] == '1']['cal_date'].iloc[-1]
print(f"Last trading day: {last_trading_day}")
```

### Approach 2: Work Backwards from Today
```python
import tushare as ts

pro = ts.pro_api()

# Start from today and work backwards
today = '20260328'  # Current date in YYYYMMDD format

# Get last 5 days to ensure we find a trading day
df = pro.trade_cal(start_date='20260323', end_date=today)

# Sort by date descending and find first trading day
df_sorted = df.sort_values('cal_date', ascending=False)
last_trading_day = df_sorted[df_sorted['is_open'] == '1']['cal_date'].iloc[0]
print(f"Last trading day: {last_trading_day}")
```

### Approach 3: Use pretrade_date Field
```python
import tushare as ts

pro = ts.pro_api()

# Get today's info (if today is trading day)
df = pro.trade_cal(start_date='20260328', end_date='20260328')

if df.iloc[0]['is_open'] == '1':
    print(f"Today ({df.iloc[0]['cal_date']}) is a trading day")
else:
    # Get previous trading day from pretrade_date
    prev_trading_day = df.iloc[0]['pretrade_date']
    print(f"Today is holiday, last trading day was: {prev_trading_day}")
```

## Known API Issues and Limitations

### Issue #1607: end_date Parameter Recognition Error
- **Problem**: end_date parameter sometimes not recognized correctly  
- **Workaround**: Extend end_date by 1-2 days to ensure inclusion  
- **Reference**: https://github.com/waditu/tushare/issues/1607

### Issue #1141: is_open Parameter Type Mismatch
- **Problem**: Documentation says str type, but API expects int  
- **Workaround**: Try both `'1'` and `1` for trading days  
- **Reference**: https://github.com/waditu/tushare/issues/1141

### Issue #1635: Incorrect Data Return
- **Problem**: API sometimes returns incorrect trading status  
- **Solution**: Cross-validate with multiple date queries  
- **Reference**: https://github.com/waditu/tushare/issues/1635

## Best Practices

1. **Always validate date format** before API call
2. **Check积分 balance** programmatically before making calls
3. **Implement retry logic** with different parameter formats
4. **Cache results** to avoid repeated API calls
5. **Handle empty results gracefully** with fallback strategies
6. **Log API responses** for debugging parameter issues

## Debugging Checklist

When trade_cal returns empty results:

- [ ] Verify date format is YYYYMMDD (8 digits, no separators)
- [ ] Check that dates are not in the future
- [ ] Verify积分 balance ≥ 2000
- [ ] Try with minimal parameters: `pro.trade_cal()`
- [ ] Test with known trading dates (e.g., '20260102' - should be trading)
- [ ] Try both string and integer for is_open parameter
- [ ] Extend date range to ensure coverage
- [ ] Check API status and network connectivity

## Code Examples for Testing

### Basic Connectivity Test
```python
import tushare as ts

pro = ts.pro_api()

# Minimal call - should return some data
df = pro.trade_cal()
print(f"Records returned: {len(df)}")
print(df.head())
```

### Parameter Validation Test
```python
import tushare as ts

pro = ts.pro_api()

# Test various date formats
test_cases = [
    ('20260102', '20260105'),  # Correct format
    ('2026-01-02', '2026-01-05'),  # Incorrect format
]

for start, end in test_cases:
    try:
        df = pro.trade_cal(start_date=start, end_date=end)
        print(f"Dates {start} to {end}: {len(df)} records")
    except Exception as e:
        print(f"Error with {start} to {end}: {e}")
```

## References

1. **Official Documentation**: https://tushare.pro/document/2?doc_id=26
2. **GitHub Repository**: https://github.com/waditu/tushare
3. **Data Tools**: https://tushare.pro/webclient/
4. **Community Issues**: https://github.com/waditu/tushare/issues?q=trade_cal

### Specific Issue References:
- Issue #1607: end_date parameter recognition error
- Issue #1141: is_open parameter type mismatch  
- Issue #1635: incorrect data return
- Issue #1240: trade_cal调用老是失败 (resolved)

## Conclusion

The most common cause of empty results from trade_cal API is incorrect date formatting. Always use 'YYYYMMDD' format without separators. The API requires 2000积分 and may have parameter type inconsistencies. Implement robust error handling and parameter validation to ensure reliable operation.

For the failing script mentioned in the downstream task, focus on:
1. Verifying date parameter format
2. Checking积分 balance
3. Adding error handling for empty responses
4. Implementing fallback strategies for finding the last trading day
