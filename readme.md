# AI Stock Watcher

智能股票监测系统，集成 AkShare 数据源与 LLM 大模型分析。

## 功能特性

- **股票管理**: 添加/删除股票，设置监测频率。
- **指标库**: 统一维护指标配置，每只股票可多选启用指标。
- **自定义指标**: 支持任意 AkShare 接口，支持参数动态替换 (`{symbol}`, `{today}`)。
- **AI 分析**: 对接 OpenAI/SiliconFlow/DeepSeek，自动分析数据并给出建议。
- **自动预警**: 监测到风险时发送邮件通知。
- **可视化**: 实时查看监测日志和 AI 分析结果。

## 快速开始

### 1. 后端 (Python/FastAPI)

```bash
cd backend
pip install -r requirements.txt
python init_db.py  # 初始化数据库（会重建 stock_watch.db）
python3 -m uvicorn main:app --reload --port 8000
```

### 2. 前端 (React)

```bash
cd frontend
npm install
npm run dev
```

打开浏览器访问 `http://localhost:5173`。

## 配置说明

1. 在 **AI 配置** 中配置您的大模型 API（如 SiliconFlow、DeepSeek）。
2. 在 **指标库** 中新增需要的指标：
   - AkShare 接口名：`stock_zh_a_spot_em`
   - 参数 JSON：`{"symbol": "{symbol}"}`
3. 在 **看盘** 添加股票。
4. 点击股票右侧的配置按钮，为该股票选择多个监控指标，并可填写 Prompt。
5. 点击 **开始** 开始监测。

## 环境变量

在 backend 目录下创建 `.env` 配置邮件发送 (可选):

```
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your_email@gmail.com
SENDER_PASSWORD=your_password
RECEIVER_EMAIL=your_email@gmail.com
```

# 选股脚本
cd /Users/huangchuanjian/workspace/my_projects/ai_watch_stock && python3 -m streamlit run backend/scripts/hot_money_stage2_picker.py -- --streamlit