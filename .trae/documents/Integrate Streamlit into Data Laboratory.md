# 集成 Streamlit 到数据实验室计划

## 目标
允许用户在“数据实验室”中编写 Python 代码，并以 Streamlit 应用的形式实时运行和预览，从而利用 Streamlit 强大的交互和可视化能力。

## 核心方案
1. **后端 (Backend)**:
   - 引入 `streamlit` 库。
   - 在后台启动一个常驻的 Streamlit 服务进程，监听特定的脚本文件（如 `backend/streamlit_runner.py`）。
   - 提供 API 接口，用于将用户在前端编写的代码保存到该脚本文件。Streamlit 的自动重载机制会立即更新应用。
2. **前端 (Frontend)**:
   - 在“数据实验室”界面增加“运行 Streamlit”按钮。
   - 增加一个专门的 Tab 页用于显示 Streamlit 界面（通过 `iframe` 嵌入）。

## 详细实施步骤

### 1. 依赖更新
- 修改 `backend/requirements.txt`，添加 `streamlit`。

### 2. 后端开发
- **创建 Streamlit 管理服务** (`backend/services/streamlit_service.py`):
  - 实现启动 Streamlit 子进程的逻辑：`streamlit run backend/streamlit_runner.py --server.port 8501 --server.headless true`。
  - 实现更新脚本内容的逻辑：将用户代码写入 `backend/streamlit_runner.py`。
  - 确保服务只启动一次，并在应用启动时初始化。
- **更新应用入口** (`backend/main.py`):
  - 在 `startup_event` 中调用 Streamlit 服务启动函数。
- **新增 API 路由** (`backend/routers/research.py`):
  - 新增接口 `POST /research/streamlit/run`。
  - 接收代码内容，更新文件，并返回 Streamlit 服务的 URL (默认 `http://localhost:8501`)。

### 3. 前端开发 (`frontend/src/components/ResearchPage.tsx`)
- **UI 更新**:
  - 在工具栏增加 "Run as Streamlit" 按钮。
  - 在下方的 `Tabs` 组件中增加 "Streamlit" 标签页。
- **交互逻辑**:
  - 点击运行按钮时，调用新 API 保存代码。
  - 成功后，自动切换到 "Streamlit" 标签页。
  - `iframe` 重新加载以确保显示最新内容。

### 4. 验证计划
- 编写一段简单的 Streamlit 代码（如 `st.write("Hello")`），点击运行，确认 iframe 中正确显示。
- 修改代码并再次运行，确认内容更新。
