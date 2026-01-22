我已确认本地脚本 `scripts/tushare_indicator_script.py` 验证通过，且数据库中确实尚未包含此脚本。

接下来的执行计划如下：

1. **创建同步脚本** (`scripts/sync_tushare_script_to_db.py`)：

   * 读取本地 `scripts/tushare_indicator_script.py` 的完整代码内容。

   * 连接数据库 `backend/stock_watch.db`。

   * 将代码写入 `indicator_definitions` 表，指标名称定为 **"Tushare全维数据"**。

   * 设置 `akshare_api` 为空（激活纯脚本模式），并标记为置顶 (`is_pinned=1`)。

2. **执行同步**：

   * 运行该同步脚本，完成入库操作。

3. **最终验证**：

   * 查询数据库，确认记录已存在。

   * 清理临时同步脚本。

