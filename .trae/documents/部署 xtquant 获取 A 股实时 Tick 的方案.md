## 目标
- 在现有项目不依赖爬虫的前提下，引入迅投 QMT/miniQMT 的 xtquant 行情能力，获取 A 股实时 tick（分笔/全推快照/订阅回调）。

## 现状调研结论
- 代码仓库内目前没有 xtquant/QMT/miniQMT 的接入实现或依赖配置；已有“实时 tick”主要来自 Tushare 文档与脚本的轮询快照方式（非订阅推送）。
- 后端依赖以 FastAPI/SQLAlchemy/Tushare/AkShare 为主，[requirements.txt](file:///Users/huangchuanjian/workspace/my_projects/ai_watch_stock/backend/requirements.txt) 未包含 xtquant。

## 关键技术点（你需要知道的约束）
- xtquant 的行情模块 xtdata 本质是与 miniQMT 建立连接，由 miniQMT 负责行情连接与数据落地，再回传给 Python（官方说明）。来源：迅投知识库（xtdata 概述、行情示例）
  - http://dict.thinktrader.net/nativeApi/xtdata.html
  - http://dict.thinktrader.net/nativeApi/code_examples.html
- 实时 tick 获取通常有两类：
  - “快照拉取”：`xtdata.get_full_tick([...])`（单次拿最新）
  - “订阅推送”：`xtdata.subscribe_quote(stock_code, period='tick', callback=...)`，并用 `xtdata.run()` 阻塞主线程以持续接收（官方示例）。来源同上。
- 运行平台：miniQMT/QMT 通常以 Windows 终端形式部署更稳；macOS 直接跑 xtquant 常见会因二进制依赖/配套进程缺失受限，因此建议准备 Windows 节点作为行情网关（即使你的主项目在 macOS）。

## 部署方案（推荐路径）
### 方案 A：把“行情采集 + 本项目后端”都跑在 Windows（最省事）
- 适用：你接受在 Windows 上运行后端服务。
- 优点：不需要跨机器传输 tick；调试最简单。
- 缺点：需要把本项目运行环境迁到 Windows（或至少后端）。

### 方案 B：Windows 跑 xtquant 行情网关；macOS 继续跑本项目（更符合你当前环境）
- 适用：你当前在 macOS 开发/部署主服务，但想用 xtquant 的实时 tick。
- 思路：
  - Windows 上运行“tick 网关进程”（xtquant + miniQMT）。
  - tick 网关把数据通过 HTTP/WebSocket 推给 macOS 后端（或推入消息队列），macOS 后端按需消费/入库/触发策略。
- 优点：主项目保持不动；xtquant 的平台依赖集中在 Windows。
- 缺点：需要设计一个轻量的跨机数据通道（但实现简单、可控）。

## Windows 节点的环境部署步骤（A/B 都需要）
1. 准备 Windows 10/11（物理机或虚拟机都行），网络尽量稳定。
2. 安装并登录 miniQMT/QMT（按迅投官方流程）。确保行情能正常显示。
3. 准备 Python 环境
   - 建议用 conda 或 venv，64 位 Python。
   - Python 版本需与 xtquant 配套（以你拿到的 xtquant whl/安装包为准，常见为 cp38/cp39）。
4. 安装 xtquant
   - 常见做法：使用 miniQMT 配套的 xtquant 安装包/whl（很多场景不在公开 PyPI）。
5. 联通性验证（最小验证脚本）
   - 快照：验证能拿到最新盘口
   - 订阅：验证回调持续输出

示例（用于验证 tick 订阅逻辑，字段以实际返回为准）：
```python
from xtquant import xtdata

code = "600000.SH"

def on_tick(data):
    print(data)

seq = xtdata.subscribe_quote(stock_code=code, period="tick", count=-1, callback=on_tick)
xtdata.run()
```

## 接入本项目的实现计划（以方案 B 为主）
1. 设计“tick 数据协议”
   - 统一字段：`symbol`（如 600000.SH）、`ts`（毫秒时间戳）、`last_price`、`volume/amount`、`bid/ask`（五档可选）、原始字典 `raw`（可选）。
   - 约定编码与时区：统一用 epoch(ms)；落库/展示时再转换。
2. 新增 Windows tick 网关服务
   - 用 FastAPI + WebSocket 或 HTTP POST（项目已依赖 FastAPI/uvicorn，便于复用栈）。
   - 回调函数内不做重计算：只做入队（Queue）+ 批量发送/写入，避免阻塞导致丢数据。
3. macOS 后端增加一个“行情入口”
   - 接收网关推送的 tick（WebSocket client / HTTP endpoint）。
   - 选择落库策略：
     - 只做实时策略触发，不落每笔 tick（性能最好）。
     - 或按分钟/按笔聚合写入（更适合回测/复盘）。
4. 可用性与重连
   - 断线自动重连：网关检测 `on_disconnected`/异常后触发 `reconnect` 或重启进程（迅投示例里也强调断线处理）。来源：迅投知识库示例页 http://dict.thinktrader.net/nativeApi/code_examples.html
   - 进程守护：Windows 任务计划程序 / NSSM / Supervisor（任选其一）。
5. 权限与性能评估
   - 若需要“全市场全推”，通常涉及额外权限/成本；若只订阅少量标的，优先用 `subscribe_quote`。
   - 对订阅标的数量设置上限与动态管理（订阅/退订），避免资源占用。

## 验证与验收标准
- 能在交易时段稳定获得单股 tick（或最接近 tick 的全推数据）并持续 30 分钟不掉线。
- 在断网/重启 miniQMT 后可自动恢复订阅。
- 与项目现有 symbol 规则兼容（.SH/.SZ）。
- 若启用落库：写入前检查目标 DB 文件存在，避免误建新库（遵守项目 DB 安全规则）。

## 风险与替代方案
- 如果 xtquant 在 macOS 无法直接安装/运行：采用方案 B（Windows 网关）规避。
- 若只需要“准实时”而非逐笔：可继续用现有 Tushare 的 realtime_tick/realtime_quote（但属于拉取方式，延迟/频率受限）。

## 下一步（我在你确认后会做什么）
- 在仓库内新增一个“xtquant 行情网关”模块（独立目录/独立运行入口），并提供 macOS 后端对接示例与最小联通测试脚本。
- 增加一份部署文档：Windows 端安装检查清单、端口配置、防火墙与运行方式。