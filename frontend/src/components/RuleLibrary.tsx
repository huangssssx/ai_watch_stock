import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, message, Space, Card, Tag, Tooltip } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined, InfoCircleOutlined, PushpinOutlined, PushpinFilled } from '@ant-design/icons';
import type { RuleScript, RuleTestResponse } from '../types';
import { getRules, createRule, updateRule, deleteRule, testRule } from '../api';

type RuleFormValues = {
  name: string;
  description?: string;
  code: string;
};

const RuleLibrary: React.FC = () => {
  const [rules, setRules] = useState<RuleScript[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingRule, setEditingRule] = useState<RuleScript | null>(null);
  const [form] = Form.useForm();
  
  // Test State
  const [testModalVisible, setTestModalVisible] = useState(false);
  const [testSymbol, setTestSymbol] = useState("sh600519");
  const [testResult, setTestResult] = useState<RuleTestResponse | null>(null);
  const [testing, setTesting] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await getRules();
      const data = res.data;
      data.sort((a, b) => {
          if (!!a.is_pinned === !!b.is_pinned) return 0;
          return a.is_pinned ? -1 : 1;
      });
      setRules(data);
    } catch {
      message.error('Failed to load rules');
    } finally {
      setLoading(false);
    }
  };

  const togglePin = async (record: RuleScript) => {
      try {
          await updateRule(record.id, { is_pinned: !record.is_pinned });
          fetchData();
      } catch {
          message.error('Failed to update pin status');
      }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleEdit = (record: RuleScript) => {
    setEditingRule(record);
    form.setFieldsValue(record);
    setModalVisible(true);
  };

  const handleCreate = () => {
    setEditingRule(null);
    form.resetFields();
    form.setFieldsValue({
      code: `import akshare as ak
# Example: Check if price > 20
# df = ak.stock_zh_a_spot_em()
# price = ...

triggered = False
message = "Not Triggered"
`
    });
    setModalVisible(true);
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteRule(id);
      message.success('Rule deleted');
      fetchData();
    } catch {
      message.error('Failed to delete rule (it might be in use)');
    }
  };

  const handleSave = async (values: RuleFormValues) => {
    try {
      if (editingRule) {
        await updateRule(editingRule.id, values);
        message.success('Rule updated');
      } else {
        await createRule(values);
        message.success('Rule created');
      }
      setModalVisible(false);
      fetchData();
    } catch {
      message.error('Failed to save rule');
    }
  };

  const openTestModal = (record: RuleScript) => {
    setEditingRule(record);
    setTestResult(null);
    setTestModalVisible(true);
  };

  const runTest = async () => {
    if (!editingRule) return;
    setTesting(true);
    try {
      const res = await testRule(editingRule.id, { symbol: testSymbol });
      setTestResult(res.data);
    } catch {
      message.error('Test failed');
    } finally {
      setTesting(false);
    }
  };

  const ruleHelp = (
    <div style={{ maxWidth: 760, whiteSpace: 'normal' }}>
      <div style={{ fontWeight: 600, marginBottom: 8 }}>规则脚本编写说明</div>

      <div style={{ fontWeight: 600, marginBottom: 6 }}>1）可用“占位符”（系统注入变量）</div>
      <div style={{ marginBottom: 10, lineHeight: 1.7 }}>
        <div><code>symbol</code>：当前股票代码（例如 <code>sh600519</code>），脚本里用它去拉取数据。</div>
        <div><code>ak</code>：<code>akshare</code> 模块（已注入），可直接调用如 <code>ak.stock_zh_a_hist(...)</code>。</div>
        <div><code>pd</code>：<code>pandas</code> 模块（已注入），用于表格数据处理。</div>
        <div><code>np</code>：<code>numpy</code> 模块（已注入），用于数值计算。</div>
        <div><code>datetime</code>：Python <code>datetime</code> 模块（已注入），用于时间处理。</div>
        <div><code>time</code>：Python <code>time</code> 模块（已注入），用于延时/时间戳等。</div>
        <div><code>triggered</code>：脚本输出（必填，<code>bool</code>），<code>True</code> 表示规则触发。</div>
        <div><code>message</code>：脚本输出（必填，<code>str</code>），建议写“为什么触发/不触发”的简短说明。</div>
      </div>

      <div style={{ fontWeight: 600, marginBottom: 6 }}>2）规则必须遵循的格式</div>
      <div style={{ marginBottom: 10, lineHeight: 1.7 }}>
        <div>脚本是一段“可直接执行”的 Python 代码，不需要定义函数。</div>
        <div>必须在脚本里给 <code>triggered</code> 与 <code>message</code> 赋值（建议先设默认值，再根据条件覆盖）。</div>
        <div>调试时可用 <code>print(...)</code> 输出日志：测试面板会捕获并展示 <code>print</code> 输出。</div>
        <div>建议避免长时间阻塞/死循环，保证单次执行尽量快速。</div>
      </div>

      <div style={{ fontWeight: 600, marginBottom: 6 }}>示例（带完整注释，可直接复制修改）</div>
      <pre style={{ margin: 0, background: '#f6f6f6', border: '1px solid #eee', padding: 12, borderRadius: 6, overflow: 'auto' }}>
{`# ===== 规则示例：20 日新高 + 成交额放大 =====
# 目标：
# - 当最近一个交易日收盘价创 20 日新高，且成交额较前一日放大（例如 > 1.3 倍）时触发
#
# 你可以直接使用以下“占位符/注入变量”：
# - symbol: 当前股票代码，例如 "sh600519"
# - ak/pd/np/datetime/time: 已注入可直接用
# - triggered/message: 你必须给它们赋值作为输出
#
# 注意：
# - 调试时可 print(...)，测试面板会显示输出
# - 规则脚本最终只看 triggered/message，不需要 return

# 1) 先给输出设默认值（非常推荐）
triggered = False
message = "未触发：条件不满足"

# 2) 拉取最近一段日线数据（后复权）
#    akshare 的 symbol 参数在不同接口可能不同，这里以常用接口为例
df = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")

# 3) 做健壮性检查：避免数据为空或列缺失导致脚本报错
if df is None or df.empty:
    message = "未触发：无行情数据"
else:
    # 尝试标准化列名（不同数据源列名可能不同）
    # 常见列：收盘 / 成交额
    close_col_candidates = ["收盘", "close", "收盘价"]
    amount_col_candidates = ["成交额", "amount", "成交额(元)"]

    def pick_col(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    close_col = pick_col(close_col_candidates)
    amount_col = pick_col(amount_col_candidates)

    if close_col is None or amount_col is None:
        message = f"未触发：缺少必要列 close={close_col} amount={amount_col}"
    else:
        # 4) 只取最近 25 行，足够计算 20 日新高 + 最近两日对比
        df2 = df.tail(25).copy()
        df2[close_col] = pd.to_numeric(df2[close_col], errors="coerce")
        df2[amount_col] = pd.to_numeric(df2[amount_col], errors="coerce")
        df2 = df2.dropna(subset=[close_col, amount_col])

        if len(df2) < 21:
            message = "未触发：数据不足（至少需要 21 个交易日）"
        else:
            last_close = float(df2[close_col].iloc[-1])
            high_20d = float(df2[close_col].iloc[-20:].max())

            last_amount = float(df2[amount_col].iloc[-1])
            prev_amount = float(df2[amount_col].iloc[-2])

            amount_ratio = (last_amount / prev_amount) if prev_amount > 0 else 0.0
            is_new_high = last_close >= high_20d
            is_amount_spike = amount_ratio >= 1.3

            print(f"[debug] last_close={last_close}, high_20d={high_20d}, amount_ratio={amount_ratio:.2f}")

            if is_new_high and is_amount_spike:
                triggered = True
                message = f"触发：20日新高且成交额放大（{amount_ratio:.2f}x）"
            else:
                message = f"未触发：新高={is_new_high} 成交额放大={is_amount_spike}（{amount_ratio:.2f}x）"
`}
      </pre>
    </div>
  );

  const columns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { 
        title: 'Name', 
        dataIndex: 'name', 
        width: 200,
        render: (text: string, record: RuleScript) => (
            <span>
                {record.is_pinned && <PushpinFilled style={{color: '#1890ff', marginRight: 5}} />}
                {text}
            </span>
        )
    },
    { title: 'Description', dataIndex: 'description' },
    {
      title: 'Action',
      width: 250,
      render: (_: unknown, record: RuleScript) => (
        <Space>
          <Button 
              type="text" 
              icon={record.is_pinned ? <PushpinFilled style={{color: '#1890ff'}} /> : <PushpinOutlined />} 
              onClick={() => togglePin(record)} 
          />
          <Button icon={<PlayCircleOutlined />} onClick={() => openTestModal(record)}>Test</Button>
          <Button icon={<EditOutlined />} onClick={() => handleEdit(record)} />
          <Button danger icon={<DeleteOutlined />} onClick={() => handleDelete(record.id)} />
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <h2 style={{ margin: 0 }}>硬规则脚本库 (Hard Rules)</h2>
          <Tooltip title={ruleHelp} placement="right" overlayStyle={{ maxWidth: 820 }}>
            <InfoCircleOutlined style={{ color: '#1677ff' }} />
          </Tooltip>
        </div>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
          New Rule
        </Button>
      </div>
      
      <div style={{ marginBottom: 16, background: '#f5f5f5', padding: 12, borderRadius: 4 }}>
        <InfoCircleOutlined style={{ marginRight: 8 }} />
        规则脚本是 Python 代码。系统会注入 <code>ak</code>, <code>pd</code>, <code>np</code>, <code>datetime</code>, <code>time</code>, <code>symbol</code> 等变量。
        <br/>
        脚本必须设置 <code>triggered</code> (bool) 和 <code>message</code> (str) 变量来告知系统结果。更多说明请 hover 标题旁的图标。
      </div>

      <Table 
        columns={columns} 
        dataSource={rules} 
        rowKey="id" 
        loading={loading} 
        pagination={{ pageSize: 10 }}
      />

      {/* Edit Modal */}
      <Modal
        title={editingRule ? "Edit Rule" : "New Rule"}
        open={modalVisible}
        onCancel={() => setModalVisible(false)}
        onOk={() => form.submit()}
        width={800}
      >
        <Form form={form} layout="vertical" onFinish={handleSave}>
          <Form.Item name="name" label="Name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item
            name="code"
            label={
              <span>
                Python Code&nbsp;
                <Tooltip title={ruleHelp} placement="topLeft" overlayStyle={{ maxWidth: 820 }}>
                  <InfoCircleOutlined style={{ color: '#1677ff' }} />
                </Tooltip>
              </span>
            }
            rules={[{ required: true }]}
          >
            <Input.TextArea rows={15} style={{ fontFamily: 'monospace' }} />
          </Form.Item>
        </Form>
      </Modal>

      {/* Test Modal */}
      <Modal
        title={`Test Rule: ${editingRule?.name}`}
        open={testModalVisible}
        onCancel={() => setTestModalVisible(false)}
        footer={null}
        width={700}
      >
        <Space style={{ marginBottom: 16 }}>
          <Input 
            addonBefore="Symbol" 
            value={testSymbol} 
            onChange={e => setTestSymbol(e.target.value)} 
            placeholder="e.g. sh600519" 
          />
          <Button type="primary" loading={testing} onClick={runTest} icon={<PlayCircleOutlined />}>
            Run Test
          </Button>
        </Space>

        {testResult && (
          <Card title="Result" size="small" style={{ marginTop: 16 }}>
             <p><strong>Triggered:</strong> {testResult.triggered ? <Tag color="red">YES</Tag> : <Tag color="green">NO</Tag>}</p>
             <p><strong>Message:</strong> {testResult.message}</p>
             <div style={{ marginTop: 8 }}>
               <strong>Log Output:</strong>
               <pre style={{ background: '#f0f0f0', padding: 8, maxHeight: 300, overflow: 'auto' }}>
                 {testResult.log || "(No output)"}
               </pre>
             </div>
          </Card>
        )}
      </Modal>
    </div>
  );
};

export default RuleLibrary;
