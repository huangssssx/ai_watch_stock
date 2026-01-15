import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Table, message, Space, Collapse, Radio, Tag, Tooltip, Typography } from 'antd';
import { DeleteOutlined, EditOutlined, CodeOutlined, PlayCircleOutlined, PushpinOutlined, PushpinFilled } from '@ant-design/icons';
import type { IndicatorDefinition, IndicatorTestResponse } from '../types';
import { createIndicator, deleteIndicator, getIndicators, testIndicator, updateIndicator } from '../api';
import type { ColumnsType } from 'antd/es/table';
import Editor from 'react-simple-code-editor';
import Prism from 'prismjs';
const { highlight, languages } = Prism;
import 'prismjs/components/prism-clike';
import 'prismjs/components/prism-python';
import 'prismjs/themes/prism.css';

type IndicatorCreateFormValues = {
  name: string;
  mode?: 'akshare' | 'pure_script';
  akshare_api?: string | null;
  params_json?: string | null;
  post_process_json?: string | null;
  python_code?: string | null;
};

const IndicatorLibrary: React.FC = () => {
  const [items, setItems] = useState<IndicatorDefinition[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [form] = Form.useForm();
  const [testOpen, setTestOpen] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testIndicatorItem, setTestIndicatorItem] = useState<IndicatorDefinition | null>(null);
  const [testResult, setTestResult] = useState<IndicatorTestResponse | null>(null);
  const [testForm] = Form.useForm();
  const mode = Form.useWatch('mode', form) as IndicatorCreateFormValues['mode'];

  const refresh = async () => {
    setLoading(true);
    try {
      const res = await getIndicators();
      const data = res.data;
      data.sort((a, b) => {
          if (!!a.is_pinned === !!b.is_pinned) return 0;
          return a.is_pinned ? -1 : 1;
      });
      setItems(data);
    } catch {
      message.error('加载指标库失败');
    } finally {
      setLoading(false);
    }
  };

  const togglePin = async (record: IndicatorDefinition) => {
      try {
          await updateIndicator(record.id, { is_pinned: !record.is_pinned });
          refresh();
      } catch {
          message.error('Failed to update pin status');
      }
  };

  useEffect(() => {
    refresh();
  }, []);

  const handleSubmit = async (values: IndicatorCreateFormValues) => {
    try {
      const payload: Omit<IndicatorCreateFormValues, 'mode'> = {
        name: values.name,
        akshare_api: values.mode === 'pure_script' ? '' : values.akshare_api || '',
        params_json: values.mode === 'pure_script' ? '' : values.params_json || '{}',
        post_process_json: values.post_process_json || '',
        python_code: values.python_code || '',
      };
      if (editingId) {
        await updateIndicator(editingId, payload);
        message.success('指标已更新');
      } else {
        await createIndicator(payload);
        message.success('指标已添加');
      }
      setOpen(false);
      setEditingId(null);
      form.resetFields();
      refresh();
    } catch {
      message.error(editingId ? '更新指标失败' : '添加指标失败');
    }
  };

  const handleEdit = (record: IndicatorDefinition) => {
    setEditingId(record.id);
    const recordMode: IndicatorCreateFormValues['mode'] = record.akshare_api ? 'akshare' : 'pure_script';
    form.setFieldsValue({
      name: record.name,
      mode: recordMode,
      akshare_api: record.akshare_api || '',
      params_json: record.params_json || '{}',
      post_process_json: record.post_process_json || '',
      python_code: record.python_code || '',
    });
    setOpen(true);
  };

  const handleCancel = () => {
    setOpen(false);
    setEditingId(null);
    form.resetFields();
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteIndicator(id);
      message.success('指标已删除');
      refresh();
    } catch {
      message.error('删除指标失败');
    }
  };

  const handleOpenTest = (record: IndicatorDefinition) => {
    setTestIndicatorItem(record);
    setTestResult(null);
    testForm.resetFields();
    setTestOpen(true);
  };

  const handleCloseTest = () => {
    setTestOpen(false);
    setTesting(false);
    setTestIndicatorItem(null);
    setTestResult(null);
    testForm.resetFields();
  };

  const handleRunTest = async () => {
    const indicator = testIndicatorItem;
    if (!indicator) return;

    const values = await testForm.validateFields();
    setTesting(true);
    setTestResult(null);
    try {
      const res = await testIndicator(indicator.id, { symbol: values.symbol });
      setTestResult(res.data);
      if (res.data.ok) {
        message.success('测试完成');
      } else {
        message.error(res.data.error || '测试失败');
        if (res.data.error === 'Indicator not found') {
          refresh();
          message.info('指标不存在，已刷新列表');
        }
      }
    } catch {
      message.error('测试失败');
    } finally {
      setTesting(false);
    }
  };

  const testOutput = (() => {
    if (!testResult) return '';
    if (testResult.ok) {
      if (testResult.parsed !== undefined && testResult.parsed !== null) {
        return JSON.stringify(testResult.parsed, null, 2);
      }
      return testResult.raw;
    }
    return testResult.error || testResult.raw || '';
  })();

  const getRowMode = (record: IndicatorDefinition) => (record.akshare_api ? 'akshare' : 'pure_script');

  const formatJsonMaybe = (text?: string | null) => {
    const raw = String(text || '').trim();
    if (!raw) return '';
    try {
      const obj = JSON.parse(raw) as unknown;
      return JSON.stringify(obj, null, 2);
    } catch {
      return raw;
    }
  };

  const tryParseObject = (text?: string | null) => {
    const raw = String(text || '').trim();
    if (!raw) return null;
    try {
      const obj = JSON.parse(raw) as unknown;
      if (obj && typeof obj === 'object' && !Array.isArray(obj)) return obj as Record<string, unknown>;
      return null;
    } catch {
      return null;
    }
  };

  const getParamsPreview = (record: IndicatorDefinition) => {
    const obj = tryParseObject(record.params_json);
    if (!obj) return '';
    const keys = ['symbol', 'market', 'period', 'start_date', 'end_date', 'begin', 'end', 'date', 'adjust'];
    const pairs: string[] = [];
    for (const k of keys) {
      if (k in obj) {
        const v = obj[k];
        pairs.push(`${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`);
      }
      if (pairs.length >= 4) break;
    }
    return pairs.join('  ');
  };

  const getScriptStats = (code?: string | null) => {
    const raw = String(code || '').trim();
    if (!raw) return { ok: false, lines: 0, chars: 0 };
    return { ok: true, lines: raw.split(/\r?\n/).length, chars: raw.length };
  };

  const columns: ColumnsType<IndicatorDefinition> = [
    { 
        title: '名称', 
        dataIndex: 'name', 
        key: 'name', 
        width: 150,
        render: (text: string, record: IndicatorDefinition) => (
            <span>
                {record.is_pinned && <PushpinFilled style={{color: '#1890ff', marginRight: 5}} />}
                {text}
            </span>
        )
    },
    {
      title: '模式',
      key: 'mode',
      width: 110,
      render: (_: unknown, record: IndicatorDefinition) => {
        const modeValue = getRowMode(record);
        return modeValue === 'akshare' ? <Tag color="blue">AkShare</Tag> : <Tag color="purple">纯脚本</Tag>;
      },
    },
    {
      title: '来源/接口',
      key: 'source',
      width: 220,
      render: (_: unknown, record: IndicatorDefinition) => {
        if (getRowMode(record) === 'pure_script') return <Typography.Text type="secondary">Script</Typography.Text>;
        return <Typography.Text code>{record.akshare_api}</Typography.Text>;
      },
    },
    {
      title: '配置摘要',
      key: 'summary',
      ellipsis: true,
      render: (_: unknown, record: IndicatorDefinition) => {
        if (getRowMode(record) === 'pure_script') {
          const st = getScriptStats(record.python_code);
          return (
            <Space size={6} wrap>
              <Tag color={st.ok ? 'green' : 'red'}>{st.ok ? '脚本✅' : '脚本❌'}</Tag>
              {st.ok ? (
                <Typography.Text type="secondary">
                  {st.lines} 行 / {st.chars} 字符
                </Typography.Text>
              ) : null}
            </Space>
          );
        }

        const obj = tryParseObject(record.params_json);
        const ok = Boolean(obj);
        const preview = ok ? getParamsPreview(record) : '';
        return (
          <Space size={6} wrap>
            <Tag color={ok ? 'green' : 'red'}>{ok ? 'JSON✅' : 'JSON❌'}</Tag>
            {preview ? (
              <Tooltip title={preview}>
                <Typography.Text type="secondary" ellipsis style={{ maxWidth: 420 }}>
                  {preview}
                </Typography.Text>
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    },
    {
      title: '脚本',
      key: 'scripts',
      width: 140,
      render: (_: unknown, record: IndicatorDefinition) => {
        const hasPython = Boolean(String(record.python_code || '').trim());
        const hasPost = Boolean(String(record.post_process_json || '').trim());
        if (!hasPython && !hasPost) return <Typography.Text type="secondary">-</Typography.Text>;
        return (
          <Space size={6} wrap>
            {hasPython ? (
              <Tooltip title="python_code">
                <Tag icon={<CodeOutlined />} color="geekblue">
                  py
                </Tag>
              </Tooltip>
            ) : null}
            {hasPost ? (
              <Tooltip title="post_process_json">
                <Tag color="gold">post</Tag>
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 160,
      render: (_: unknown, record: IndicatorDefinition) => (
        <Space>
          <Button 
              type="text" 
              icon={record.is_pinned ? <PushpinFilled style={{color: '#1890ff'}} /> : <PushpinOutlined />} 
              onClick={() => togglePin(record)} 
          />
          <Button icon={<PlayCircleOutlined />} onClick={() => handleOpenTest(record)} />
          <Button icon={<EditOutlined />} onClick={() => handleEdit(record)} />
          <Button danger icon={<DeleteOutlined />} onClick={() => handleDelete(record.id)} />
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" onClick={() => setOpen(true)}>
          添加指标
        </Button>
      </div>
      <Table
        rowKey="id"
        loading={loading}
        dataSource={items}
        columns={columns}
        expandable={{
          rowExpandable: (record) => Boolean(record.akshare_api || record.params_json || record.python_code || record.post_process_json),
          expandedRowRender: (record) => {
            const modeValue = getRowMode(record);
            const parts: { title: string; content: string }[] = [];
            if (modeValue === 'akshare') {
              parts.push({ title: 'AkShare 接口名', content: String(record.akshare_api || '') });
              parts.push({ title: '参数 JSON', content: formatJsonMaybe(record.params_json) });
            } else {
              parts.push({ title: '纯脚本', content: String(record.python_code || '').trim() });
            }
            if (modeValue === 'akshare' && String(record.python_code || '').trim()) {
              parts.push({ title: 'Python 处理脚本', content: String(record.python_code || '').trim() });
            }
            if (String(record.post_process_json || '').trim()) {
              parts.push({ title: 'Post Process JSON', content: formatJsonMaybe(record.post_process_json) });
            }

            return (
              <div style={{ padding: 8 }}>
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  {parts.map((p) => (
                    <div key={p.title}>
                      <Typography.Text strong>{p.title}</Typography.Text>
                      <pre
                        style={{
                          marginTop: 6,
                          marginBottom: 0,
                          padding: 10,
                          border: '1px solid #f0f0f0',
                          borderRadius: 6,
                          background: '#fafafa',
                          whiteSpace: 'pre-wrap',
                          wordBreak: 'break-word',
                          maxHeight: 260,
                          overflow: 'auto',
                        }}
                      >
                        {p.content || '-'}
                      </pre>
                    </div>
                  ))}
                </Space>
              </div>
            );
          },
        }}
      />

      <Modal
        title={editingId ? '编辑指标' : '添加指标'}
        open={open}
        onOk={form.submit}
        onCancel={handleCancel}
        width={800}
      >
        <Form
          form={form}
          onFinish={handleSubmit}
          layout="vertical"
          initialValues={{ mode: 'akshare', params_json: '{}', post_process_json: '', python_code: '' }}
        >
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：ATR指标、MACD指标" />
          </Form.Item>
          <Form.Item name="mode" label="模式" rules={[{ required: true, message: '请选择模式' }]}>
            <Radio.Group
              options={[
                { label: 'AkShare + 参数(JSON)', value: 'akshare' },
                { label: '纯 Python 脚本', value: 'pure_script' },
              ]}
              optionType="button"
              buttonStyle="solid"
            />
          </Form.Item>
          <Form.Item
            name="akshare_api"
            label="AkShare 接口名"
            rules={[
              {
                validator: async (_, value) => {
                  if (mode === 'pure_script') return;
                  if (String(value || '').trim()) return;
                  throw new Error('请输入 AkShare 接口名');
                },
              },
            ]}
          >
            <Input placeholder="例如：stock_zh_a_hist" disabled={mode === 'pure_script'} />
          </Form.Item>
          <Form.Item
            name="params_json"
            label="参数 JSON"
            rules={[
              {
                validator: async (_, value) => {
                  if (mode === 'pure_script') return;
                  const raw = String(value || '').trim();
                  if (!raw) throw new Error('请输入参数 JSON');
                  try {
                    JSON.parse(raw);
                  } catch {
                    throw new Error('参数 JSON 不是合法 JSON');
                  }
                },
              },
            ]}
          >
            <Input.TextArea
              rows={2}
              disabled={mode === 'pure_script'}
              placeholder='{"symbol":"{symbol}","period":"daily","start_date":"{today-50}","end_date":"{today}"}'
            />
          </Form.Item>

          <Form.Item 
            name="python_code" 
            label={mode === 'pure_script' ? 'Python 脚本 (必须设置 df 或 result)' : 'Python 处理脚本 (df 为 Pandas DataFrame)'}
            help={mode === 'pure_script' ? "可用变量: context, ak, pd, np, requests, json, datetime, time。输出: df(DataFrame) 或 result(list/dict)。" : "可用变量: df (DataFrame), pd (pandas), np (numpy)。直接修改 df 即可。"}
            rules={[
              {
                validator: async (_, value) => {
                  if (mode !== 'pure_script') return;
                  if (String(value || '').trim()) return;
                  throw new Error('纯脚本模式下必须填写 Python 脚本');
                },
              },
            ]}
          >
            <PythonEditor />
          </Form.Item>

          <Collapse 
            ghost
            items={[{
              key: '1',
              label: '旧版配置 (Post Process JSON)',
              children: (
                <Form.Item name="post_process_json" label="后处理 JSON">
                  <Input.TextArea
                    rows={4}
                    placeholder='{"rename_columns": {"最高": "high"}, "numeric_columns": ["high"], "tail": 1}'
                  />
                </Form.Item>
              )
            }]}
          />
        </Form>
      </Modal>

      <Modal
        title={testIndicatorItem ? `测试指标：${testIndicatorItem.name}` : '测试指标'}
        open={testOpen}
        onOk={handleRunTest}
        onCancel={handleCloseTest}
        confirmLoading={testing}
        okText="测试"
        cancelText="关闭"
        width={900}
      >
        <Form form={testForm} layout="vertical" initialValues={{ symbol: '' }}>
          <Form.Item name="symbol" label="股票代码" rules={[{ required: true, message: '请输入股票代码' }]}>
            <Input placeholder="例如：600000 / 000001" />
          </Form.Item>
          <Form.Item label="输出">
            <Input.TextArea value={testOutput} readOnly rows={14} placeholder="点击确定开始测试" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// Wrapper component to adapt Editor to Antd Form interface (value, onChange)
const PythonEditor = ({ value, onChange }: { value?: string; onChange?: (val: string) => void }) => {
  return (
    <div style={{ border: '1px solid #d9d9d9', borderRadius: '4px', overflow: 'hidden' }}>
      <Editor
        value={value || ''}
        onValueChange={code => onChange?.(code)}
        highlight={code => highlight(code, languages.python, 'python')}
        padding={10}
        style={{
          fontFamily: '"Fira code", "Fira Mono", monospace',
          fontSize: 14,
          backgroundColor: '#f5f5f5',
          minHeight: '150px',
        }}
        textareaClassName="focus:outline-none"
      />
    </div>
  );
};

export default IndicatorLibrary;
