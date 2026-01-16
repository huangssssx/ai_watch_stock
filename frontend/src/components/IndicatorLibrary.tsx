import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Table, message, Space, Tag, Tooltip, Typography } from 'antd';
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
      const payload: IndicatorCreateFormValues = {
        name: values.name,
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
    form.setFieldsValue({
      name: record.name,
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
        width: 200,
        render: (text: string, record: IndicatorDefinition) => (
            <span>
                {record.is_pinned && <PushpinFilled style={{color: '#1890ff', marginRight: 5}} />}
                {text}
            </span>
        )
    },
    {
      title: '脚本状态',
      key: 'status',
      width: 200,
      render: (_: unknown, record: IndicatorDefinition) => {
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
      },
    },
    {
      title: '类型',
      key: 'type',
      width: 100,
      render: (_: unknown) => (
        <Tooltip title="Pure Python Script">
           <Tag icon={<CodeOutlined />} color="geekblue">Script</Tag>
        </Tooltip>
      ),
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
          rowExpandable: (record) => Boolean(record.python_code),
          expandedRowRender: (record) => {
            return (
              <div style={{ padding: 8 }}>
                <Typography.Text strong>Python 脚本</Typography.Text>
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
                  {String(record.python_code || '').trim() || '-'}
                </pre>
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
          initialValues={{ python_code: '' }}
        >
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：ATR指标、MACD指标" />
          </Form.Item>

          <Form.Item 
            name="python_code" 
            label="Python 脚本 (必须设置 df 或 result)"
            help="可用变量: context, ak, pd, np, requests, json, datetime, time。输出: df(DataFrame) 或 result(list/dict)。"
            rules={[
              { required: true, message: '请输入 Python 脚本' }
            ]}
          >
            <PythonEditor />
          </Form.Item>
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
