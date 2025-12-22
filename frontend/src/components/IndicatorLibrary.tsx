import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Table, message, Space, Collapse } from 'antd';
import { DeleteOutlined, EditOutlined, CodeOutlined, PlayCircleOutlined } from '@ant-design/icons';
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
  akshare_api: string;
  params_json: string;
  post_process_json?: string;
  python_code?: string;
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
      setItems(res.data);
    } catch {
      message.error('加载指标库失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const handleSubmit = async (values: IndicatorCreateFormValues) => {
    try {
      if (editingId) {
        await updateIndicator(editingId, values);
        message.success('指标已更新');
      } else {
        await createIndicator(values);
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
      akshare_api: record.akshare_api,
      params_json: record.params_json,
      post_process_json: record.post_process_json,
      python_code: record.python_code,
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

  const columns: ColumnsType<IndicatorDefinition> = [
    { title: '名称', dataIndex: 'name', key: 'name', width: 150 },
    { title: 'AkShare 接口名', dataIndex: 'akshare_api', key: 'akshare_api', width: 200 },
    { title: '参数 JSON', dataIndex: 'params_json', key: 'params_json', ellipsis: true },
    { 
      title: 'Python 代码', 
      dataIndex: 'python_code', 
      key: 'python_code',
      width: 100,
      render: (code: string) => code ? <CodeOutlined style={{ color: '#1890ff' }} /> : '-'
    },
    {
      title: '操作',
      key: 'action',
      width: 160,
      render: (_: unknown, record: IndicatorDefinition) => (
        <Space>
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
      <Table rowKey="id" loading={loading} dataSource={items} columns={columns} />

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
          initialValues={{ params_json: '{}', post_process_json: '', python_code: '' }}
        >
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：ATR指标、MACD指标" />
          </Form.Item>
          <Form.Item
            name="akshare_api"
            label="AkShare 接口名"
            rules={[{ required: true, message: '请输入 AkShare 接口名' }]}
          >
            <Input placeholder="例如：stock_zh_a_hist" />
          </Form.Item>
          <Form.Item name="params_json" label="参数 JSON" rules={[{ required: true, message: '请输入参数 JSON' }]}>
            <Input.TextArea rows={2} placeholder='{"symbol":"{symbol}","period":"daily","start_date":"{today-50}","end_date":"{today}"}' />
          </Form.Item>

          <Form.Item 
            name="python_code" 
            label="Python 处理脚本 (df 为 Pandas DataFrame)"
            help="可用变量: df (DataFrame), pd (pandas), np (numpy)。直接修改 df 即可。"
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
