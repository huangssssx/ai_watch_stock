import React, { useState, useEffect } from 'react';
import { Table, Button, Modal, Form, Input, message, Space, InputNumber, Tooltip } from 'antd';
import type { AIConfig, AIConfigTestRequest, AIConfigTestResponse } from '../types';
import { getAIConfigs, createAIConfig, deleteAIConfig, testAIConfig, updateAIConfig } from '../api';
import { DeleteOutlined, EditOutlined, InfoCircleOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

type AIConfigFormValues = {
  name: string;
  provider: string;
  base_url: string;
  api_key: string;
  model_name: string;
  max_tokens?: number;
  is_active?: boolean;
};

type AIConfigTestFormValues = AIConfigTestRequest;

const AISettings: React.FC = () => {
  const [configs, setConfigs] = useState<AIConfig[]>([]);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [form] = Form.useForm();
  const [editingConfig, setEditingConfig] = useState<AIConfig | null>(null);
  
  const [isTestModalVisible, setIsTestModalVisible] = useState(false);
  const [testForm] = Form.useForm<AIConfigTestFormValues>();
  const [testing, setTesting] = useState(false);
  const [testingConfig, setTestingConfig] = useState<AIConfig | null>(null);
  const [testResult, setTestResult] = useState<AIConfigTestResponse | null>(null);

  const fetchConfigs = async () => {
    const res = await getAIConfigs();
    setConfigs(res.data);
  };

  useEffect(() => {
    const id = window.setTimeout(() => {
      void fetchConfigs();
    }, 0);
    return () => window.clearTimeout(id);
  }, []);

  const handleOpenModal = (config?: AIConfig) => {
    form.resetFields();
    if (config) {
      setEditingConfig(config);
      form.setFieldsValue(config);
    } else {
      setEditingConfig(null);
      form.setFieldsValue({
        provider: 'openai',
        max_tokens: 100000
      });
    }
    setIsModalVisible(true);
  };

  const handleSubmit = async (values: AIConfigFormValues) => {
    try {
      if (editingConfig) {
        await updateAIConfig(editingConfig.id, values);
        message.success('配置已更新');
      } else {
        await createAIConfig(values);
        message.success('配置已添加');
      }
      setIsModalVisible(false);
      form.resetFields();
      setEditingConfig(null);
      fetchConfigs();
    } catch {
      message.error(editingConfig ? '更新失败' : '添加失败');
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteAIConfig(id);
      message.success('已删除');
      fetchConfigs();
    } catch {
      message.error('删除失败');
    }
  };

  const openTestModal = (config: AIConfig) => {
    setTestingConfig(config);
    setTestResult(null);
    setIsTestModalVisible(true);
    testForm.setFieldsValue({
      prompt_template: '',
      data_context: 'hello',
    });
  };

  const handleTest = async (values: AIConfigTestFormValues) => {
    if (!testingConfig) return;
    setTesting(true);
    try {
      const res = await testAIConfig(testingConfig.id, values);
      setTestResult(res.data);
      if (res.data.ok) {
        message.success('测试成功');
      } else {
        message.error('测试失败');
      }
    } catch {
      message.error('测试请求失败');
    } finally {
      setTesting(false);
    }
  };

  const columns: ColumnsType<AIConfig> = [
    { title: '名称', dataIndex: 'name', key: 'name' },
    { title: '厂商', dataIndex: 'provider', key: 'provider' },
    { title: '模型', dataIndex: 'model_name', key: 'model_name' },
    { title: '上下文限制', dataIndex: 'max_tokens', key: 'max_tokens', render: (val) => val ? `${val.toLocaleString()} chars` : '-' },
    { 
      title: '操作', 
      key: 'action',
      render: (_: unknown, record: AIConfig) => (
        <Space>
          <Button onClick={() => openTestModal(record)}>测试</Button>
          <Button icon={<EditOutlined />} onClick={() => handleOpenModal(record)} />
          <Button danger icon={<DeleteOutlined />} onClick={() => handleDelete(record.id)} />
        </Space>
      )
    }
  ];

  return (
    <div>
      <Button type="primary" onClick={() => handleOpenModal()} style={{ marginBottom: 16 }}>添加 AI 配置</Button>
      <Table dataSource={configs} columns={columns} rowKey="id" />

      <Modal 
        title={editingConfig ? "编辑 AI 配置" : "添加 AI 配置"} 
        open={isModalVisible} 
        onOk={form.submit} 
        onCancel={() => setIsModalVisible(false)}
      >
        <Form form={form} onFinish={handleSubmit} layout="vertical">
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="provider" label="厂商标识" initialValue="openai">
            <Input />
          </Form.Item>
          <Form.Item name="base_url" label="Base URL" rules={[{ required: true, message: '请输入 Base URL' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="api_key" label="API Key" rules={[{ required: true, message: '请输入 API Key' }]}>
            <Input.Password />
          </Form.Item>
          <Form.Item name="model_name" label="模型名称" rules={[{ required: true, message: '请输入模型名称' }]}>
            <Input />
          </Form.Item>
          <Form.Item 
            name="max_tokens" 
            label={
              <span>
                最大上下文限制 (字符数) 
                <Tooltip title="通常 1 Token ≈ 3~4 字符。DeepSeek-V3 160K Token 约等于 500,000 字符。默认 100,000 以平衡成本与性能。">
                  <InfoCircleOutlined style={{ marginLeft: 4 }} />
                </Tooltip>
              </span>
            }
            initialValue={100000}
          >
            <InputNumber style={{ width: '100%' }} min={1000} step={1000} />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={`测试 AI 配置${testingConfig ? `：${testingConfig.name}` : ''}`}
        open={isTestModalVisible}
        onOk={testForm.submit}
        confirmLoading={testing}
        onCancel={() => {
          setIsTestModalVisible(false);
          setTestingConfig(null);
          setTestResult(null);
          testForm.resetFields();
        }}
        okText="测试"
      >
        <Form form={testForm} onFinish={handleTest} layout="vertical">
          <Form.Item name="prompt_template" label="系统提示词（可选）">
            <Input.TextArea autoSize={{ minRows: 3, maxRows: 8 }} />
          </Form.Item>
          <Form.Item name="data_context" label="发送内容">
            <Input.TextArea autoSize={{ minRows: 2, maxRows: 8 }} />
          </Form.Item>
        </Form>

        {testResult && (
          <div style={{ marginTop: 12 }}>
            <div style={{ marginBottom: 8 }}>结果：{testResult.ok ? '成功' : '失败'}</div>
            <div style={{ marginBottom: 8 }}>
              <div style={{ marginBottom: 4 }}>状态信息</div>
              <Input.TextArea value={testResult.parsed?.message ?? ''} readOnly autoSize={{ minRows: 2, maxRows: 6 }} />
            </div>
            <div>
              <div style={{ marginBottom: 4 }}>AI 回复</div>
              <Input.TextArea value={testResult.raw} readOnly autoSize={{ minRows: 3, maxRows: 10 }} />
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default AISettings;
