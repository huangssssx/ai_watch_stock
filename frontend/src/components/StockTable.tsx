import React, { useState, useEffect } from 'react';
import { Table, Button, Modal, Form, Input, InputNumber, Select, message, Tag } from 'antd';
import type { Stock, AIConfig, StockTestRunResponse } from '../types';
import { getStocks, updateStock, deleteStock, createStock, getAIConfigs, testRunStock } from '../api';
import { SettingOutlined, DeleteOutlined, PlayCircleOutlined, PauseCircleOutlined } from '@ant-design/icons';
import StockConfigModal from './StockConfigModal.tsx';
import type { ColumnsType } from 'antd/es/table';

type StockCreateFormValues = {
  symbol: string;
  name?: string;
  interval_seconds: number;
  ai_provider_id?: number;
};

const StockTable: React.FC = () => {
  const [stocks, setStocks] = useState<Stock[]>([]);
  const [loading, setLoading] = useState(false);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [currentStock, setCurrentStock] = useState<Stock | null>(null);
  const [testModalVisible, setTestModalVisible] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testStock, setTestStock] = useState<Stock | null>(null);
  const [testResult, setTestResult] = useState<StockTestRunResponse | null>(null);
  
  const [form] = Form.useForm();

  const fetchData = async () => {
    setLoading(true);
    try {
      const [stocksRes, aiRes] = await Promise.all([getStocks(), getAIConfigs()]);
      setStocks(stocksRes.data);
      setAiConfigs(aiRes.data);
    } catch {
      message.error('加载数据失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const id = window.setTimeout(() => {
      void fetchData();
    }, 0);
    return () => window.clearTimeout(id);
  }, []);

  const handleToggleMonitor = async (stock: Stock) => {
    try {
      await updateStock(stock.id, { is_monitoring: !stock.is_monitoring });
      message.success(`${stock.symbol} ${!stock.is_monitoring ? '已开始监视' : '已停止监视'}`);
      fetchData();
    } catch {
      message.error('操作失败');
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteStock(id);
      message.success('股票已删除');
      fetchData();
    } catch {
      message.error('删除失败');
    }
  };

  const handleAdd = async (values: StockCreateFormValues) => {
    try {
      await createStock(values);
      message.success('股票已添加');
      setIsModalVisible(false);
      form.resetFields();
      fetchData();
    } catch {
      message.error('添加失败');
    }
  };

  const handleTestRun = async (stock: Stock) => {
    setTestStock(stock);
    setTestResult(null);
    setTestModalVisible(true);
    setTesting(true);
    try {
      const res = await testRunStock(stock.id);
      setTestResult(res.data);
      message.success('测试完成');
    } catch {
      message.error('测试失败（请确认已配置 AI 与指标）');
    } finally {
      setTesting(false);
    }
  };

  const columns: ColumnsType<Stock> = [
    { title: '代码', dataIndex: 'symbol', key: 'symbol' },
    { title: '名称', dataIndex: 'name', key: 'name' },
    { 
      title: '状态', 
      dataIndex: 'is_monitoring', 
      key: 'is_monitoring',
      render: (is_monitoring: boolean) => (
        <Tag color={is_monitoring ? 'green' : 'red'}>
          {is_monitoring ? '监视中' : '已停止'}
        </Tag>
      )
    },
    { 
      title: '间隔（秒）', 
      dataIndex: 'interval_seconds', 
      key: 'interval_seconds' 
    },
    {
      title: '操作',
      key: 'action',
      render: (_: unknown, record: Stock) => (
        <div style={{ display: 'flex', gap: '8px' }}>
          <Button 
            type={record.is_monitoring ? 'default' : 'primary'}
            icon={record.is_monitoring ? <PauseCircleOutlined /> : <PlayCircleOutlined />}
            onClick={() => handleToggleMonitor(record)}
          >
            {record.is_monitoring ? '停止' : '开始'}
          </Button>
          <Button onClick={() => handleTestRun(record)} loading={testing && testStock?.id === record.id}>
            测试
          </Button>
          <Button icon={<SettingOutlined />} onClick={() => { setCurrentStock(record); setConfigModalVisible(true); }} />
          <Button danger icon={<DeleteOutlined />} onClick={() => handleDelete(record.id)} />
        </div>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" onClick={() => setIsModalVisible(true)}>添加股票</Button>
      </div>
      <Table dataSource={stocks} columns={columns} rowKey="id" loading={loading} />

      <Modal title="添加股票" open={isModalVisible} onOk={form.submit} onCancel={() => setIsModalVisible(false)}>
        <Form form={form} onFinish={handleAdd} layout="vertical">
          <Form.Item name="symbol" label="股票代码" rules={[{ required: true, message: '请输入股票代码' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="name" label="股票名称">
            <Input />
          </Form.Item>
          <Form.Item name="interval_seconds" label="监测间隔（秒）" initialValue={300}>
            <InputNumber min={10} max={3600} />
          </Form.Item>
          <Form.Item name="ai_provider_id" label="AI 配置">
            <Select>
              {aiConfigs.map(c => <Select.Option key={c.id} value={c.id}>{c.name}</Select.Option>)}
            </Select>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={`测试：${testStock ? `${testStock.symbol} ${testStock.name || ''}` : ''}`}
        open={testModalVisible}
        onCancel={() => {
          setTestModalVisible(false);
          setTestStock(null);
          setTestResult(null);
          setTesting(false);
        }}
        footer={null}
        width={900}
      >
        {testResult ? (
          <div>
            <div style={{ marginBottom: 8 }}>
              模型：{testResult.model_name ?? '-'} / Base URL：{testResult.base_url ?? '-'}
            </div>
            {testResult.data_truncated && (
              <div style={{ marginBottom: 8 }}>
                提示：数据过长已截断（最多 {testResult.data_char_limit ?? '-'} 字符）
              </div>
            )}
            <div style={{ marginBottom: 12 }}>
              <div style={{ marginBottom: 4 }}>System Prompt</div>
              <Input.TextArea value={testResult.system_prompt} readOnly autoSize={{ minRows: 3, maxRows: 10 }} />
            </div>
            <div style={{ marginBottom: 12 }}>
              <div style={{ marginBottom: 4 }}>User Prompt（包含指标数据）</div>
              <Input.TextArea value={testResult.user_prompt} readOnly autoSize={{ minRows: 6, maxRows: 16 }} />
            </div>
            <div>
              <div style={{ marginBottom: 4 }}>AI 原始返回</div>
              <Input.TextArea value={testResult.ai_reply} readOnly autoSize={{ minRows: 6, maxRows: 16 }} />
            </div>
          </div>
        ) : (
          <div>{testing ? '测试中，请稍候…' : '暂无结果'}</div>
        )}
      </Modal>

      {currentStock && (
        <StockConfigModal 
          visible={configModalVisible} 
          stock={currentStock} 
          onClose={() => { setConfigModalVisible(false); fetchData(); }} 
        />
      )}
    </div>
  );
};

export default StockTable;
