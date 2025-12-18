import React, { useState, useEffect } from 'react';
import { Table, Button, Modal, Form, Input, InputNumber, Select, message, Tag } from 'antd';
import type { Stock, AIConfig } from '../types';
import { getStocks, updateStock, deleteStock, createStock, getAIConfigs } from '../api';
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
