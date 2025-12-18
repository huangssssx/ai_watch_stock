import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Table, message } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import type { IndicatorDefinition } from '../types';
import { createIndicator, deleteIndicator, getIndicators } from '../api';
import type { ColumnsType } from 'antd/es/table';

type IndicatorCreateFormValues = {
  name: string;
  akshare_api: string;
  params_json: string;
};

const IndicatorLibrary: React.FC = () => {
  const [items, setItems] = useState<IndicatorDefinition[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [form] = Form.useForm();

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

  const handleCreate = async (values: IndicatorCreateFormValues) => {
    try {
      await createIndicator(values);
      message.success('指标已添加');
      setOpen(false);
      form.resetFields();
      refresh();
    } catch {
      message.error('添加指标失败');
    }
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

  const columns: ColumnsType<IndicatorDefinition> = [
    { title: '名称', dataIndex: 'name', key: 'name', width: 200 },
    { title: 'AkShare 接口名', dataIndex: 'akshare_api', key: 'akshare_api', width: 240 },
    { title: '参数 JSON', dataIndex: 'params_json', key: 'params_json', ellipsis: true },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_: unknown, record: IndicatorDefinition) => (
        <Button danger icon={<DeleteOutlined />} onClick={() => handleDelete(record.id)} />
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

      <Modal title="添加指标" open={open} onOk={form.submit} onCancel={() => setOpen(false)}>
        <Form form={form} onFinish={handleCreate} layout="vertical" initialValues={{ params_json: '{}' }}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：分时、日线、资金流向" />
          </Form.Item>
          <Form.Item
            name="akshare_api"
            label="AkShare 接口名"
            rules={[{ required: true, message: '请输入 AkShare 接口名' }]}
          >
            <Input placeholder="例如：stock_zh_a_spot_em" />
          </Form.Item>
          <Form.Item name="params_json" label="参数 JSON" rules={[{ required: true, message: '请输入参数 JSON' }]}>
            <Input.TextArea rows={4} placeholder='{"symbol":"{symbol}","start_date":"{today-20}"}' />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default IndicatorLibrary;
