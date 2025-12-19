import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Table, message, Space } from 'antd';
import { DeleteOutlined, EditOutlined } from '@ant-design/icons';
import type { IndicatorDefinition } from '../types';
import { createIndicator, deleteIndicator, getIndicators, updateIndicator } from '../api';
import type { ColumnsType } from 'antd/es/table';

type IndicatorCreateFormValues = {
  name: string;
  akshare_api: string;
  params_json: string;
  post_process_json?: string;
};

const IndicatorLibrary: React.FC = () => {
  const [items, setItems] = useState<IndicatorDefinition[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
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

  const columns: ColumnsType<IndicatorDefinition> = [
    { title: '名称', dataIndex: 'name', key: 'name', width: 200 },
    { title: 'AkShare 接口名', dataIndex: 'akshare_api', key: 'akshare_api', width: 240 },
    { title: '参数 JSON', dataIndex: 'params_json', key: 'params_json', ellipsis: true },
    { title: '后处理 JSON', dataIndex: 'post_process_json', key: 'post_process_json', ellipsis: true },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (_: unknown, record: IndicatorDefinition) => (
        <Space>
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
      >
        <Form
          form={form}
          onFinish={handleSubmit}
          layout="vertical"
          initialValues={{ params_json: '{}', post_process_json: '' }}
        >
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
          <Form.Item name="post_process_json" label="后处理 JSON（可选）">
            <Input.TextArea
              rows={4}
              placeholder='{"select_columns":["日期","收盘","换手率"],"sort_by":"日期","tail":30}'
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default IndicatorLibrary;
