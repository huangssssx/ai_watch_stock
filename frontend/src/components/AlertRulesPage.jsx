import React, { useEffect, useState, useCallback } from 'react';
import { Card, Table, Button, Space, Tag, Switch, Modal, Form, Input, Select, InputNumber, message, Divider, notification } from 'antd';
import { 
  getAlertRules, 
  updateAlertRule, 
  deleteAlertRule, 
  batchCreateAlertRules, 
  batchDeleteAlertRules,
  getAlertNotifications,
  updateAlertNotification,
  clearAllAlertNotifications
} from '../api';

const typeOptions = [
  { value: '跌到', label: '跌到' },
  { value: '涨到', label: '涨到' },
];

const AlertRulesPage = () => {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [pasteText, setPasteText] = useState('');
  const [editing, setEditing] = useState(null);
  const [form] = Form.useForm();
  const [notifications, setNotifications] = useState([]);

  const loadNotifications = useCallback(async () => {
    try {
      const res = await getAlertNotifications({ include_cleared: false });
      const list = Array.isArray(res.data) ? res.data : [];
      setNotifications(list);
      const newItems = list.filter(n => !n.is_cleared && !n.last_notified_at);
      newItems.forEach(n => {
        const type = (n.level || 'WARNING').toLowerCase();
        notification[type === 'error' ? 'error' : (type === 'info' ? 'info' : 'warning')]({
          message: '规则告警',
          description: n.message,
          duration: 0
        });
        updateAlertNotification(n.id, { last_notified_at: new Date().toISOString() }).catch(() => {});
      });
    } catch {
      message.error('加载告警通知失败');
    }
  }, []);

  useEffect(() => {
    loadRules();
    loadNotifications();
    const interval = setInterval(() => {
      loadRules();
      loadNotifications();
    }, 5000);
    return () => clearInterval(interval);
  }, [loadNotifications]);

  const loadRules = async () => {
    setLoading(true);
    try {
      const res = await getAlertRules();
      setRules(res.data || []);
    } catch {
      message.error('加载规则失败');
    } finally {
      setLoading(false);
    }
  };

  

  const handleClearNotifications = async () => {
    try {
      await clearAllAlertNotifications();
      message.success('提醒列表已清空');
      loadNotifications();
    } catch {
      message.error('清空提醒列表失败');
    }
  };

  const handleToggleEnabled = async (record, enabled) => {
    try {
      await updateAlertRule(record.id, { enabled });
      message.success(enabled ? '规则已启用' : '规则已停用');
      loadRules();
    } catch {
      message.error('更新状态失败');
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteAlertRule(id);
      message.success('已删除');
      loadRules();
    } catch {
      message.error('删除失败');
    }
  };

  const handleBatchDeleteSelected = async () => {
    if (!selectedRowKeys.length) {
      message.warning('请先选择要删除的规则');
      return;
    }
    try {
      await batchDeleteAlertRules({ ids: selectedRowKeys });
      message.success('批量删除成功');
      setSelectedRowKeys([]);
      loadRules();
    } catch {
      message.error('批量删除失败');
    }
  };

  const handleDeleteAll = async () => {
    Modal.confirm({
      title: '确认删除全部规则？',
      content: '该操作不可撤销',
      okText: '删除全部',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await batchDeleteAlertRules({ all: true });
          message.success('已删除全部规则');
          setSelectedRowKeys([]);
          loadRules();
        } catch {
          message.error('删除全部失败');
        }
      }
    });
  };

  const handleBatchImport = async () => {
    if (!pasteText.trim()) {
      message.warning('请粘贴规则内容');
      return;
    }
    try {
      await batchCreateAlertRules({ paste: pasteText });
      message.success('批量导入成功');
      setPasteText('');
      loadRules();
    } catch {
      message.error('批量导入失败');
    }
  };

  const openEdit = (record) => {
    setEditing(record);
    const cond = (record.condition || '').trim();
    let type = undefined;
    let threshold = undefined;
    if (/price\s*<=\s*\d+(\.\d+)?/.test(cond)) {
      type = '跌到';
      threshold = Number(cond.match(/\d+(\.\d+)?/)[0]);
    } else if (/price\s*>=\s*\d+(\.\d+)?/.test(cond)) {
      type = '涨到';
      threshold = Number(cond.match(/\d+(\.\d+)?/)[0]);
    }
    form.setFieldsValue({
      name: record.name,
      symbol: record.symbol,
      period: String(record.period || '1'),
      type,
      threshold,
      message: record.message,
      level: record.level || 'WARNING',
      condition: record.condition || '',
    });
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      let condition = values.condition;
      if (values.type && values.threshold != null) {
        const t = Number(values.threshold);
        condition = values.type === '跌到' ? `price <= ${t}` : `price >= ${t}`;
      }
      const payload = {
        name: values.name,
        symbol: values.symbol,
        period: String(values.period || '1'),
        message: values.message,
        level: values.level,
        condition,
      };
      await updateAlertRule(editing.id, payload);
      message.success('规则已更新');
      setEditing(null);
      loadRules();
    } catch {
      message.error('保存失败');
    }
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 80 },
    { title: '名称', dataIndex: 'name', key: 'name', width: 180 },
    { title: '股票代码', dataIndex: 'symbol', key: 'symbol', width: 120 },
    { title: '周期', dataIndex: 'period', key: 'period', width: 80 },
    { title: '条件', dataIndex: 'condition', key: 'condition', ellipsis: true },
    { title: '消息', dataIndex: 'message', key: 'message', ellipsis: true },
    { 
      title: '级别', 
      dataIndex: 'level', 
      key: 'level', 
      width: 120,
      render: (level) => <Tag color={level === 'WARNING' ? 'red' : 'default'}>{level}</Tag>
    },
    { 
      title: '状态', 
      dataIndex: 'enabled', 
      key: 'enabled', 
      width: 120,
      render: (enabled, record) => (
        <Switch checked={enabled} onChange={(v) => handleToggleEnabled(record, v)} />
      )
    },
    {
      title: '操作',
      key: 'action',
      width: 180,
      render: (_, record) => (
        <Space>
          <Button type="link" onClick={() => openEdit(record)}>编辑</Button>
          <Button type="link" danger onClick={() => handleDelete(record.id)}>删除</Button>
        </Space>
      ),
    },
  ];

  return (
    <Card
      title="规则管理"
      extra={
        <Space>
          <Button danger onClick={handleDeleteAll}>删除全部</Button>
          <Button onClick={handleBatchDeleteSelected}>批量删除选中</Button>
          <Button type="primary" onClick={loadRules}>刷新</Button>
        </Space>
      }
    >
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
          <Input.TextArea
            value={pasteText}
            onChange={(e) => setPasteText(e.target.value)}
            placeholder="粘贴规则。支持 JSON 数组或每行格式：600498 涨到 10 1 提示文本"
            rows={4}
            style={{ fontFamily: 'monospace' }}
          />
          <Button type="primary" onClick={handleBatchImport}>批量导入</Button>
        </div>
      </div>

      <Table
        loading={loading}
        columns={columns}
        dataSource={rules}
        rowKey="id"
        rowSelection={{
          selectedRowKeys,
          onChange: setSelectedRowKeys,
        }}
        pagination={{ pageSize: 20 }}
      />

      <Divider />

      <Card
        title="提醒列表"
        extra={
          <Space>
            <Button danger onClick={handleClearNotifications}>清空提醒列表</Button>
            <Button onClick={loadNotifications}>刷新提醒</Button>
          </Space>
        }
        style={{ marginTop: 16 }}
      >
        <Table
          dataSource={notifications}
          rowKey="id"
          size="small"
          pagination={{ pageSize: 10 }}
          columns={[
            { title: 'ID', dataIndex: 'id', width: 80 },
            { title: '规则ID', dataIndex: 'rule_id', width: 100 },
            { title: '消息', dataIndex: 'message' },
            { 
              title: '级别', 
              dataIndex: 'level', 
              width: 120,
              render: (level) => <Tag color={level === 'ERROR' ? 'volcano' : (level === 'WARNING' ? 'red' : 'default')}>{level}</Tag>
            },
            { title: '触发时间', dataIndex: 'triggered_at', width: 200 },
          ]}
        />
      </Card>

      <Modal
        title={`编辑规则 #${editing?.id || ''}`}
        open={!!editing}
        onCancel={() => setEditing(null)}
        onOk={handleSave}
        okText="保存"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="symbol" label="股票代码" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="period" label="周期" initialValue="1">
            <Select
              options={[
                { value: '1', label: '1分钟' },
                { value: '5', label: '5分钟' },
                { value: '15', label: '15分钟' },
                { value: '30', label: '30分钟' },
                { value: '60', label: '60分钟' },
              ]}
            />
          </Form.Item>
          <Form.Item name="type" label="提示类型">
            <Select options={typeOptions} allowClear />
          </Form.Item>
          <Form.Item name="threshold" label="阈值">
            <InputNumber style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="message" label="提示文本" rules={[{ required: true }]}>
            <Input.TextArea rows={3} />
          </Form.Item>
          <Form.Item name="level" label="级别" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'INFO', label: 'INFO' },
                { value: 'WARNING', label: 'WARNING' },
                { value: 'ERROR', label: 'ERROR' },
              ]}
            />
          </Form.Item>
          <Form.Item name="condition" label="自定义条件（可选）">
            <Input placeholder="例如：price >= 10 或 price <= 8" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
};

export default AlertRulesPage;
