import React, { useEffect, useState } from 'react';
import { Card, Table, Button, Space, Modal, Form, Input, message, Divider, Typography, Drawer } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import { 
  getIndicatorConfigs, 
  createIndicatorConfig, 
  updateIndicatorConfig, 
  deleteIndicatorConfig,
  proxyAkshareGet
} from '../api';

const { TextArea } = Input;
const { Title } = Typography;

const IndicatorPage = () => {
  const [configs, setConfigs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingConfig, setEditingConfig] = useState(null);
  const [form] = Form.useForm();
  
  const [result, setResult] = useState(null);
  const [resultLoading, setResultLoading] = useState(false);
  const [resultVisible, setResultVisible] = useState(false); // Whether to show result area
  const [currentRunningName, setCurrentRunningName] = useState('');

  useEffect(() => {
    loadConfigs();
  }, []);

  const loadConfigs = async () => {
    setLoading(true);
    try {
      const res = await getIndicatorConfigs();
      setConfigs(res.data || []);
    } catch (err) {
      message.error('加载配置失败');
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = () => {
    setEditingConfig(null);
    form.resetFields();
    form.setFieldsValue({
        name: '新指标',
        api_name: 'stock_zh_a_hist',
        params: '{\n  "symbol": "600498",\n  "period": "daily",\n  "start_date": "20250101",\n  "end_date": "20250201",\n  "adjust": "qfq"\n}'
    });
    setModalVisible(true);
  };

  const handleEdit = (record) => {
    setEditingConfig(record);
    form.setFieldsValue({
      name: record.name,
      api_name: record.api_name,
      description: record.description,
      params: JSON.stringify(record.params, null, 2)
    });
    setModalVisible(true);
  };

  const handleDelete = async (id) => {
    try {
      await deleteIndicatorConfig(id);
      message.success('删除成功');
      loadConfigs();
    } catch (err) {
      message.error('删除失败');
    }
  };

  const handleModalOk = async () => {
    try {
      const values = await form.validateFields();
      let paramsObj = {};
      try {
        paramsObj = JSON.parse(values.params);
      } catch (e) {
        message.error('参数格式错误，必须是有效的 JSON');
        return;
      }

      const payload = {
        name: values.name,
        api_name: values.api_name,
        description: values.description,
        params: paramsObj
      };

      if (editingConfig) {
        await updateIndicatorConfig(editingConfig.id, payload);
        message.success('更新成功');
      } else {
        await createIndicatorConfig(payload);
        message.success('创建成功');
      }
      setModalVisible(false);
      loadConfigs();
    } catch (err) {
      // Form validation error or API error
      if (err.message) message.error('保存失败: ' + err.message);
    }
  };

  const handleRun = async (record) => {
    if (!record.api_name) {
      message.warning('该配置缺少 API Name');
      return;
    }
    
    setResultLoading(true);
    setResultVisible(true);
    setResult(null);
    setCurrentRunningName(record.name);

    try {
      // record.params is already an object from API response
      const res = await proxyAkshareGet(record.api_name, record.params || {});
      if (res.data && res.data.code === 200) {
        setResult(res.data.data);
        message.success('调用成功');
      } else {
        message.error(res.data?.message || '调用失败');
        setResult(res.data);
      }
    } catch (err) {
      message.error('调用异常: ' + err.message);
    } finally {
      setResultLoading(false);
    }
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      width: 80,
    },
    {
      title: '名称',
      dataIndex: 'name',
      width: 200,
    },
    {
      title: '接口名 (API)',
      dataIndex: 'api_name',
      width: 200,
      render: (text) => <code style={{ background: '#f0f0f0', padding: '2px 4px', borderRadius: 4 }}>{text}</code>
    },
    {
      title: '描述',
      dataIndex: 'description',
      ellipsis: true,
    },
    {
      title: '参数预览',
      dataIndex: 'params',
      ellipsis: true,
      render: (params) => JSON.stringify(params)
    },
    {
      title: '操作',
      key: 'action',
      width: 250,
      render: (_, record) => (
        <Space>
          <Button 
            type="primary" 
            ghost 
            icon={<PlayCircleOutlined />} 
            onClick={() => handleRun(record)}
          >
            运行
          </Button>
          <Button 
            icon={<EditOutlined />} 
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Button 
            danger 
            icon={<DeleteOutlined />} 
            onClick={() => Modal.confirm({
              title: '确认删除',
              content: `确定要删除指标 "${record.name}" 吗？`,
              onOk: () => handleDelete(record.id)
            })}
          >
            删除
          </Button>
        </Space>
      ),
    },
  ];

  // Helper to render result
  const renderResult = () => {
    if (!result) return <div style={{ color: '#999', textAlign: 'center', padding: 20 }}>暂无数据</div>;
    
    if (Array.isArray(result) && result.length > 0) {
      const first = result[0];
      const resultColumns = Object.keys(first).map(key => ({
        title: key,
        dataIndex: key,
        key: key,
        ellipsis: true,
        width: 150, // default width
        render: (text) => {
            if (typeof text === 'object' && text !== null) return JSON.stringify(text);
            return text;
        }
      }));
      return <Table dataSource={result} columns={resultColumns} scroll={{ x: 'max-content', y: 500 }} pagination={{ pageSize: 50 }} rowKey={(r, i) => i} size="small" />;
    }
    
    return (
      <pre style={{ maxHeight: '500px', overflow: 'auto', background: '#f5f5f5', padding: 16, borderRadius: 4 }}>
        {JSON.stringify(result, null, 2)}
      </pre>
    );
  };

  return (
    <>
      <Card
        title="数据指标管理"
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={loadConfigs}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
              新建指标
            </Button>
          </Space>
        }
      >
        <Table
          loading={loading}
          columns={columns}
          dataSource={configs}
          rowKey="id"
          pagination={{ pageSize: 10 }}
        />
      </Card>

      {resultVisible && (
        <Card 
          style={{ marginTop: 24 }} 
          title={`查询结果: ${currentRunningName}`}
          extra={<Button onClick={() => setResultVisible(false)}>关闭结果</Button>}
          loading={resultLoading}
        >
          {renderResult()}
        </Card>
      )}

      <Modal
        title={editingConfig ? "编辑指标" : "新建指标"}
        open={modalVisible}
        onOk={handleModalOk}
        onCancel={() => setModalVisible(false)}
        width={600}
        maskClosable={false}
      >
        <Form
          form={form}
          layout="vertical"
        >
          <Form.Item name="name" label="指标名称" rules={[{ required: true, message: '请输入名称' }]}>
            <Input placeholder="例如：个股日线" />
          </Form.Item>
          <Form.Item name="api_name" label="AkShare 接口名" rules={[{ required: true, message: '请输入接口名' }]}>
            <Input placeholder="例如：stock_zh_a_hist" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea rows={2} placeholder="描述该指标的用途" />
          </Form.Item>
          <Form.Item 
            name="params" 
            label="请求参数 (JSON 格式)" 
            rules={[{ required: true, message: '请输入参数JSON' }]}
            help="请填写标准的 JSON 对象格式"
          >
            <TextArea 
              rows={8} 
              style={{ fontFamily: 'monospace' }} 
              placeholder='{ "symbol": "600498", "period": "daily" }' 
            />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
};

export default IndicatorPage;
