import { useEffect, useState } from 'react';
import { Card, Table, Button, Space, Modal, Form, Input, message, Tabs, Select, DatePicker, Transfer, Typography, Tag } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import { 
  getIndicatorConfigs, 
  createIndicatorConfig, 
  updateIndicatorConfig, 
  deleteIndicatorConfig,
  proxyAkshareGet,
  getIndicatorCollections,
  createIndicatorCollection,
  updateIndicatorCollection,
  deleteIndicatorCollection,
  runIndicatorCollection
} from '../api';

const { TextArea } = Input;
const { Option } = Select;

const IndicatorPage = () => {
  const [activeTab, setActiveTab] = useState('1');
  
  // --- Indicators State ---
  const [configs, setConfigs] = useState([]);
  const [configLoading, setConfigLoading] = useState(false);
  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [editingConfig, setEditingConfig] = useState(null);
  const [configForm] = Form.useForm();
  
  // --- Collections State ---
  const [collections, setCollections] = useState([]);
  const [collectionLoading, setCollectionLoading] = useState(false);
  const [collectionModalVisible, setCollectionModalVisible] = useState(false);
  const [editingCollection, setEditingCollection] = useState(null);
  const [collectionForm] = Form.useForm();
  const [targetKeys, setTargetKeys] = useState([]); // For Transfer component

  // --- Run State ---
  const [result, setResult] = useState(null);
  const [resultLoading, setResultLoading] = useState(false);
  const [resultVisible, setResultVisible] = useState(false);
  const [currentRunningName, setCurrentRunningName] = useState('');
  const [exportModalVisible, setExportModalVisible] = useState(false);
  const [exportText, setExportText] = useState('');
  const [copyLoading, setCopyLoading] = useState(false);
  
  // --- Collection Run Modal State ---
  const [runModalVisible, setRunModalVisible] = useState(false);
  const [runCollectionId, setRunCollectionId] = useState(null);
  const [runForm] = Form.useForm();

  useEffect(() => {
    loadConfigs();
    loadCollections();
  }, []);

  const loadConfigs = async () => {
    setConfigLoading(true);
    try {
      const res = await getIndicatorConfigs();
      setConfigs(res.data || []);
    } catch {
      message.error('加载指标失败');
    } finally {
      setConfigLoading(false);
    }
  };

  const loadCollections = async () => {
    setCollectionLoading(true);
    try {
      const res = await getIndicatorCollections();
      setCollections(res.data || []);
    } catch {
      message.error('加载集合失败');
    } finally {
      setCollectionLoading(false);
    }
  };

  // --- Config Handlers ---

  const handleCreateConfig = () => {
    setEditingConfig(null);
    configForm.resetFields();
    configForm.setFieldsValue({
        name: '新指标',
        api_name: 'stock_zh_a_hist',
        params: '{\n  "symbol": "600498",\n  "period": "daily",\n  "start_date": "20250101",\n  "end_date": "20250201",\n  "adjust": "qfq"\n}'
    });
    setConfigModalVisible(true);
  };

  const handleEditConfig = (record) => {
    setEditingConfig(record);
    configForm.setFieldsValue({
      name: record.name,
      api_name: record.api_name,
      description: record.description,
      params: JSON.stringify(record.params, null, 2)
    });
    setConfigModalVisible(true);
  };

  const handleDeleteConfig = async (id) => {
    try {
      await deleteIndicatorConfig(id);
      message.success('删除成功');
      loadConfigs();
    } catch {
      message.error('删除失败');
    }
  };

  const handleConfigModalOk = async () => {
    try {
      const values = await configForm.validateFields();
      let paramsObj = {};
      try {
        paramsObj = JSON.parse(values.params);
      } catch {
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
      setConfigModalVisible(false);
      loadConfigs();
    } catch (err) {
      if (err.message) message.error('保存失败: ' + err.message);
    }
  };

  const handleRunConfig = async (record) => {
    if (!record.api_name) {
      message.warning('该配置缺少 API Name');
      return;
    }
    
    setResultLoading(true);
    setResultVisible(true);
    setResult(null);
    setCurrentRunningName(record.name);

    try {
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

  // --- Collection Handlers ---

  const handleCreateCollection = () => {
    setEditingCollection(null);
    collectionForm.resetFields();
    setTargetKeys([]);
    setCollectionModalVisible(true);
  };

  const handleEditCollection = (record) => {
    setEditingCollection(record);
    collectionForm.setFieldsValue({
      name: record.name,
      description: record.description,
    });
    setTargetKeys(record.indicator_ids || []);
    setCollectionModalVisible(true);
  };

  const handleDeleteCollection = async (id) => {
    try {
      await deleteIndicatorCollection(id);
      message.success('删除成功');
      loadCollections();
    } catch {
      message.error('删除失败');
    }
  };

  const handleCollectionModalOk = async () => {
    try {
      const values = await collectionForm.validateFields();
      const payload = {
        name: values.name,
        description: values.description,
        indicator_ids: targetKeys
      };

      if (editingCollection) {
        await updateIndicatorCollection(editingCollection.id, payload);
        message.success('更新成功');
      } else {
        await createIndicatorCollection(payload);
        message.success('创建成功');
      }
      setCollectionModalVisible(false);
      loadCollections();
    } catch (err) {
        if (err.message) message.error('保存失败: ' + err.message);
    }
  };

  const handleOpenRunModal = (record) => {
    setRunCollectionId(record.id);
    setCurrentRunningName(record.name);
    runForm.resetFields();
    const params = record.last_run_params || {};
    runForm.setFieldsValue({
        symbol: params.symbol || '600498',
        start_date: null,
        end_date: null,
        adjust: params.adjust !== undefined ? params.adjust : null
    });
    setRunModalVisible(true);
  };

  const handleRunCollection = async () => {
    try {
        const values = await runForm.validateFields();
        const payload = {
            symbol: values.symbol,
            start_date: values.start_date ? values.start_date.format('YYYYMMDD') : null,
            end_date: values.end_date ? values.end_date.format('YYYYMMDD') : null,
            adjust: values.adjust
        };

        setRunModalVisible(false);
        setResultLoading(true);
        setResultVisible(true);
        setResult(null);

        const res = await runIndicatorCollection(runCollectionId, payload);
        setResult(res.data.results);
        setCollections(prev =>
          Array.isArray(prev)
            ? prev.map(c =>
                c.id === runCollectionId
                  ? { ...c, last_run_params: payload }
                  : c
              )
            : prev
        );
        message.success('集合运行完成');
    } catch (err) {
        message.error('运行失败: ' + err.message);
    } finally {
        setResultLoading(false);
    }
  };

  // --- Render Helpers ---

  const buildExportText = (data, title) => {
    const formatData = (d) => {
      if (d && typeof d === 'object' && d.error) {
        const detailText = d.detail === undefined ? '' : String(d.detail);
        return `错误信息: ${String(d.error)}\n详情: ${detailText}`;
      }
      if (Array.isArray(d) || (d && typeof d === 'object')) return JSON.stringify(d, null, 2);
      return String(d);
    };

    if (!data) return '';

    const isCollectionResult =
      !Array.isArray(data) &&
      data !== null &&
      typeof data === 'object' &&
      Object.keys(data).some((k) => typeof data[k] === 'object');

    if (isCollectionResult) {
      const keys = Object.keys(data);
      return keys
        .map((k) => `【${k}】\n${formatData(data[k])}`)
        .join('\n\n');
    }

    return `【${title || '结果'}】\n${formatData(data)}`;
  };

  const copyToClipboard = async (text) => {
    if (!text) return false;
    let copied = false;
    if (navigator?.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        copied = true;
      } catch {
        copied = false;
      }
    }
    if (copied) return true;
    try {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'fixed';
      textarea.style.top = '0';
      textarea.style.left = '-9999px';
      document.body.appendChild(textarea);
      textarea.select();
      textarea.setSelectionRange(0, textarea.value.length);
      const ok = document.execCommand('copy');
      document.body.removeChild(textarea);
      return ok;
    } catch {
      return false;
    }
  };

  const handleOpenExport = () => {
    const text = buildExportText(result, currentRunningName);
    setExportText(text);
    setExportModalVisible(true);
  };

  const handleCopyExport = async () => {
    setCopyLoading(true);
    try {
      const ok = await copyToClipboard(exportText);
      if (ok) message.success('已复制到剪贴板');
      else message.error('复制失败，请手动复制');
    } finally {
      setCopyLoading(false);
    }
  };

  const renderResult = () => {
    if (!result) return <div style={{ color: '#999', textAlign: 'center', padding: 20 }}>暂无数据</div>;
    
    // Check if it's a collection result (object with keys) or single result (array/object)
    const isCollectionResult = !Array.isArray(result) && result !== null && typeof result === 'object' && Object.keys(result).some(k => typeof result[k] === 'object');
    
    if (isCollectionResult) {
        return (
            <Tabs defaultActiveKey={Object.keys(result)[0]} items={Object.keys(result).map(key => ({
                key,
                label: key,
                children: renderSingleResult(result[key])
            }))} />
        );
    } else {
        return renderSingleResult(result);
    }
  };

  const renderSingleResult = (data) => {
    // Handle error case nicely
    if (data && data.error) {
        let detail = data.detail;
        let parsed = null;
        if (typeof detail === 'string') {
            try {
                parsed = JSON.parse(detail);
            } catch {
                parsed = null;
            }
        }
        if (parsed && typeof parsed === 'object' && parsed.detail) detail = parsed.detail;
        
        return (
            <div style={{ padding: 20, background: '#fff1f0', border: '1px solid #ffa39e', borderRadius: 4 }}>
                <Typography.Title level={5} type="danger">运行出错</Typography.Title>
                <div style={{ marginBottom: 8 }}><strong>错误信息:</strong> {data.error}</div>
                <div><strong>详情:</strong> <code style={{ color: '#cf1322' }}>{String(detail)}</code></div>
                <div style={{ marginTop: 16, color: '#666' }}>
                    建议检查：
                    <ul style={{ margin: '8px 0 0 20px', padding: 0 }}>
                        <li>指标参数是否正确（如分钟线不能用 daily 周期）</li>
                        <li>日期格式是否符合接口要求</li>
                        <li>股票代码是否有效</li>
                    </ul>
                </div>
            </div>
        );
    }

    if (Array.isArray(data) && data.length > 0) {
        const first = data[0];
        const resultColumns = Object.keys(first).map(key => ({
          title: key,
          dataIndex: key,
          key: key,
          ellipsis: true,
          width: 150, 
          render: (text) => {
              if (typeof text === 'object' && text !== null) return JSON.stringify(text);
              return text;
          }
        }));
        return <Table dataSource={data} columns={resultColumns} scroll={{ x: 'max-content', y: 400 }} pagination={{ pageSize: 20 }} rowKey={(_, i) => i} size="small" />;
    } else if (typeof data === 'object') {
        return (
            <pre style={{ maxHeight: '400px', overflow: 'auto', background: '#f5f5f5', padding: 16, borderRadius: 4 }}>
              {JSON.stringify(data, null, 2)}
            </pre>
        );
    }
    return <div style={{ padding: 16 }}>{String(data)}</div>;
  };

  const configColumns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { title: '名称', dataIndex: 'name', width: 150 },
    { title: '接口名', dataIndex: 'api_name', width: 180, render: t => <code style={{background:'#f0f0f0'}}>{t}</code> },
    { title: '描述', dataIndex: 'description', ellipsis: true },
    {
      title: '操作',
      key: 'action',
      width: 250,
      render: (_, record) => (
        <Space>
          <Button type="primary" ghost icon={<PlayCircleOutlined />} onClick={() => handleRunConfig(record)}>运行</Button>
          <Button icon={<EditOutlined />} onClick={() => handleEditConfig(record)}>编辑</Button>
          <Button danger icon={<DeleteOutlined />} onClick={() => Modal.confirm({ title: '确认删除', onOk: () => handleDeleteConfig(record.id) })}>删除</Button>
        </Space>
      ),
    },
  ];

  const configIdToName = (configs || []).reduce((acc, c) => {
    acc[c.id] = c.name;
    return acc;
  }, {});

  const collectionColumns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { title: '名称', dataIndex: 'name', width: 150 },
    {
      title: '指标标签',
      dataIndex: 'indicator_ids',
      width: 380,
      render: (ids) => {
        const list = Array.isArray(ids) ? ids : [];
        if (list.length === 0) return <span style={{ color: '#999' }}>无</span>;
        return (
          <Space size={[0, 8]} wrap>
            {list.map((id) => (
              <Tag key={id}>{configIdToName[id] || `#${id}`}</Tag>
            ))}
          </Space>
        );
      },
    },
    { title: '包含指标数', dataIndex: 'indicator_ids', width: 120, render: (ids) => ids?.length || 0 },
    { title: '描述', dataIndex: 'description', ellipsis: true },
    {
      title: '操作',
      key: 'action',
      width: 250,
      render: (_, record) => (
        <Space>
          <Button type="primary" ghost icon={<PlayCircleOutlined />} onClick={() => handleOpenRunModal(record)}>运行</Button>
          <Button icon={<EditOutlined />} onClick={() => handleEditCollection(record)}>编辑</Button>
          <Button danger icon={<DeleteOutlined />} onClick={() => Modal.confirm({ title: '确认删除', onOk: () => handleDeleteCollection(record.id) })}>删除</Button>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ background: '#fff' }}>
      <Tabs activeKey={activeTab} onChange={setActiveTab} items={[
        {
            key: '1',
            label: '原子指标',
            children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                    <div style={{ textAlign: 'right', marginBottom: 16 }}>
                        <Button icon={<ReloadOutlined />} onClick={loadConfigs} style={{ marginRight: 8 }}>刷新</Button>
                        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreateConfig}>新建指标</Button>
                    </div>
                    <Table 
                        loading={configLoading} 
                        columns={configColumns} 
                        dataSource={configs} 
                        rowKey="id" 
                        pagination={{ pageSize: 10 }}
                        scroll={{ x: 'max-content' }}
                    />
                </Space>
            )
        },
        {
            key: '2',
            label: '指标集合',
            children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                    <div style={{ textAlign: 'right', marginBottom: 16 }}>
                        <Button icon={<ReloadOutlined />} onClick={loadCollections} style={{ marginRight: 8 }}>刷新</Button>
                        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreateCollection}>新建集合</Button>
                    </div>
                    <Table 
                        loading={collectionLoading} 
                        columns={collectionColumns} 
                        dataSource={collections} 
                        rowKey="id" 
                        pagination={{ pageSize: 10 }}
                        scroll={{ x: 'max-content' }}
                    />
                </Space>
            )
        }
      ]} />

      {resultVisible && (
        <Card 
          style={{ marginTop: 24 }} 
          title={`查询结果: ${currentRunningName}`}
          extra={
            <Space>
              <Button disabled={!result} onClick={handleOpenExport}>导出</Button>
              <Button onClick={() => setResultVisible(false)}>关闭结果</Button>
            </Space>
          }
          loading={resultLoading}
        >
          {renderResult()}
        </Card>
      )}

      {/* Config Modal */}
      <Modal
        title={editingConfig ? "编辑指标" : "新建指标"}
        open={configModalVisible}
        onOk={handleConfigModalOk}
        onCancel={() => setConfigModalVisible(false)}
        width={600}
        maskClosable={false}
      >
        <Form form={configForm} layout="vertical">
          <Form.Item name="name" label="指标名称" rules={[{ required: true }]}>
            <Input placeholder="例如：个股日线" />
          </Form.Item>
          <Form.Item name="api_name" label="AkShare 接口名" rules={[{ required: true }]}>
            <Input placeholder="例如：stock_zh_a_hist" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="params" label="参数模板 (JSON)" rules={[{ required: true }]}>
            <TextArea rows={6} style={{ fontFamily: 'monospace' }} />
          </Form.Item>
        </Form>
      </Modal>

      {/* Collection Modal */}
      <Modal
        title={editingCollection ? "编辑集合" : "新建集合"}
        open={collectionModalVisible}
        onOk={handleCollectionModalOk}
        onCancel={() => setCollectionModalVisible(false)}
        width={700}
        maskClosable={false}
      >
        <Form form={collectionForm} layout="vertical">
          <Form.Item name="name" label="集合名称" rules={[{ required: true }]}>
            <Input placeholder="例如：基本面分析集合" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item label="选择指标">
            <Transfer
                dataSource={configs}
                titles={['可选指标', '已选指标']}
                targetKeys={targetKeys}
                onChange={setTargetKeys}
                render={item => item.name}
                rowKey={item => item.id}
                listStyle={{ width: 300, height: 300 }}
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* Run Collection Modal */}
      <Modal
        title={`运行集合: ${currentRunningName}`}
        open={runModalVisible}
        onOk={handleRunCollection}
        onCancel={() => setRunModalVisible(false)}
        okText="运行"
      >
        <Form form={runForm} layout="vertical">
            <Form.Item name="symbol" label="股票代码 (Symbol)" rules={[{ required: true }]} help="将自动替换所有指标中的 symbol/stock/code 参数">
                <Input placeholder="例如：600498" />
            </Form.Item>
            
            <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
                高级选项（选填，留空则使用指标自带配置）
            </Typography.Text>

            <div style={{ display: 'flex', gap: 16 }}>
                <Form.Item name="start_date" label="开始日期" style={{ flex: 1 }}>
                    <DatePicker format="YYYYMMDD" style={{ width: '100%' }} placeholder="覆盖原配置" />
                </Form.Item>
                <Form.Item name="end_date" label="结束日期" style={{ flex: 1 }}>
                    <DatePicker format="YYYYMMDD" style={{ width: '100%' }} placeholder="覆盖原配置" />
                </Form.Item>
            </div>
            <Form.Item name="adjust" label="复权方式">
                <Select placeholder="覆盖原配置" allowClear>
                    <Option value="qfq">前复权 (qfq)</Option>
                    <Option value="hfq">后复权 (hfq)</Option>
                    <Option value="">不复权</Option>
                </Select>
            </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={`导出结果: ${currentRunningName}`}
        open={exportModalVisible}
        onCancel={() => setExportModalVisible(false)}
        footer={[
          <Button key="copy" loading={copyLoading} onClick={handleCopyExport} disabled={!exportText}>
            复制
          </Button>,
          <Button key="close" type="primary" onClick={() => setExportModalVisible(false)}>
            关闭
          </Button>,
        ]}
        width={800}
        maskClosable={false}
      >
        <Input.TextArea value={exportText} rows={18} readOnly style={{ fontFamily: 'monospace' }} />
      </Modal>
    </div>
  );
};

export default IndicatorPage;
