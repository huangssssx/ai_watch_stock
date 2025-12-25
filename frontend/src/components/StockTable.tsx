import React, { useState, useEffect } from 'react';
import { Table, Button, Modal, Form, Input, InputNumber, Select, message, Tag, Card, Space } from 'antd';
import type { Stock, AIConfig, StockTestRunResponse, IndicatorDefinition } from '../types';
import { getStocks, updateStock, deleteStock, createStock, getAIConfigs, testRunStock, getIndicators } from '../api';
import { SettingOutlined, DeleteOutlined, PlayCircleOutlined, PauseCircleOutlined, FileTextOutlined } from '@ant-design/icons';
import StockConfigModal from './StockConfigModal.tsx';
import LogsViewer from './LogsViewer.tsx';
import type { ColumnsType } from 'antd/es/table';

type StockCreateFormValues = {
  symbol: string;
  name?: string;
  interval_seconds: number;
  ai_provider_id?: number;
  indicator_ids?: number[];
};

const StockTable: React.FC = () => {
  const [stocks, setStocks] = useState<Stock[]>([]);
  const [loading, setLoading] = useState(false);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);
  const [allIndicators, setAllIndicators] = useState<IndicatorDefinition[]>([]);
  const [loadingIndicators, setLoadingIndicators] = useState(false);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [currentStock, setCurrentStock] = useState<Stock | null>(null);
  const [testModalVisible, setTestModalVisible] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testStock, setTestStock] = useState<Stock | null>(null);
  const [testResult, setTestResult] = useState<StockTestRunResponse | null>(null);
  
  const [logsModalVisible, setLogsModalVisible] = useState(false);
  const [logsStock, setLogsStock] = useState<Stock | null>(null);

  const [form] = Form.useForm();

  const fetchIndicators = async () => {
    setLoadingIndicators(true);
    try {
      const res = await getIndicators();
      setAllIndicators(res.data);
    } catch {
      message.error('加载指标列表失败');
    } finally {
      setLoadingIndicators(false);
    }
  };

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

  useEffect(() => {
    if (!isModalVisible) return;
    void fetchIndicators();
  }, [isModalVisible]);

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
      const res = await testRunStock(stock.id, { send_alerts: true, bypass_checks: true });
      setTestResult(res.data);
      if (res.data.ok) {
        if (res.data.skipped_reason) message.warning(`已跳过：${res.data.skipped_reason}`);
        else message.success('测试完成');
      } else {
        message.error(res.data.error || '测试失败');
      }
    } catch {
      message.error('测试失败（请确认已配置策略所需的 AI / 指标 / 规则脚本）');
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
          <Button icon={<FileTextOutlined />} onClick={() => { setLogsStock(record); setLogsModalVisible(true); }} title="查看日志" />
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
          <Form.Item
            name="indicator_ids"
            label={
              <Space size={8}>
                <span>投喂指标（Context Data）</span>
                <Button
                  type="link"
                  size="small"
                  disabled={loadingIndicators || allIndicators.length === 0}
                  onClick={() => form.setFieldValue('indicator_ids', allIndicators.map((x) => x.id))}
                >
                  全部选择
                </Button>
              </Space>
            }
          >
            <Select
              mode="multiple"
              placeholder="请选择投喂给 AI 的指标数据"
              loading={loadingIndicators}
              options={allIndicators.map((x) => ({
                value: x.id,
                label: `${x.name}（${x.akshare_api || '纯脚本'}）`,
              }))}
            />
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
        <div style={{ marginBottom: 12 }}>
          <span style={{ marginRight: 8 }}>策略：</span>
          {(() => {
            const mode = testResult?.monitoring_mode ?? testStock?.monitoring_mode ?? 'ai_only';
            if (mode === 'ai_only') return <Tag color="blue">仅 AI</Tag>;
            if (mode === 'script_only') return <Tag color="green">仅硬规则</Tag>;
            return <Tag color="purple">混合（Rule → AI）</Tag>;
          })()}
        </div>

        {testResult && (
          <Card title="执行信息" size="small" style={{ marginBottom: 12 }}>
            <div style={{ marginBottom: 8 }}>
              状态：
              {testResult.ok ? <Tag color="green">OK</Tag> : <Tag color="red">FAIL</Tag>}
              {testResult.skipped_reason ? <Tag>{`SKIP: ${testResult.skipped_reason}`}</Tag> : null}
              {testResult.is_alert ? <Tag color="red">ALERT</Tag> : <Tag>NO ALERT</Tag>}
              {testResult.alert_attempted ? <Tag color="blue">已尝试告警</Tag> : <Tag>未发送告警</Tag>}
              {testResult.alert_suppressed ? <Tag color="gold">告警被抑制</Tag> : null}
            </div>
            <div style={{ marginBottom: 8 }}>Run ID：{testResult.run_id || '-'}</div>
            <div style={{ marginBottom: 8 }}>错误：{testResult.error || '-'}</div>
          </Card>
        )}

        {(() => {
          const precheckSkips = new Set(['monitoring_disabled', 'outside_schedule', 'not_trade_day']);
          const isPrecheckSkip = testResult?.skipped_reason ? precheckSkips.has(testResult.skipped_reason) : false;
          if (isPrecheckSkip) return null;
          if (testResult?.monitoring_mode === 'ai_only') return null;
          return (
          <Card title="硬规则执行" size="small" style={{ marginBottom: 12 }}>
            <div style={{ marginBottom: 8 }}>
              触发：
              {testResult?.script_triggered ? <Tag color="red">YES</Tag> : <Tag color="green">NO</Tag>}
            </div>
            <div style={{ marginBottom: 8 }}>Message：{testResult?.script_message || '-'}</div>
            <div>
              <div style={{ marginBottom: 4 }}>Log Output</div>
              <pre style={{ background: '#f5f5f5', padding: 8, borderRadius: 4, maxHeight: 240, overflow: 'auto' }}>
                {testResult?.script_log || '(No output)'}
              </pre>
            </div>
          </Card>
          );
        })()}

        {(() => {
          const precheckSkips = new Set(['monitoring_disabled', 'outside_schedule', 'not_trade_day']);
          const isPrecheckSkip = testResult?.skipped_reason ? precheckSkips.has(testResult.skipped_reason) : false;
          if (isPrecheckSkip) return null;
          if (testResult?.monitoring_mode === 'script_only') return null;
          if (!testResult?.ai_reply) return null;
          return (
          <Card title="AI 执行" size="small">
            <div style={{ marginBottom: 8 }}>
              模型：{testResult.model_name ?? '-'} / Base URL：{testResult.base_url ?? '-'} / 耗时：
              {testResult.ai_duration_ms ?? '-'}ms
            </div>
            {testResult.data_truncated && (
              <div style={{ marginBottom: 8 }}>
                提示：数据过长已截断（最多 {testResult.data_char_limit ?? '-'} 字符）
              </div>
            )}
            {testResult.system_prompt ? (
              <div style={{ marginBottom: 12 }}>
                <div style={{ marginBottom: 4 }}>System Prompt</div>
                <Input.TextArea value={testResult.system_prompt} readOnly autoSize={{ minRows: 3, maxRows: 10 }} />
              </div>
            ) : null}
            {testResult.user_prompt ? (
              <div style={{ marginBottom: 12 }}>
                <div style={{ marginBottom: 4 }}>User Prompt（包含指标数据）</div>
                <Input.TextArea value={testResult.user_prompt} readOnly autoSize={{ minRows: 6, maxRows: 16 }} />
              </div>
            ) : null}
            <div>
              <div style={{ marginBottom: 4 }}>AI 返回（解析后 JSON）</div>
              <div style={{ border: '1px solid #d9d9d9', padding: '8px', borderRadius: '4px', background: '#f5f5f5' }}>
                <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>
                  {JSON.stringify(testResult.ai_reply, null, 2)}
                </pre>
              </div>
            </div>
          </Card>
          );
        })()}

        {!testResult && (
          <div>{testing ? '测试中，请稍候…' : '暂无结果'}</div>
        )}
      </Modal>

      <Modal
        title={`监控日志：${logsStock ? `${logsStock.symbol} ${logsStock.name || ''}` : ''}`}
        open={logsModalVisible}
        onCancel={() => {
          setLogsModalVisible(false);
          setLogsStock(null);
        }}
        footer={null}
        width="95%"
        style={{ top: 20 }}
        styles={{ body: { height: 'calc(100vh - 100px)', overflow: 'auto' } }}
        destroyOnHidden={true}
      >
        {logsStock && <LogsViewer stockId={logsStock.id} />}
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
