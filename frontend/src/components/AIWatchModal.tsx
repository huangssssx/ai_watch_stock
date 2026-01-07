import React, { useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select, Card, Alert, Collapse } from 'antd';
import type { Stock, IndicatorDefinition, AIConfig } from '../types';
import { getIndicators, getAIWatchConfig, runAIWatchAnalyze, getAIConfigs } from '../api';
import { CaretRightOutlined } from '@ant-design/icons';

const { Panel } = Collapse;

interface Props {
  visible: boolean;
  stock: Stock;
  onClose: () => void;
}

const AIWatchModal: React.FC<Props> = ({ visible, stock, onClose }) => {
  const [form] = Form.useForm();
  const [allIndicators, setAllIndicators] = useState<IndicatorDefinition[]>([]);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);
  const [loadingIndicators, setLoadingIndicators] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [analyzing, setAnalyzing] = useState(false);

  const fetchInitData = async () => {
    setLoadingIndicators(true);
    try {
      const [indRes, configRes, aiRes] = await Promise.all([
        getIndicators(),
        getAIWatchConfig(stock.id),
        getAIConfigs()
      ]);
      setAllIndicators(indRes.data);
      setAiConfigs(aiRes.data);
      
      const config = configRes.data;
      let indicatorIds: number[] = [];
      try {
        indicatorIds = JSON.parse(config.indicator_ids || '[]');
      } catch {}
      
      let hist: any[] = [];
      try {
        hist = JSON.parse(config.analysis_history || '[]');
      } catch {}
      
      setHistory(hist);

      form.setFieldsValue({
        indicator_ids: indicatorIds.length > 0 ? indicatorIds : undefined,
        custom_prompt: config.custom_prompt || '',
        ai_provider_id: config.ai_provider_id || stock.ai_provider_id,
      });
      
    } catch {
      message.error('加载配置失败');
    } finally {
      setLoadingIndicators(false);
    }
  };

  useEffect(() => {
    if (visible) {
        fetchInitData();
        setResult(null);
    }
  }, [visible, stock]);

  const handleAnalyze = async (values: any) => {
    setAnalyzing(true);
    setResult(null);
    try {
        const res = await runAIWatchAnalyze(stock.id, {
            indicator_ids: values.indicator_ids || [],
            custom_prompt: values.custom_prompt,
            ai_provider_id: values.ai_provider_id || stock.ai_provider_id
        });
        
        if (res.data.ok) {
            setResult(res.data);
            message.success('分析完成');
            fetchInitData(); // Refresh history
        } else {
            message.error(res.data.error || '分析失败');
        }
    } catch (e) {
        message.error('请求失败');
    } finally {
        setAnalyzing(false);
    }
  };

  const renderAnalysisContent = (data: any) => {
      if (!data) return null;
      const aiReply = data.ai_reply;
      
      if (!aiReply) {
          return <pre style={{ whiteSpace: 'pre-wrap' }}>{data.raw_response || 'No response'}</pre>;
      }
      
      return (
        <div style={{ marginTop: 16 }}>
             <Alert
                message={aiReply.signal}
                description={aiReply.action_advice}
                type={aiReply.type === 'warning' ? 'warning' : 'info'}
                showIcon
                style={{ marginBottom: 16 }}
             />
             <Card size="small" title="分析详情">
                <p><strong>建议仓位:</strong> {aiReply.suggested_position}</p>
                <p><strong>持仓时间:</strong> {aiReply.duration}</p>
                <p><strong>止损价:</strong> {aiReply.stop_loss_price}</p>
                <p><strong>分析逻辑:</strong> {aiReply.message}</p>
             </Card>
             <Collapse ghost>
                <Panel header="原始响应" key="1">
                    <pre style={{ whiteSpace: 'pre-wrap' }}>{data.raw_response}</pre>
                </Panel>
             </Collapse>
        </div>
      );
  };

  return (
    <Modal
      title={`AI 看盘 - ${stock.symbol} ${stock.name}`}
      open={visible}
      onCancel={onClose}
      footer={null}
      width={900}
      maskClosable={false}
      destroyOnClose
    >
      <Form form={form} layout="vertical" onFinish={handleAnalyze}>
        <Form.Item
            name="ai_provider_id"
            label="AI 配置"
            rules={[{ required: true, message: '请选择 AI 配置' }]}
        >
            <Select placeholder="选择 AI 配置">
                {aiConfigs.map(c => <Select.Option key={c.id} value={c.id}>{c.name}</Select.Option>)}
            </Select>
        </Form.Item>
        <Form.Item
            name="indicator_ids"
            label="选择指标 (Context)"
            rules={[{ required: true, message: '请至少选择一个指标' }]}
        >
            <Select
                mode="multiple"
                placeholder="选择要发送给 AI 的指标"
                options={allIndicators.map(x => ({ value: x.id, label: x.name }))}
                loading={loadingIndicators}
                maxTagCount="responsive"
            />
        </Form.Item>
        <Form.Item
            name="custom_prompt"
            label="自定义提示词 (Prompt)"
            rules={[{ required: true, message: '请输入提示词' }]}
        >
            <Input.TextArea 
                rows={4} 
                placeholder="例如：请分析该股的短线趋势，并给出具体买卖点..." 
            />
        </Form.Item>
        <Form.Item>
            <Button type="primary" htmlType="submit" loading={analyzing} block size="large" icon={<CaretRightOutlined />}>
                开始分析
            </Button>
        </Form.Item>
      </Form>

      {result && (
          <div style={{ marginTop: 24, borderTop: '1px solid #eee', paddingTop: 16 }}>
              <h3>本次分析结果</h3>
              {renderAnalysisContent(result)}
          </div>
      )}

      {history.length > 0 && (
          <div style={{ marginTop: 24, borderTop: '1px solid #eee', paddingTop: 16 }}>
              <h3>历史记录 (最近 3 条)</h3>
              <Collapse>
                  {history.map((item, idx) => (
                      <Panel header={`${item.timestamp} - ${item.result?.ai_reply?.signal || 'Unknown'}`} key={idx}>
                          {renderAnalysisContent(item.result)}
                      </Panel>
                  ))}
              </Collapse>
          </div>
      )}
    </Modal>
  );
};

export default AIWatchModal;
