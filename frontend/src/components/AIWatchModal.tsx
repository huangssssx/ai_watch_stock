import React, { useCallback, useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select, Card, Collapse } from 'antd';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeRaw from 'rehype-raw';
import type { Stock, IndicatorDefinition, AIConfig, AIWatchAnalyzeResponse } from '../types';
import { getIndicators, getAIWatchConfig, runAIWatchAnalyze, getAIConfigs } from '../api';
import { CaretRightOutlined } from '@ant-design/icons';

const { Panel } = Collapse;

interface Props {
  visible: boolean;
  stock: Stock;
  onClose: () => void;
}

type AIWatchHistoryEntry = {
  timestamp: string;
  result: AIWatchAnalyzeResponse;
};

type AIWatchFormValues = {
  ai_provider_id?: number;
  indicator_ids?: number[];
  custom_prompt: string;
};

const AIWatchModal: React.FC<Props> = ({ visible, stock, onClose }) => {
  const [form] = Form.useForm();
  const [allIndicators, setAllIndicators] = useState<IndicatorDefinition[]>([]);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);
  const [loadingIndicators, setLoadingIndicators] = useState(false);
  const [result, setResult] = useState<AIWatchAnalyzeResponse | null>(null);
  const [history, setHistory] = useState<AIWatchHistoryEntry[]>([]);
  const [analyzing, setAnalyzing] = useState(false);

  const fetchInitData = useCallback(async () => {
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
      } catch {
        indicatorIds = [];
      }
      
      let hist: AIWatchHistoryEntry[] = [];
      try {
        hist = JSON.parse(config.analysis_history || '[]');
      } catch {
        hist = [];
      }
      
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
  }, [form, stock.ai_provider_id, stock.id]);

  useEffect(() => {
    if (visible) {
      fetchInitData();
      setResult(null);
    }
  }, [fetchInitData, visible, stock.id]);

  const handleAnalyze = async (values: AIWatchFormValues) => {
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
    } catch {
        message.error('请求失败');
    } finally {
        setAnalyzing(false);
    }
  };

  const handleSelectAll = () => {
      const allIds = allIndicators.map(i => i.id);
      form.setFieldsValue({ indicator_ids: allIds });
  };

  const renderAnalysisContent = (data: AIWatchAnalyzeResponse | null) => {
      if (!data) return null;

      // Render the raw response as Markdown
      const rawResponse = data.raw_response || "No response";

      return (
        <div style={{ marginTop: 16 }}>
            <Card size="small" title="AI 分析结果">
                <div className="markdown-content" style={{
                    fontSize: '14px',
                    lineHeight: '1.7',
                    color: '#1f2937'
                }}>
                    <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        rehypePlugins={[rehypeRaw]}
                        components={{
                            // 自定义标题样式
                            h1: ({children}) => <h1 style={{fontSize: '24px', fontWeight: 'bold', marginTop: '16px', marginBottom: '12px', paddingBottom: '8px', borderBottom: '2px solid #e5e7eb'}}>{children}</h1>,
                            h2: ({children}) => <h2 style={{fontSize: '20px', fontWeight: 'semibold', marginTop: '14px', marginBottom: '10px', paddingBottom: '6px', borderBottom: '1px solid #e5e7eb'}}>{children}</h2>,
                            h3: ({children}) => <h3 style={{fontSize: '18px', fontWeight: 'semibold', marginTop: '12px', marginBottom: '8px'}}>{children}</h3>,
                            h4: ({children}) => <h4 style={{fontSize: '16px', fontWeight: 'semibold', marginTop: '10px', marginBottom: '6px'}}>{children}</h4>,
                            // 自定义段落样式
                            p: ({children}) => <p style={{marginBottom: '12px'}}>{children}</p>,
                            // 自定义列表样式
                            ul: ({children}) => <ul style={{marginLeft: '20px', marginBottom: '12px', listStyleType: 'disc'}}>{children}</ul>,
                            ol: ({children}) => <ol style={{marginLeft: '20px', marginBottom: '12px', listStyleType: 'decimal'}}>{children}</ol>,
                            li: ({children}) => <li style={{marginBottom: '4px'}}>{children}</li>,
                            // 自定义代码块样式
                            code: ({children, className, ...props}) => !className
                                ? <code
                                    {...props}
                                    style={{
                                        backgroundColor: '#f3f4f6',
                                        padding: '2px 6px',
                                        borderRadius: '4px',
                                        fontFamily: 'Monaco, Consolas, monospace',
                                        fontSize: '13px',
                                        color: '#ef4444'
                                    }}
                                >{children}</code>
                                : <code {...props} className={className}>{children}</code>,
                            pre: ({children}) => <pre style={{
                                backgroundColor: '#1f2937',
                                color: '#f9fafb',
                                padding: '16px',
                                borderRadius: '8px',
                                overflow: 'auto',
                                marginBottom: '16px',
                                fontFamily: 'Monaco, Consolas, monospace',
                                fontSize: '13px',
                                lineHeight: '1.5'
                            }}>{children}</pre>,
                            // 自定义引用块样式
                            blockquote: ({children}) => <blockquote style={{
                                borderLeft: '4px solid #3b82f6',
                                paddingLeft: '16px',
                                marginLeft: '0',
                                marginBottom: '12px',
                                color: '#6b7280',
                                fontStyle: 'italic'
                            }}>{children}</blockquote>,
                            // 自定义表格样式
                            table: ({children}) => <div style={{overflowX: 'auto', marginBottom: '16px'}}><table style={{
                                borderCollapse: 'collapse',
                                width: '100%',
                                fontSize: '14px'
                            }}>{children}</table></div>,
                            thead: ({children}) => <thead style={{backgroundColor: '#f9fafb'}}>{children}</thead>,
                            tbody: ({children}) => <tbody>{children}</tbody>,
                            tr: ({children}) => <tr style={{borderBottom: '1px solid #e5e7eb'}}>{children}</tr>,
                            th: ({children}) => <th style={{
                                padding: '8px 12px',
                                textAlign: 'left',
                                fontWeight: 'semibold',
                                borderBottom: '1px solid #d1d5db'
                            }}>{children}</th>,
                            td: ({children}) => <td style={{
                                padding: '8px 12px',
                                borderBottom: '1px solid #e5e7eb'
                            }}>{children}</td>,
                            // 自定义分隔线样式
                            hr: () => <hr style={{border: 'none', borderTop: '1px solid #e5e7eb', margin: '16px 0'}} />,
                            // 自定义强调样式
                            strong: ({children}) => <strong style={{color: '#1f2937', fontWeight: '600'}}>{children}</strong>,
                            em: ({children}) => <em style={{color: '#4b5563'}}>{children}</em>,
                        }}
                    >
                        {rawResponse}
                    </ReactMarkdown>
                </div>
            </Card>
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
            label={
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
                    <span>选择指标 (Context)</span>
                    <Button type="link" size="small" onClick={handleSelectAll}>
                        选择全部指标
                    </Button>
                </div>
            }
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
                      <Panel header={`${item.timestamp}`} key={idx}>
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
