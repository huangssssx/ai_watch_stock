import React, { useCallback, useEffect, useState } from 'react';
import { Modal, Form, Button, message, Select, Input } from 'antd';
import { CopyOutlined, ReloadOutlined } from '@ant-design/icons';
import type { Stock, IndicatorDefinition } from '../types';
import { getIndicators, getAIWatchConfig, previewStockIndicators } from '../api';

interface Props {
  visible: boolean;
  stock: Stock;
  onClose: () => void;
}

const IndicatorPreviewModal: React.FC<Props> = ({ visible, stock, onClose }) => {
  const [form] = Form.useForm();
  const [allIndicators, setAllIndicators] = useState<IndicatorDefinition[]>([]);
  const [loading, setLoading] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [resultData, setResultData] = useState<Record<string, unknown> | null>(null);

  const fetchInitData = useCallback(async () => {
    setLoading(true);
    try {
      const [indRes, configRes] = await Promise.all([
        getIndicators(),
        getAIWatchConfig(stock.id)
      ]);
      setAllIndicators(indRes.data);
      
      const config = configRes.data;
      let indicatorIds: number[] = [];
      try {
        indicatorIds = JSON.parse(config.indicator_ids || '[]');
      } catch {
        indicatorIds = [];
      }
      
      form.setFieldsValue({
        indicator_ids: indicatorIds.length > 0 ? indicatorIds : undefined,
      });
      
    } catch {
      message.error('加载配置失败');
    } finally {
      setLoading(false);
    }
  }, [form, stock.id]);

  useEffect(() => {
    if (visible) {
        fetchInitData();
        setResultData(null);
    }
  }, [fetchInitData, visible, stock.id]);

  const handleFetch = async (values: { indicator_ids?: number[] }) => {
      setFetching(true);
      setResultData(null);
      try {
          // Reusing structure of AIWatchAnalyzeRequest for convenience
          const res = await previewStockIndicators(stock.id, {
              indicator_ids: values.indicator_ids || [],
              custom_prompt: "" 
          });
          
          if (res.data.ok) {
              setResultData(res.data.data || null);
              message.success('获取成功');
          } else {
              message.error(res.data.error || '获取失败');
          }
      } catch {
          message.error('请求失败');
      } finally {
          setFetching(false);
      }
  };

  const handleCopy = () => {
      if (!resultData) return;
      const text = JSON.stringify(resultData, null, 2);
      navigator.clipboard.writeText(text).then(() => {
          message.success('已复制到剪贴板');
      });
  };

  const handleSelectAll = () => {
      const allIds = allIndicators.map(i => i.id);
      form.setFieldsValue({ indicator_ids: allIds });
  };

  return (
    <Modal
      title={`指标数据预览 - ${stock.symbol} ${stock.name}`}
      open={visible}
      onCancel={onClose}
      footer={null}
      width={900}
      maskClosable={false}
      destroyOnClose
    >
        <Form form={form} layout="vertical" onFinish={handleFetch}>
            <Form.Item
                name="indicator_ids"
                label={
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
                        <span>选择指标</span>
                        <Button type="link" size="small" onClick={handleSelectAll}>
                            选择全部指标
                        </Button>
                    </div>
                }
                rules={[{ required: true, message: '请至少选择一个指标' }]}
            >
                <Select
                    mode="multiple"
                    placeholder="选择要查看的指标"
                    options={allIndicators.map(x => ({ value: x.id, label: x.name }))}
                    loading={loading}
                    maxTagCount="responsive"
                />
            </Form.Item>
            <Form.Item>
                <Button type="primary" htmlType="submit" loading={fetching} icon={<ReloadOutlined />}>
                    获取数据
                </Button>
                {resultData && (
                    <Button onClick={handleCopy} icon={<CopyOutlined />} style={{ marginLeft: 8 }}>
                        复制 JSON
                    </Button>
                )}
            </Form.Item>
        </Form>

        {resultData && (
            <div style={{ marginTop: 16 }}>
                <Input.TextArea 
                    value={JSON.stringify(resultData, null, 2)} 
                    rows={15} 
                    readOnly 
                    style={{ fontFamily: 'monospace', whiteSpace: 'pre', backgroundColor: '#f5f5f5' }}
                />
            </div>
        )}
    </Modal>
  );
};

export default IndicatorPreviewModal;
