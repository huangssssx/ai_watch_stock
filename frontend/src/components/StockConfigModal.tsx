import React, { useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select, InputNumber } from 'antd';
import type { Stock, IndicatorDefinition, AIConfig } from '../types';
import { updateStock, getIndicators, getAIConfigs } from '../api';

type StockConfigFormValues = {
  indicator_ids?: number[];
  prompt_template?: string;
  interval_seconds?: number;
  ai_provider_id?: number;
};

interface Props {
  visible: boolean;
  stock: Stock;
  onClose: () => void;
}

const StockConfigModal: React.FC<Props> = ({ visible, stock, onClose }) => {
  const [form] = Form.useForm();
  const [allIndicators, setAllIndicators] = useState<IndicatorDefinition[]>([]);
  const [loadingIndicators, setLoadingIndicators] = useState(false);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);

  const fetchData = async () => {
    setLoadingIndicators(true);
    try {
      const [indRes, aiRes] = await Promise.all([getIndicators(), getAIConfigs()]);
      setAllIndicators(indRes.data);
      setAiConfigs(aiRes.data);
    } catch {
      message.error('加载配置数据失败');
    } finally {
      setLoadingIndicators(false);
    }
  };

  useEffect(() => {
    if (!visible) return;
    fetchData();
  }, [visible]);

  useEffect(() => {
    if (!visible) return;
    form.setFieldsValue({
      prompt_template: stock.prompt_template,
      indicator_ids: (stock.indicators || []).map((x) => x.id),
      interval_seconds: stock.interval_seconds,
      ai_provider_id: stock.ai_provider_id,
    });
  }, [form, stock, visible]);

  const handleUpdate = async (values: StockConfigFormValues) => {
    try {
      await updateStock(stock.id, values);
      message.success('已保存');
      onClose();
    } catch {
      message.error('保存失败');
    }
  };

  return (
    <Modal 
      title={`配置 ${stock.symbol}`} 
      open={visible} 
      onCancel={onClose}
      footer={null}
      width={800}
    >
      <Form 
        form={form} 
        layout="vertical" 
        onFinish={handleUpdate}
      >
        <Form.Item name="interval_seconds" label="监测间隔（秒）">
          <InputNumber min={10} max={3600} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item name="ai_provider_id" label="AI 配置">
          <Select placeholder="请选择 AI 配置">
            {aiConfigs.map(c => <Select.Option key={c.id} value={c.id}>{c.name}</Select.Option>)}
          </Select>
        </Form.Item>
        <Form.Item name="indicator_ids" label="监控指标（可多选）">
          <Select
            mode="multiple"
            placeholder="请选择用于监控的指标"
            loading={loadingIndicators}
            options={allIndicators.map((x) => ({
              value: x.id,
              label: `${x.name}（${x.akshare_api}）`,
            }))}
          />
        </Form.Item>
        <Form.Item name="prompt_template" label="分析提示词（Prompt）">
          <Input.TextArea rows={4} />
        </Form.Item>
        <Button type="primary" htmlType="submit">保存</Button>
      </Form>
    </Modal>
  );
};

export default StockConfigModal;
