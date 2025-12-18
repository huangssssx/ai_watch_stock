import React, { useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select } from 'antd';
import type { Stock, IndicatorDefinition } from '../types';
import { updateStock, getIndicators } from '../api';

type StockConfigFormValues = {
  indicator_ids?: number[];
  prompt_template?: string;
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

  const refreshIndicators = async () => {
    setLoadingIndicators(true);
    try {
      const res = await getIndicators();
      setAllIndicators(res.data);
    } catch {
      message.error('加载指标库失败');
    } finally {
      setLoadingIndicators(false);
    }
  };

  useEffect(() => {
    if (!visible) return;
    refreshIndicators();
  }, [visible]);

  useEffect(() => {
    if (!visible) return;
    form.setFieldsValue({
      prompt_template: stock.prompt_template,
      indicator_ids: (stock.indicators || []).map((x) => x.id),
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
