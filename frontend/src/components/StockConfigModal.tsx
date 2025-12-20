import React, { useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select, InputNumber, Space, TimePicker, Switch } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { Stock, IndicatorDefinition, AIConfig } from '../types';
import { updateStock, getIndicators, getAIConfigs } from '../api';

type MonitoringSchedulePeriod = { start: string; end: string };
type MonitoringScheduleFormPeriod = { start: dayjs.Dayjs | null; end: dayjs.Dayjs | null };

type StockConfigFormValues = {
  indicator_ids?: number[];
  prompt_template?: string;
  interval_seconds?: number;
  only_trade_days?: boolean;
  ai_provider_id?: number;
  monitoring_schedule_list?: MonitoringScheduleFormPeriod[];
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
    
    let scheduleList: { start: dayjs.Dayjs | null; end: dayjs.Dayjs | null }[] = [];
    if (stock.monitoring_schedule) {
      try {
        const parsed: unknown = JSON.parse(stock.monitoring_schedule);
        if (Array.isArray(parsed)) {
          scheduleList = parsed
            .map((item: unknown) => {
              const raw = item as Partial<MonitoringSchedulePeriod>;
              return {
                start: raw.start ? dayjs(raw.start, 'HH:mm') : null,
                end: raw.end ? dayjs(raw.end, 'HH:mm') : null,
              };
            })
            .filter((x) => x.start && x.end);
        }
      } catch {
        scheduleList = [];
      }
    } else {
       // Default
       scheduleList = [
         { start: dayjs('09:30', 'HH:mm'), end: dayjs('11:30', 'HH:mm') },
         { start: dayjs('13:00', 'HH:mm'), end: dayjs('15:00', 'HH:mm') },
       ];
    }

    form.setFieldsValue({
      prompt_template: stock.prompt_template,
      indicator_ids: (stock.indicators || []).map((x) => x.id),
      interval_seconds: stock.interval_seconds,
      ai_provider_id: stock.ai_provider_id,
      monitoring_schedule_list: scheduleList,
      only_trade_days: stock.only_trade_days ?? true,
    });
  }, [form, stock, visible]);

  const handleUpdate = async (values: StockConfigFormValues) => {
    try {
      const schedule = (values.monitoring_schedule_list || [])
        .map((item) => ({
          start: item.start ? item.start.format('HH:mm') : '',
          end: item.end ? item.end.format('HH:mm') : '',
        }))
        .filter((x) => x.start && x.end);

      const payload = {
          ...values,
          monitoring_schedule: JSON.stringify(schedule)
      };
      delete payload.monitoring_schedule_list;

      await updateStock(stock.id, payload);
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
        
        <Form.Item name="only_trade_days" label="只在交易日监控" valuePropName="checked">
          <Switch checkedChildren="开启" unCheckedChildren="关闭" />
        </Form.Item>

        <Form.Item label="监控时段">
          <Form.List name="monitoring_schedule_list">
            {(fields, { add, remove }) => (
              <>
                {fields.map(({ key, name, ...restField }) => (
                  <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                    <Form.Item
                      {...restField}
                      name={[name, 'start']}
                      rules={[{ required: true, message: 'Missing start time' }]}
                      style={{ marginBottom: 0 }}
                    >
                      <TimePicker format="HH:mm" placeholder="开始时间" />
                    </Form.Item>
                    <span style={{ padding: '0 8px' }}>-</span>
                    <Form.Item
                      {...restField}
                      name={[name, 'end']}
                      rules={[{ required: true, message: 'Missing end time' }]}
                      style={{ marginBottom: 0 }}
                    >
                      <TimePicker format="HH:mm" placeholder="结束时间" />
                    </Form.Item>
                    <MinusCircleOutlined onClick={() => remove(name)} />
                  </Space>
                ))}
                <Form.Item style={{ marginBottom: 0 }}>
                  <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                    添加监控时段
                  </Button>
                </Form.Item>
              </>
            )}
          </Form.List>
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
