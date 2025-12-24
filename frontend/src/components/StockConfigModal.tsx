import React, { useEffect, useState } from 'react';
import { Modal, Form, Input, Button, message, Select, InputNumber, Space, TimePicker, Switch, Divider, Tooltip, Row, Col } from 'antd';
import { MinusCircleOutlined, PlusOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { Stock, IndicatorDefinition, AIConfig, RuleScript } from '../types';
import { updateStock, getIndicators, getAIConfigs, getRules } from '../api';

type MonitoringSchedulePeriod = { start: string; end: string };
type MonitoringScheduleFormPeriod = { start: dayjs.Dayjs | null; end: dayjs.Dayjs | null };

type StockConfigFormValues = {
  indicator_ids?: number[];
  prompt_template?: string;
  interval_seconds?: number;
  only_trade_days?: boolean;
  ai_provider_id?: number;
  monitoring_schedule_list?: MonitoringScheduleFormPeriod[];
  monitoring_mode?: 'ai_only' | 'script_only' | 'hybrid';
  rule_script_id?: number;
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
  const [rules, setRules] = useState<RuleScript[]>([]);
  
  // Watch monitoring_mode to toggle fields
  const monitoringMode = Form.useWatch('monitoring_mode', form);

  const fetchData = async () => {
    setLoadingIndicators(true);
    try {
      const [indRes, aiRes, ruleRes] = await Promise.all([getIndicators(), getAIConfigs(), getRules()]);
      setAllIndicators(indRes.data);
      setAiConfigs(aiRes.data);
      setRules(ruleRes.data);
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
      monitoring_mode: stock.monitoring_mode || 'ai_only',
      rule_script_id: stock.rule_script_id,
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

  const showAIConfig = monitoringMode !== 'script_only';
  const showRuleConfig = monitoringMode !== 'ai_only';

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
        initialValues={{ monitoring_mode: 'ai_only' }}
      >
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item name="interval_seconds" label="监测间隔（秒）">
              <InputNumber min={10} max={3600} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item name="only_trade_days" label="只在交易日监控" valuePropName="checked">
              <Switch checkedChildren="开启" unCheckedChildren="关闭" />
            </Form.Item>
          </Col>
        </Row>

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

        <Divider>策略配置</Divider>

        <Form.Item 
          name="monitoring_mode" 
          label={
            <span>
              监控模式&nbsp;
              <Tooltip title="AI Only: 每次都问AI; Script Only: 仅跑脚本规则; Hybrid: 脚本触发后才问AI (省钱)">
                <QuestionCircleOutlined />
              </Tooltip>
            </span>
          }
        >
          <Select>
            <Select.Option value="ai_only">仅 AI 分析 (AI Only)</Select.Option>
            <Select.Option value="script_only">仅硬规则 (Script Only)</Select.Option>
            <Select.Option value="hybrid">混合漏斗 (Hybrid: Rule -》 AI)</Select.Option>
          </Select>
        </Form.Item>

        {showRuleConfig && (
          <Form.Item 
            name="rule_script_id" 
            label="关联硬规则脚本" 
            rules={[{ required: true, message: '请选择规则脚本' }]}
            style={{ background: '#f6ffed', padding: 8, borderRadius: 4, border: '1px solid #b7eb8f' }}
          >
            <Select placeholder="请选择规则脚本">
              {rules.map(r => <Select.Option key={r.id} value={r.id}>{r.name}</Select.Option>)}
            </Select>
          </Form.Item>
        )}

        {showAIConfig && (
          <div style={{ background: '#e6f7ff', padding: 8, borderRadius: 4, border: '1px solid #91d5ff', marginBottom: 16 }}>
            <Form.Item name="ai_provider_id" label="AI 配置" rules={[{ required: true, message: '请选择AI' }]}>
              <Select placeholder="请选择 AI 配置">
                {aiConfigs.map(c => <Select.Option key={c.id} value={c.id}>{c.name}</Select.Option>)}
              </Select>
            </Form.Item>
            
            <Form.Item name="indicator_ids" label="投喂指标（Context Data）">
              <Select
                mode="multiple"
                placeholder="请选择投喂给 AI 的指标数据"
                loading={loadingIndicators}
                options={allIndicators.map((x) => ({
                  value: x.id,
                  label: `${x.name}（${x.akshare_api}）`,
                }))}
              />
            </Form.Item>
            
            <Form.Item name="prompt_template" label="个股分析提示词 (Prompt)">
              <Input.TextArea rows={4} placeholder="覆盖全局提示词..." />
            </Form.Item>
          </div>
        )}

        <Button type="primary" htmlType="submit" block size="large">保存配置</Button>
      </Form>
    </Modal>
  );
};

export default StockConfigModal;
