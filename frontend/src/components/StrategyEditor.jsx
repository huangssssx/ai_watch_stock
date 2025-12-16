import React, { useState, useEffect } from 'react';
import { Form, Input, Button, Card, message, InputNumber } from 'antd';
import { createStrategy, updateStrategy, getStrategy } from '../api';
import { useNavigate, useParams } from 'react-router-dom';

const StrategyEditor = () => {
  const [form] = Form.useForm();
  const navigate = useNavigate();
  const { id } = useParams();
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (id) {
      loadStrategy();
    } else {
        // Set default template
        form.setFieldsValue({
            content: JSON.stringify({
                symbol: "600498",
                variables: {
                  stop_loss: 24.95,
                  buy_price: 25.65,
                  holding: 800
                },
                data: [
                  {id: "spot", api: "stock_zh_a_spot_em", refresh: 3}
                ],
                indicators: {},
                scenarios: []
            }, null, 2)
        })
    }
  }, [id]);

  const loadStrategy = async () => {
    try {
      const res = await getStrategy(id);
      form.setFieldsValue({
        name: res.data.name,
        symbol: res.data.symbol,
        content: JSON.stringify(res.data.content, null, 2)
      });
    } catch {
      message.error('Failed to load strategy');
    }
  };

  const onFinish = async (values) => {
    setLoading(true);
    try {
      const payload = {
        name: values.name,
        symbol: values.symbol,
        content: JSON.parse(values.content)
      };

      if (id) {
        await updateStrategy(id, payload);
        message.success('Strategy updated');
      } else {
        await createStrategy(payload);
        message.success('Strategy created');
      }
      navigate('/');
    } catch {
      message.error('Error saving strategy');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card title={id ? "Edit Strategy" : "New Strategy"}>
      <Form form={form} layout="vertical" onFinish={onFinish}>
        <Form.Item name="name" label="Strategy Name" rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="symbol" label="Stock Symbol" rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="content" label="Strategy JSON (DSL)" rules={[{ required: true }]}>
          <Input.TextArea rows={20} style={{ fontFamily: 'monospace' }} />
        </Form.Item>
        <Button type="primary" htmlType="submit" loading={loading}>
          Save Strategy
        </Button>
      </Form>
    </Card>
  );
};

export default StrategyEditor;
