import React, { useState, useEffect, useCallback } from 'react';
import { Form, Input, Button, Card, Tabs, message, InputNumber, Switch } from 'antd';
import { MailOutlined, EditOutlined, SaveOutlined, SendOutlined, BellOutlined } from '@ant-design/icons';
import { getEmailConfig, updateEmailConfig, testEmailConfig, getGlobalPrompt, updateGlobalPrompt, getAlertRateLimitConfig, updateAlertRateLimitConfig } from '../api';
import type { EmailConfig, GlobalPromptConfig, AlertRateLimitConfig } from '../types';

const Settings: React.FC = () => {
  const [activeTab, setActiveTab] = useState('email');

  return (
    <Card title="系统设置" bordered={false}>
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'email',
            label: (
              <span>
                <MailOutlined />
                邮件设置
              </span>
            ),
            children: <EmailSettings />,
          },
          {
            key: 'prompt',
            label: (
              <span>
                <EditOutlined />
                全局 Prompt
              </span>
            ),
            children: <PromptSettings />,
          },
          {
            key: 'alerts',
            label: (
              <span>
                <BellOutlined />
                告警设置
              </span>
            ),
            children: <AlertSettings />,
          },
        ]}
      />
    </Card>
  );
};

const EmailSettings: React.FC = () => {
  const [form] = Form.useForm<EmailConfig>();
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);

  const fetchConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getEmailConfig();
      form.setFieldsValue(res.data);
    } catch {
      message.error('加载邮件配置失败');
    } finally {
      setLoading(false);
    }
  }, [form]);

  useEffect(() => {
    void fetchConfig();
  }, [fetchConfig]);

  const onFinish = async (values: EmailConfig) => {
    try {
      await updateEmailConfig(values);
      message.success('邮件配置已保存');
    } catch {
      message.error('保存失败');
    }
  };

  const handleTest = async () => {
    setTesting(true);
    try {
      const values = await form.validateFields();
      const res = await testEmailConfig(values);
      if (res.data.ok) {
        message.success(res.data.message);
      } else {
        message.error(`测试发送失败: ${res.data.message}`);
      }
    } catch (e) {
      console.error(e);
      message.error('测试发送失败');
    } finally {
      setTesting(false);
    }
  };

  return (
    <div style={{ maxWidth: 600 }}>
      <Form form={form} layout="vertical" onFinish={onFinish} disabled={loading}>
        <Form.Item name="smtp_server" label="SMTP 服务器" rules={[{ required: true }]}>
          <Input placeholder="smtp.gmail.com" />
        </Form.Item>
        <Form.Item name="smtp_port" label="SMTP 端口" rules={[{ required: true }]}>
          <InputNumber style={{ width: '100%' }} placeholder="587" />
        </Form.Item>
        <Form.Item name="sender_email" label="发件人邮箱" rules={[{ required: true, type: 'email' }]}>
          <Input placeholder="sender@example.com" />
        </Form.Item>
        <Form.Item name="sender_password" label="发件人密码/授权码" rules={[{ required: true }]}>
          <Input.Password placeholder="password" />
        </Form.Item>
        <Form.Item name="receiver_email" label="收件人邮箱" rules={[{ required: true, type: 'email' }]}>
          <Input placeholder="receiver@example.com" />
        </Form.Item>

        <Form.Item>
          <div style={{ display: 'flex', gap: 16 }}>
            <Button type="primary" htmlType="submit" icon={<SaveOutlined />} loading={loading}>
              保存配置
            </Button>
            <Button onClick={handleTest} loading={testing} icon={<SendOutlined />}>
              发送测试邮件
            </Button>
          </div>
        </Form.Item>
      </Form>
    </div>
  );
};

const PromptSettings: React.FC = () => {
  const [form] = Form.useForm<GlobalPromptConfig>();
  const [loading, setLoading] = useState(false);

  const fetchConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getGlobalPrompt();
      form.setFieldsValue(res.data);
    } catch {
      message.error('加载 Prompt 配置失败');
    } finally {
      setLoading(false);
    }
  }, [form]);

  useEffect(() => {
    void fetchConfig();
  }, [fetchConfig]);

  const onFinish = async (values: GlobalPromptConfig) => {
    try {
      await updateGlobalPrompt(values);
      message.success('全局 Prompt 已保存');
    } catch {
      message.error('保存失败');
    }
  };

  return (
    <div style={{ maxWidth: 800 }}>
      <p style={{ color: '#888', marginBottom: 16 }}>
        当单只股票未配置独立的 Prompt Template 时，系统将使用此全局 Prompt。
        支持的占位符将由数据获取模块决定（通常包含在指标数据中）。
      </p>
      <Form form={form} layout="vertical" onFinish={onFinish} disabled={loading}>
        <Form.Item name="account_info" label="账户信息（可选）">
          <Input.TextArea
            autoSize={{ minRows: 4, maxRows: 10 }}
            placeholder="例如：总资金、当前持仓、成本价、风险偏好、是否允许开新仓等"
          />
        </Form.Item>
        <Form.Item name="prompt_template" label="Prompt Template" rules={[{ required: true }]}>
          <Input.TextArea 
            autoSize={{ minRows: 10, maxRows: 20 }} 
            placeholder="Enter global prompt template here..." 
          />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit" icon={<SaveOutlined />} loading={loading}>
            保存配置
          </Button>
        </Form.Item>
      </Form>
    </div>
  );
};

export default Settings;

const AlertSettings: React.FC = () => {
  const [form] = Form.useForm<AlertRateLimitConfig>();
  const [loading, setLoading] = useState(false);

  const fetchConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getAlertRateLimitConfig();
      form.setFieldsValue(res.data);
    } catch {
      message.error('加载告警配置失败');
    } finally {
      setLoading(false);
    }
  }, [form]);

  useEffect(() => {
    void fetchConfig();
  }, [fetchConfig]);

  const onFinish = async (values: AlertRateLimitConfig) => {
    try {
      const payload: AlertRateLimitConfig = {
        enabled: Boolean(values.enabled),
        max_per_hour_per_stock: Number(values.max_per_hour_per_stock || 0),
      };
      await updateAlertRateLimitConfig(payload);
      message.success('告警配置已保存');
    } catch {
      message.error('保存失败');
    }
  };

  const enabled = Form.useWatch('enabled', form);

  return (
    <div style={{ maxWidth: 600 }}>
      <p style={{ color: '#888', marginBottom: 16 }}>
        该限流只影响同一只股票的邮件发送频率。类型为 warning 或信号为 STRONG_* 的提醒默认不受限流影响。
      </p>
      <Form form={form} layout="vertical" onFinish={onFinish} initialValues={{ enabled: false, max_per_hour_per_stock: 0 }}>
        <Form.Item name="enabled" label="启用限流" valuePropName="checked">
          <Switch checkedChildren="开启" unCheckedChildren="关闭" loading={loading} />
        </Form.Item>

        <Form.Item
          name="max_per_hour_per_stock"
          label="每股每小时最多邮件数"
          rules={[
            {
              validator: async (_, value) => {
                const num = Number(value || 0);
                if (!enabled) return;
                if (!Number.isFinite(num) || num <= 0) throw new Error('开启限流时需填写大于 0 的数字');
              },
            },
          ]}
        >
          <InputNumber min={0} max={120} style={{ width: '100%' }} disabled={!enabled} />
        </Form.Item>

        <Form.Item>
          <Button type="primary" htmlType="submit" icon={<SaveOutlined />}>
            保存配置
          </Button>
        </Form.Item>
      </Form>
    </div>
  );
};
