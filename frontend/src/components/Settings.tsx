import React, { useState, useEffect } from 'react';
import { Form, Input, Button, Card, Tabs, message, InputNumber, Divider } from 'antd';
import { MailOutlined, EditOutlined, SaveOutlined, SendOutlined } from '@ant-design/icons';
import { getEmailConfig, updateEmailConfig, testEmailConfig, getGlobalPrompt, updateGlobalPrompt } from '../api';
import type { EmailConfig, GlobalPromptConfig } from '../types';

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
        ]}
      />
    </Card>
  );
};

const EmailSettings: React.FC = () => {
  const [form] = Form.useForm<EmailConfig>();
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);

  const fetchConfig = async () => {
    setLoading(true);
    try {
      const res = await getEmailConfig();
      form.setFieldsValue(res.data);
    } catch {
      message.error('加载邮件配置失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void fetchConfig();
  }, []);

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
      <Form form={form} layout="vertical" onFinish={onFinish}>
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
            <Button type="primary" htmlType="submit" icon={<SaveOutlined />}>
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

  const fetchConfig = async () => {
    setLoading(true);
    try {
      const res = await getGlobalPrompt();
      form.setFieldsValue(res.data);
    } catch {
      message.error('加载 Prompt 配置失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void fetchConfig();
  }, []);

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
      <Form form={form} layout="vertical" onFinish={onFinish}>
        <Form.Item name="prompt_template" label="Prompt Template" rules={[{ required: true }]}>
          <Input.TextArea 
            autoSize={{ minRows: 10, maxRows: 20 }} 
            placeholder="Enter global prompt template here..." 
          />
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

export default Settings;
