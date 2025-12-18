import React, { useState, useEffect } from 'react';
import { Table, Tag, Button } from 'antd';
import type { Log } from '../types';
import { getLogs } from '../api';
import { ReloadOutlined } from '@ant-design/icons';

const LogsViewer: React.FC = () => {
  const [logs, setLogs] = useState<Log[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchLogs = async () => {
    setLoading(true);
    try {
      const res = await getLogs();
      setLogs(res.data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, []);

  const columns = [
    { title: '时间', dataIndex: 'timestamp', key: 'timestamp', render: (t: string) => new Date(t).toLocaleString() },
    { title: '股票ID', dataIndex: 'stock_id', key: 'stock_id' },
    { 
      title: '级别', 
      dataIndex: 'is_alert', 
      key: 'is_alert',
      render: (isAlert: boolean) => (
        <Tag color={isAlert ? 'red' : 'green'}>{isAlert ? '预警' : '信息'}</Tag>
      )
    },
    { 
      title: '消息', 
      dataIndex: ['ai_analysis', 'message'], 
      key: 'message',
      width: 400
    },
    {
        title: 'AI 原始返回',
        dataIndex: 'ai_response',
        key: 'ai_response',
        ellipsis: true
    }
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button icon={<ReloadOutlined />} onClick={fetchLogs}>刷新</Button>
      </div>
      <Table dataSource={logs} columns={columns} rowKey="id" loading={loading} />
    </div>
  );
};

export default LogsViewer;
