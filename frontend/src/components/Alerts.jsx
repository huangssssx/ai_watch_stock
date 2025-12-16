import React, { useEffect, useState } from 'react';
import { Table, Tag, Typography } from 'antd';
import { getAlerts } from '../api';
import { useNavigate } from 'react-router-dom';

const Alerts = ({ strategyId }) => {
  const [alerts, setAlerts] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    if (strategyId) {
      loadAlerts();
      const interval = setInterval(loadAlerts, 5000);
      return () => clearInterval(interval);
    }
  }, [strategyId]);

  const loadAlerts = async () => {
    try {
      const res = await getAlerts(strategyId);
      setAlerts(res.data);
    } catch (error) {
      console.error(error);
    }
  };

  const columns = [
    {
      title: '时间',
      dataIndex: 'timestamp',
      key: 'timestamp',
      render: (t) => (
        <Typography.Text type="secondary">
          {t ? new Date(t).toLocaleString() : ''}
        </Typography.Text>
      ),
      width: 220,
    },
    {
      title: '级别',
      dataIndex: 'level',
      key: 'level',
      render: (level) => <Tag color={level === 'WARNING' ? 'red' : 'default'}>{level}</Tag>,
      width: 120,
    },
    {
      title: '消息',
      dataIndex: 'message',
      key: 'message',
    },
  ];

  return (
    <Table
      columns={columns}
      dataSource={alerts}
      rowKey="id"
      pagination={false}
      onRow={(record) => ({
        onClick: () => navigate(`/chart/${strategyId}?alertId=${record.id}`)
      })}
    />
  );
};

export default Alerts;
