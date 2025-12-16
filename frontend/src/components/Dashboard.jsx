import React, { useEffect, useState } from 'react';
import { Table, Button, Space, Tag, message, Modal } from 'antd';
import { getStrategies, startMonitor, stopMonitor } from '../api';
import { Link } from 'react-router-dom';
import Alerts from './Alerts';

const Dashboard = () => {
  const [strategies, setStrategies] = useState([]);
  const [selectedStrategy, setSelectedStrategy] = useState(null);

  const loadStrategies = async () => {
    try {
      const res = await getStrategies();
      setStrategies(res.data);
    } catch {
      message.error('Failed to load strategies');
    }
  };

  useEffect(() => {
    const t = setTimeout(() => {
      loadStrategies();
    }, 0);
    return () => clearTimeout(t);
  }, []);

  const handleStart = async (id) => {
    try {
      await startMonitor(id);
      message.success('Monitoring started');
      loadStrategies();
    } catch {
      message.error('Failed to start');
    }
  };

  const handleStop = async (id) => {
    try {
      await stopMonitor(id);
      message.success('Monitoring stopped');
      loadStrategies();
    } catch {
      message.error('Failed to stop');
    }
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id' },
    { title: 'Name', dataIndex: 'name', key: 'name' },
    { title: 'Symbol', dataIndex: 'symbol', key: 'symbol' },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      render: (status) => (
        <Tag color={status === 'running' ? 'green' : 'default'}>
          {status.toUpperCase()}
        </Tag>
      )
    },
    {
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space size="middle">
          {record.status === 'stopped' ? (
            <Button type="primary" onClick={() => handleStart(record.id)}>Start</Button>
          ) : (
            <Button danger onClick={() => handleStop(record.id)}>Stop</Button>
          )}
          <Link to={`/edit/${record.id}`}>Edit</Link>
          <Button onClick={() => setSelectedStrategy(record.id)}>View Alerts</Button>
          <Link to={`/chart/${record.id}`}>
            <Button type="default">K线图</Button>
          </Link>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <h2>Strategy Dashboard</h2>
        <Link to="/create">
            <Button type="primary">New Strategy</Button>
        </Link>
      </div>
      <Table columns={columns} dataSource={strategies} rowKey="id" />
      
      <Modal 
        title="Alert Log" 
        open={!!selectedStrategy} 
        onCancel={() => setSelectedStrategy(null)}
        footer={null}
        width={800}
      >
        {selectedStrategy && <Alerts strategyId={selectedStrategy} />}
      </Modal>
    </div>
  );
};

export default Dashboard;
