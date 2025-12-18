import React, { useState, useEffect } from 'react';
import { Table, Tag, Button, Tooltip, Space, Input, message, Popconfirm } from 'antd';
import type { Log } from '../types';
import { getLogs, clearLogs } from '../api';
import { ReloadOutlined, DeleteOutlined, SearchOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

interface Props {
  stockId?: number;
}

const LogsViewer: React.FC<Props> = ({ stockId }) => {
  const [logs, setLogs] = useState<Log[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);

  const fetchLogs = async () => {
    setLoading(true);
    try {
      const res = await getLogs();
      let data = res.data;
      if (stockId) {
        data = data.filter(l => l.stock_id === stockId);
      }
      setLogs(data);
      // Clear selection after refresh if items are gone
      setSelectedRowKeys(keys => keys.filter(k => data.find(l => l.id === k)));
    } finally {
      setLoading(false);
    }
  };

  const handleClear = async (ids?: number[]) => {
    try {
      await clearLogs(ids);
      message.success(ids ? '选中日志已删除' : '所有日志已清空');
      if (ids) {
        setSelectedRowKeys([]);
      }
      fetchLogs();
    } catch {
      message.error('操作失败');
    }
  };

  useEffect(() => {
    fetchLogs();
    const interval = setInterval(fetchLogs, 10000);
    return () => clearInterval(interval);
  }, [stockId]);

  const filteredLogs = logs.filter(log => {
    if (!searchText) return true;
    const lower = searchText.toLowerCase();
    return (
      log.stock?.name.toLowerCase().includes(lower) ||
      log.stock?.symbol.toLowerCase().includes(lower) ||
      log.ai_analysis.message.toLowerCase().includes(lower)
    );
  });

  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
  };

  const columns: ColumnsType<Log> = [
    { 
      title: '时间', 
      dataIndex: 'timestamp', 
      key: 'timestamp', 
      width: 180,
      render: (t: string) => {
        // Backend returns UTC time without 'Z' (e.g. "2023-10-01T10:00:00")
        // We append 'Z' to treat it as UTC, then convert to local time
        const dateStr = t.endsWith('Z') ? t : `${t}Z`;
        return new Date(dateStr).toLocaleString();
      },
      sorter: (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
      defaultSortOrder: 'descend',
    },
    { 
      title: '股票', 
      key: 'stock',
      width: 180,
      render: (_, record) => (
        record.stock ? (
          <span>{record.stock.name} <span style={{ color: '#999', fontSize: '12px' }}>({record.stock.symbol})</span></span>
        ) : (
          <span style={{ color: '#ccc' }}>ID: {record.stock_id}</span>
        )
      ),
      filters: Array.from(new Set(logs.map(l => l.stock ? `${l.stock.name}|${l.stock.symbol}` : ''))).filter(Boolean).map(s => {
        const [name, symbol] = s.split('|');
        return { text: `${name} (${symbol})`, value: symbol };
      }),
      onFilter: (value, record) => record.stock?.symbol === value,
    },
    { 
      title: '级别', 
      dataIndex: 'is_alert', 
      key: 'is_alert',
      width: 80,
      render: (isAlert: boolean) => (
        <Tag color={isAlert ? 'red' : 'green'}>{isAlert ? '预警' : '信息'}</Tag>
      ),
      filters: [
        { text: '预警', value: true },
        { text: '信息', value: false },
      ],
      onFilter: (value, record) => record.is_alert === value,
    },
    { 
      title: '消息', 
      dataIndex: ['ai_analysis', 'message'], 
      key: 'message',
      width: 300,
      ellipsis: {
        showTitle: false,
      },
      render: (text) => (
        <Tooltip title={text} placement="topLeft" overlayStyle={{ maxWidth: 500 }}>
          <span>{text}</span>
        </Tooltip>
      )
    },
    {
      title: 'AI 原始返回',
      dataIndex: 'ai_response',
      key: 'ai_response',
      ellipsis: {
        showTitle: false,
      },
      render: (text) => (
        <Tooltip title={<div style={{ whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto' }}>{text}</div>} placement="topLeft" overlayStyle={{ maxWidth: 600 }}>
          <span style={{ fontFamily: 'monospace', color: '#666' }}>{text}</span>
        </Tooltip>
      )
    },
    {
      title: '发送内容',
      dataIndex: 'raw_data',
      key: 'raw_data',
      ellipsis: {
        showTitle: false,
      },
      render: (text) => (
        <Tooltip title={<div style={{ whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto' }}>{text}</div>} placement="topLeft" overlayStyle={{ maxWidth: 600 }}>
          <span style={{ fontFamily: 'monospace', color: '#666', cursor: 'pointer' }}>查看内容</span>
        </Tooltip>
      )
    }
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={fetchLogs}>刷新</Button>
          <Input 
            placeholder="搜索股票名称/代码/消息..." 
            prefix={<SearchOutlined />} 
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            style={{ width: 300 }}
          />
        </Space>
        <Space>
          {selectedRowKeys.length > 0 && (
            <Popconfirm 
              title={`确定要删除选中的 ${selectedRowKeys.length} 条日志吗？`} 
              onConfirm={() => handleClear(selectedRowKeys as number[])}
            >
              <Button danger icon={<DeleteOutlined />}>删除选中</Button>
            </Popconfirm>
          )}
          <Popconfirm title="确定要清空所有日志吗？" onConfirm={() => handleClear()}>
            <Button danger type="primary" icon={<DeleteOutlined />}>清空所有</Button>
          </Popconfirm>
        </Space>
      </div>
      <Table 
        dataSource={filteredLogs} 
        columns={columns} 
        rowKey="id" 
        loading={loading}
        pagination={{ defaultPageSize: 20, showSizeChanger: true }}
        rowSelection={rowSelection}
      />
    </div>
  );
};

export default LogsViewer;
