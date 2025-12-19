import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Table, Tag, Button, Tooltip, Space, Input, message, Popconfirm, Switch, Collapse } from 'antd';
import type { Log } from '../types';
import { getLogs, clearLogs } from '../api';
import { ReloadOutlined, DeleteOutlined, SearchOutlined, CopyOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

interface Props {
  stockId?: number;
}

const LogsViewer: React.FC<Props> = ({ stockId }) => {
  const [logs, setLogs] = useState<Log[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [groupByStock, setGroupByStock] = useState(false);

  const fetchLogs = useCallback(async () => {
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
  }, [stockId]);

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
  }, [fetchLogs]);

  const filteredLogs = logs.filter(log => {
    if (!searchText) return true;
    const lower = searchText.toLowerCase();
    return (
      log.stock?.name.toLowerCase().includes(lower) ||
      log.stock?.symbol.toLowerCase().includes(lower) ||
      log.ai_analysis.message.toLowerCase().includes(lower)
    );
  });

  const groupedLogs = useMemo(() => {
    if (!groupByStock) return null;
    const groups: Record<string, Log[]> = {};
    filteredLogs.forEach(log => {
      const key = log.stock ? `${log.stock.name} (${log.stock.symbol})` : '未分类';
      if (!groups[key]) groups[key] = [];
      groups[key].push(log);
    });
    return groups;
  }, [filteredLogs, groupByStock]);

  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
  };

  const handleCopyLog = (log: Log) => {
    const content = JSON.stringify(log, null, 2);
    navigator.clipboard.writeText(content).then(() => {
      message.success('日志信息已复制');
    });
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
      title: '信号', 
      key: 'signal',
      width: 100,
      render: (_, record) => {
        const signal = record.ai_analysis?.signal;
        let color = 'default';
        let text = 'WAIT';
        
        switch(signal) {
          case 'STRONG_BUY': color = '#f50'; text = '强力买入'; break;
          case 'BUY': color = '#faad14'; text = '买入'; break;
          case 'SELL': color = '#52c41a'; text = '卖出'; break;
          case 'STRONG_SELL': color = '#135200'; text = '强力卖出'; break;
          case 'WAIT': color = '#8c8c8c'; text = '观望'; break;
          default: color = '#8c8c8c'; text = signal || '未知';
        }
        
        return <Tag color={color} style={{ fontWeight: 'bold' }}>{text}</Tag>;
      }
    },
    { 
      title: '建议 & 仓位', 
      key: 'advice',
      width: 250,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <div style={{ fontWeight: 500 }}>{record.ai_analysis?.action_advice || '-'}</div>
          <div style={{ fontSize: '12px', color: '#666' }}>
            仓位: {record.ai_analysis?.suggested_position || '-'} | 
            持仓: {record.ai_analysis?.duration || '-'}
          </div>
          {record.ai_analysis?.stop_loss_price && (
            <div style={{ fontSize: '12px', color: '#ff4d4f' }}>
              止损: {record.ai_analysis.stop_loss_price}
            </div>
          )}
        </Space>
      )
    },
    { 
      title: '消息', 
      dataIndex: ['ai_analysis', 'message'], 
      key: 'message',
      width: 200,
      render: (text) => (
        <Tooltip title={text} placement="topLeft" overlayStyle={{ maxWidth: 500 }}>
          <div style={{
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            wordBreak: 'break-word', // 允许长单词换行，但不强制截断
            whiteSpace: 'normal',    // 允许换行
            cursor: 'help'
          }}>
            {text}
          </div>
        </Tooltip>
      )
    },
    {
      title: 'AI 原始返回',
      dataIndex: 'ai_response',
      key: 'ai_response',
      width: 200,
      render: (text) => (
        <Tooltip title={<div style={{ whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto' }}>{text}</div>} placement="topLeft" overlayStyle={{ maxWidth: 600 }}>
          <div style={{
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            wordBreak: 'break-all', // JSON 字符串通常需要 break-all
            cursor: 'help',
            fontFamily: 'monospace',
            color: '#666'
          }}>
            {text}
          </div>
        </Tooltip>
      )
    },
    {
      title: '发送内容',
      dataIndex: 'raw_data',
      key: 'raw_data',
      width: 200,
      render: (text) => (
        <Tooltip title={<div style={{ whiteSpace: 'pre-wrap', maxHeight: 400, overflow: 'auto' }}>{text}</div>} placement="topLeft" overlayStyle={{ maxWidth: 600 }}>
           <div style={{
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            wordBreak: 'break-all', // 包含 JSON 或长字符串，需要 break-all
            cursor: 'help',
            fontFamily: 'monospace',
            color: '#666'
          }}>
            {text}
          </div>
        </Tooltip>
      )
    },
    {
      title: '操作',
      key: 'action',
      width: 80,
      render: (_, record) => (
        <Button 
          icon={<CopyOutlined />} 
          size="small" 
          onClick={() => handleCopyLog(record)}
          title="复制本条完整日志 JSON"
        />
      )
    }
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Space>
          <Switch 
            checked={groupByStock} 
            onChange={setGroupByStock} 
            checkedChildren="分组" 
            unCheckedChildren="列表" 
          />
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
      
      {groupByStock && groupedLogs ? (
        <Collapse 
          defaultActiveKey={Object.keys(groupedLogs)} 
          items={Object.entries(groupedLogs).map(([key, groupLogs]) => ({
            key,
            label: `${key} (${groupLogs.length})`,
            children: (
              <Table 
                dataSource={groupLogs} 
                columns={columns.filter(c => c.key !== 'stock')} 
                rowKey="id" 
                loading={loading}
                pagination={{ defaultPageSize: 10, showSizeChanger: true }}
                rowSelection={{
                  selectedRowKeys,
                  onChange: (newKeys) => {
                    const groupIds = groupLogs.map(l => l.id);
                    const otherKeys = selectedRowKeys.filter(k => !groupIds.includes(k as number));
                    setSelectedRowKeys([...otherKeys, ...newKeys]);
                  }
                }}
                scroll={{ x: 'max-content' }}
              />
            )
          }))}
        />
      ) : (
        <Table 
          dataSource={filteredLogs} 
          columns={columns} 
          rowKey="id" 
          loading={loading}
          pagination={{ defaultPageSize: 20, showSizeChanger: true }}
          rowSelection={rowSelection}
          scroll={{ x: 'max-content' }}
        />
      )}
    </div>
  );
};

export default LogsViewer;
