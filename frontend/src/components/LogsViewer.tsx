import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Table, Tag, Button, Tooltip, Space, Input, message, Popconfirm, Switch, Collapse, Modal } from 'antd';
import type { Log } from '../types';
import { getLogs, clearLogs } from '../api';
import { ReloadOutlined, DeleteOutlined, SearchOutlined, CopyOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

interface Props {
  stockId?: number;
}

const GROUP_BY_STOCK_STORAGE_KEY = 'ai_watch_stock.logsViewer.groupByStock';
const AI_REQUEST_PAYLOAD_MARKER = 'AI Request Payload:\n';

const parseAiRequestPayloadFromRawData = (text: string) => {
  const raw = text ?? '';
  const idx = raw.indexOf(AI_REQUEST_PAYLOAD_MARKER);
  if (idx < 0) return { ok: false as const, preamble: raw, payload: null as unknown };

  const preamble = raw.slice(0, idx).trimEnd();
  const payloadText = raw.slice(idx + AI_REQUEST_PAYLOAD_MARKER.length).trim();
  try {
    const payload = JSON.parse(payloadText) as unknown;
    return { ok: true as const, preamble, payload };
  } catch {
    return { ok: false as const, preamble: raw, payload: null as unknown };
  }
};

const RenderRawData: React.FC<{ text: string }> = React.memo(({ text }) => {
  const parsed = useMemo(() => parseAiRequestPayloadFromRawData(text), [text]);

  if (!parsed.ok) {
    return <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>{text}</pre>;
  }

  const payload = parsed.payload as Partial<{
    model: string;
    temperature: number;
    response_format: { type?: string };
    messages: { role?: string; content?: string }[];
  }>;

  const messages = Array.isArray(payload.messages) ? payload.messages : [];

  // Limit content display to avoid massive DOM rendering
  const MAX_CONTENT_LENGTH = 5000;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {parsed.preamble ? (
        <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>{parsed.preamble}</pre>
      ) : null}

      <div style={{ fontFamily: 'monospace', color: '#374151' }}>
        <div>model: {payload.model ?? '-'}</div>
        <div>temperature: {payload.temperature ?? '-'}</div>
        <div>response_format: {payload.response_format?.type ?? '-'}</div>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {messages.map((m, i) => {
          const content = m.content ?? '';
          const shouldTruncate = content.length > MAX_CONTENT_LENGTH;
          const displayContent = shouldTruncate ? content.slice(0, MAX_CONTENT_LENGTH) : content;

          return (
            <div
              key={`${m.role ?? 'unknown'}-${i}`}
              style={{
                border: '1px solid #E5E7EB',
                borderRadius: 8,
                overflow: 'hidden',
              }}
            >
              <div
                style={{
                  padding: '6px 10px',
                  background: '#F9FAFB',
                  borderBottom: '1px solid #E5E7EB',
                  fontFamily: 'monospace',
                  fontWeight: 700,
                  color: '#111827',
                }}
              >
                {m.role ?? 'message'} {shouldTruncate ? `(截取前 ${MAX_CONTENT_LENGTH} 字符，共 ${content.length} 字符)` : null}
              </div>
              <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0, padding: 10 }}>
                {displayContent}
              </pre>
            </div>
          );
        })}
      </div>
    </div>
  );
});

const LogsViewer: React.FC<Props> = ({ stockId }) => {
  const [logs, setLogs] = useState<Log[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [detailModal, setDetailModal] = useState<{ open: boolean; title: string; content: string }>({
    open: false,
    title: '',
    content: '',
  });
  const [groupByStock, setGroupByStock] = useState(() => {
    try {
      const raw = localStorage.getItem(GROUP_BY_STOCK_STORAGE_KEY);
      if (raw == null) return false;
      const parsed = JSON.parse(raw);
      return Boolean(parsed);
    } catch {
      return false;
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem(GROUP_BY_STOCK_STORAGE_KEY, JSON.stringify(groupByStock));
    } catch {
      return;
    }
  }, [groupByStock]);

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

  const handleClear = useCallback(async (ids?: number[]) => {
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
  }, [fetchLogs]);

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

  const handleCopyLog = useCallback((log: Log) => {
    const content = JSON.stringify(log, null, 2);
    navigator.clipboard.writeText(content).then(() => {
      message.success('日志信息已复制');
    });
  }, []);

  const openDetailModal = useCallback((title: string, content: string) => {
    setDetailModal({ open: true, title, content: content ?? '' });
  }, []);

  // Pre-compute stock filters to avoid recalculating on every render
  const stockFilters = useMemo(() => {
    return Array.from(new Set(logs.map(l => l.stock ? `${l.stock.name}|${l.stock.symbol}` : '')))
      .filter(Boolean)
      .map(s => {
        const [name, symbol] = s.split('|');
        return { text: `${name} (${symbol})`, value: symbol };
      });
  }, [logs]);

  const columns: ColumnsType<Log> = useMemo(() => [
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
      filters: stockFilters,
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
            wordBreak: 'break-word',
            whiteSpace: 'normal',
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
      render: (text, record) => {
        const previewText = text?.length > 100 ? text.slice(0, 100) + '...' : (text ?? '');
        return (
          <Space direction="vertical" size={0}>
            <Button type="link" size="small" onClick={() => openDetailModal(`AI 原始返回：${record.stock?.symbol ?? record.stock_id}`, text)}>
              查看完整
            </Button>
            <div style={{
              fontFamily: 'monospace',
              color: '#666',
              fontSize: '12px',
            }}>
              {previewText}
            </div>
          </Space>
        );
      },
    },
    {
      title: '发送内容',
      dataIndex: 'raw_data',
      key: 'raw_data',
      width: 200,
      render: (text, record) => {
        const previewText = text?.length > 100 ? text.slice(0, 100) + '...' : (text ?? '');
        return (
          <Space direction="vertical" size={0}>
            <Button type="link" size="small" onClick={() => openDetailModal(`发送内容：${record.stock?.symbol ?? record.stock_id}`, text)}>
              查看完整
            </Button>
            <div
              style={{
                fontFamily: 'monospace',
                color: '#666',
                fontSize: '12px',
              }}
            >
              {previewText}
            </div>
          </Space>
        );
      },
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
  ], [stockFilters, openDetailModal, handleCopyLog]);

  return (
    <div>
      <Modal
        title={detailModal.title}
        open={detailModal.open}
        onCancel={() => setDetailModal((s) => ({ ...s, open: false }))}
        footer={null}
        width="95%"
        style={{ top: 20 }}
        styles={{ body: { height: 'calc(100vh - 200px)', overflow: 'auto' } }}
        destroyOnHidden={true}
      >
        {detailModal.open && <RenderRawData text={detailModal.content} />}
      </Modal>
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
          defaultActiveKey={undefined}
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
