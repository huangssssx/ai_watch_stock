/**
 * 优化的日志查看器组件
 * 使用虚拟滚动和性能优化技术来处理大量日志数据
 */

import React, { useState, useEffect, useMemo, useCallback, memo } from 'react';
import { Table, Tag, Button, Space, Input, message, Switch, Collapse, Modal } from 'antd';
import { ReloadOutlined, DeleteOutlined, SearchOutlined, CopyOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { Log } from '../../types';
import { getLogs, clearLogs } from '../../api';

// ==================== 常量定义 ====================
const GROUP_BY_STOCK_STORAGE_KEY = 'ai_watch_stock.logsViewer.groupByStock';
const AI_REQUEST_PAYLOAD_MARKER = 'AI Request Payload:\n';
const MAX_CONTENT_LENGTH = 5000; // 内容截断长度
const AUTO_REFRESH_INTERVAL = 10000; // 自动刷新间隔

// ==================== 类型定义 ====================

interface GroupedLogs {
  [key: string]: Log[];
}

// ==================== 工具函数 ====================

const parseAiRequestPayloadFromRawData = (text: string) => {
  const raw = text ?? '';
  const idx = raw.indexOf(AI_REQUEST_PAYLOAD_MARKER);
  if (idx < 0) return { ok: false as const, preamble: raw, payload: null };

  const preamble = raw.slice(0, idx).trimEnd();
  const payloadText = raw.slice(idx + AI_REQUEST_PAYLOAD_MARKER.length).trim();
  try {
    const payload = JSON.parse(payloadText) as unknown;
    return { ok: true as const, preamble, payload };
  } catch {
    return { ok: false as const, preamble: raw, payload: null };
  }
};

// ==================== Memoized 子组件 ====================

const RenderRawData = memo(({ text }: { text: string }) => {
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

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {parsed.preamble && (
        <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0 }}>{parsed.preamble}</pre>
      )}

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

RenderRawData.displayName = 'RenderRawData';

interface Props {
  stockId?: number;
}

// ==================== 主组件 ====================

const LogsViewerOptimized: React.FC<Props> = ({ stockId }) => {
  // ==================== 状态 ====================
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

  // ==================== 数据获取 ====================

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getLogs();
      let data = res.data;
      if (stockId) {
        data = data.filter(l => l.stock_id === stockId);
      }
      setLogs(data);
      setSelectedRowKeys(keys => keys.filter(k => data.find(l => l.id === k)));
    } catch {
      message.error('加载日志失败');
    } finally {
      setLoading(false);
    }
  }, [stockId]);

  // ==================== 数据处理（使用useMemo优化） ====================

  const filteredLogs = useMemo(() => {
    if (!searchText) return logs;
    const lower = searchText.toLowerCase();
    return logs.filter(log => {
      return (
        log.stock?.name.toLowerCase().includes(lower) ||
        log.stock?.symbol.toLowerCase().includes(lower) ||
        log.ai_analysis.message.toLowerCase().includes(lower)
      );
    });
  }, [logs, searchText]);

  const groupedLogs = useMemo(() => {
    if (!groupByStock) return null;
    const groups: GroupedLogs = {};
    for (const log of filteredLogs) {
      const key = log.stock ? `${log.stock.name} (${log.stock.symbol})` : '未分类';
      if (!groups[key]) groups[key] = [];
      groups[key].push(log);
    }
    return groups;
  }, [filteredLogs, groupByStock]);

  const stockFilters = useMemo(() => {
    return Array.from(new Set(logs.map(l => l.stock ? `${l.stock.name}|${l.stock.symbol}` : '')))
      .filter(Boolean)
      .map(s => {
        const [name, symbol] = s.split('|');
        return { text: `${name} (${symbol})`, value: symbol };
      });
  }, [logs]);

  // ==================== 事件处理 ====================

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

  const handleCopyLog = useCallback((log: Log) => {
    const content = JSON.stringify(log, null, 2);
    navigator.clipboard.writeText(content).then(() => {
      message.success('日志信息已复制');
    });
  }, []);

  // ==================== 表格列定义（使用useMemo优化） ====================

  const columns: ColumnsType<Log> = useMemo(() => [
    {
      title: '时间',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 180,
      render: (t: string) => {
        const dateStr = t.endsWith('Z') ? t : `${t}Z`;
        return new Date(dateStr).toLocaleString();
      },
      sorter: (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
      defaultSortOrder: 'descend' as const,
    },
    {
      title: '股票',
      key: 'stock',
      width: 180,
      render: (_: unknown, record: Log) => (
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
      render: (_: unknown, record: Log) => {
        const signal = record.ai_analysis?.signal;
        let color = 'default';
        let text = 'WAIT';

        switch (signal) {
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
      title: '操作',
      key: 'action',
      width: 80,
      render: (_: unknown, record: Log) => (
        <Button
          icon={<CopyOutlined />}
          size="small"
          onClick={() => handleCopyLog(record)}
          title="复制本条完整日志 JSON"
        />
      )
    }
  ], [stockFilters, handleCopyLog]);

  // ==================== 副作用 ====================

  useEffect(() => {
    try {
      localStorage.setItem(GROUP_BY_STOCK_STORAGE_KEY, JSON.stringify(groupByStock));
    } catch {
      return;
    }
  }, [groupByStock]);

  useEffect(() => {
    fetchLogs();
    const interval = setInterval(fetchLogs, AUTO_REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchLogs]);

  // ==================== 渲染 ====================

  const rowSelection = {
    selectedRowKeys,
    onChange: (newSelectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(newSelectedRowKeys);
    },
  };

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
        destroyOnClose
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
            <Button danger icon={<DeleteOutlined />} onClick={() => handleClear(selectedRowKeys as number[])}>
              删除选中 ({selectedRowKeys.length})
            </Button>
          )}
          <Button danger type="primary" icon={<DeleteOutlined />} onClick={() => handleClear()}>
            清空所有
          </Button>
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

export default LogsViewerOptimized;
