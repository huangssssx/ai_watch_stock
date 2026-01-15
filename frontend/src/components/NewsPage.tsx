import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Button, Card, Input, InputNumber, message, Select, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import axios from 'axios';
import ReactMarkdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import remarkGfm from 'remark-gfm';
import type { AIConfig, StockNews } from '../types';
import { analyzeMarketNews, fetchMarketNews, getAIConfigs, getLatestNews } from '../api';

const { Text, Paragraph } = Typography;
const CUSTOM_PROMPT_STORAGE_KEY = 'ai_watch_stock.news.customPrompt';
const SELECTED_AI_STORAGE_KEY = 'ai_watch_stock.news.selectedAiId';
const LIMIT_STORAGE_KEY = 'ai_watch_stock.news.limit';

const formatDateTime = (value?: string | null) => {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return d.toLocaleString();
};

const readDetailFromErrorData = (data: unknown) => {
  if (typeof data !== 'object' || data === null) return undefined;
  if (!('detail' in data)) return undefined;
  const value = (data as Record<string, unknown>).detail;
  if (value === null || value === undefined) return undefined;
  return String(value);
};

const NewsPage: React.FC = () => {
  const [news, setNews] = useState<StockNews[]>([]);
  const [loadingNews, setLoadingNews] = useState(false);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);
  const [aiConfigs, setAiConfigs] = useState<AIConfig[]>([]);
  const [selectedAiId, setSelectedAiId] = useState<number | undefined>(() => {
    try {
      const raw = localStorage.getItem(SELECTED_AI_STORAGE_KEY);
      if (!raw) return undefined;
      const v = Number(raw);
      return Number.isFinite(v) ? v : undefined;
    } catch {
      return undefined;
    }
  });
  const [customPrompt, setCustomPrompt] = useState<string>(() => {
    try {
      return localStorage.getItem(CUSTOM_PROMPT_STORAGE_KEY) || '';
    } catch {
      return '';
    }
  });
  const [limit, setLimit] = useState<number>(() => {
    try {
      const raw = localStorage.getItem(LIMIT_STORAGE_KEY);
      if (!raw) return 50;
      const v = Number(raw);
      if (!Number.isFinite(v)) return 50;
      return Math.max(10, Math.min(200, v));
    } catch {
      return 50;
    }
  });
  const [analysisMarkdown, setAnalysisMarkdown] = useState<string>('');
  const [analysisAt, setAnalysisAt] = useState<string>('');

  const loadAiConfigs = useCallback(async () => {
    try {
      const res = await getAIConfigs();
      const configs = res.data || [];
      setAiConfigs(configs);
      const current = selectedAiId;
      if (current && configs.some((c) => c.id === current)) return;

      const nextId = configs.find((c) => Boolean(c.is_active))?.id ?? configs[0]?.id;
      if (!nextId) return;
      setSelectedAiId(nextId);
      try {
        localStorage.setItem(SELECTED_AI_STORAGE_KEY, String(nextId));
      } catch {
        return;
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      message.error(`加载 AI 列表失败：${msg}`);
    }
  }, [selectedAiId]);

  const loadLatest = useCallback(async () => {
    setLoadingNews(true);
    try {
      const newsRes = await getLatestNews(limit);
      setNews(newsRes.data || []);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      message.error(`加载失败：${msg}`);
    } finally {
      setLoadingNews(false);
    }
  }, [limit]);

  useEffect(() => {
    loadLatest();
    loadAiConfigs();
  }, [loadLatest, loadAiConfigs]);

  const handleFetch = async () => {
    setLoadingNews(true);
    try {
      const res = await fetchMarketNews(limit);
      message.success(`已抓取 ${res.data.count} 条新闻`);
      await loadLatest();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      message.error(`抓取失败：${msg}`);
    } finally {
      setLoadingNews(false);
    }
  };

  const handleAnalyze = async () => {
    setLoadingAnalysis(true);
    try {
      const normalizedPrompt = (customPrompt || '').trim();
      setAnalysisMarkdown('');
      setAnalysisAt('');
      const res = await analyzeMarketNews(selectedAiId, normalizedPrompt ? normalizedPrompt : undefined, limit);
      const raw = String(res.data?.raw || '').trim();
      setAnalysisMarkdown(raw);
      setAnalysisAt(new Date().toISOString());
      message.success('分析完成');
    } catch (e: unknown) {
      if (axios.isAxiosError(e)) {
        const detail = readDetailFromErrorData(e.response?.data);
        if (detail) {
          message.error(`分析失败：${detail}`);
          return;
        }
      }
      const msg = e instanceof Error ? e.message : String(e);
      message.error(`分析失败：${msg}`);
    } finally {
      setLoadingAnalysis(false);
    }
  };

  const columns: ColumnsType<StockNews> = useMemo(
    () => [
      {
        title: '时间',
        dataIndex: 'publish_time',
        key: 'publish_time',
        width: 180,
        render: (v: string) => <Text>{formatDateTime(v)}</Text>,
      },
      {
        title: '来源',
        dataIndex: 'source',
        key: 'source',
        width: 140,
        render: (v: string) => <Tag>{String(v || '-')}</Tag>,
      },
      {
        title: '标题/内容',
        key: 'content',
        render: (_: unknown, record: StockNews) => (
          <div>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>{String(record.title || '').trim() || '-'}</div>
            <Paragraph style={{ margin: 0 }} ellipsis={{ rows: 2, expandable: true, symbol: '展开' }}>
              {String(record.content || '').trim() || '-'}
            </Paragraph>
          </div>
        ),
      },
    ],
    [],
  );

  const selectedAiName = useMemo(() => {
    const id = selectedAiId;
    if (!id) return '';
    return aiConfigs.find((c) => c.id === id)?.name || '';
  }, [aiConfigs, selectedAiId]);

  return (
    <Space orientation="vertical" size={16} style={{ width: '100%' }}>
      <Card
        title="新闻 AI 分析"
        extra={
          <Space>
            <Text type="secondary">AI</Text>
            <Select
              style={{ width: 220 }}
              placeholder="选择 AI"
              value={selectedAiId}
              onChange={(v) => {
                setSelectedAiId(v);
                try {
                  localStorage.setItem(SELECTED_AI_STORAGE_KEY, String(v));
                } catch {
                  return;
                }
              }}
              options={aiConfigs.map((c) => ({
                label: `${c.name}${c.is_active ? '' : '（未启用）'}`,
                value: c.id,
              }))}
            />
            <Text type="secondary">条数</Text>
            <InputNumber
              min={10}
              max={200}
              value={limit}
              onChange={(v) => {
                const next = Math.max(10, Math.min(200, Number(v || 50)));
                setLimit(next);
                try {
                  localStorage.setItem(LIMIT_STORAGE_KEY, String(next));
                } catch {
                  return;
                }
              }}
            />
            <Button onClick={loadLatest} loading={loadingNews}>
              刷新
            </Button>
            <Button type="primary" onClick={handleFetch} loading={loadingNews}>
              抓取新闻
            </Button>
            <Button onClick={handleAnalyze} loading={loadingAnalysis}>
              AI 分析
            </Button>
          </Space>
        }
      >
        <div style={{ marginBottom: 12 }}>
          <Text type="secondary">自定义提示词（可选）</Text>
          <Input.TextArea
            value={customPrompt}
            onChange={(e) => {
              const v = e.target.value;
              setCustomPrompt(v);
              try {
                localStorage.setItem(CUSTOM_PROMPT_STORAGE_KEY, v);
              } catch {
                return;
              }
            }}
            placeholder="例如：更关注政策面与国企改革；输出时给出最值得关注的3个板块，并说明理由。"
            rows={3}
            style={{ marginTop: 8 }}
          />
        </div>
        {analysisMarkdown ? (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Space wrap>
              {selectedAiName ? (
                <>
                  <Text type="secondary">AI</Text>
                  <Tag>{selectedAiName}</Tag>
                </>
              ) : null}
              {analysisAt ? (
                <>
                  <Text type="secondary">时间</Text>
                  <Text>{formatDateTime(analysisAt)}</Text>
                </>
              ) : null}
            </Space>
            <div
              style={{
                border: '1px solid #f0f0f0',
                borderRadius: 8,
                padding: 12,
                background: '#fff',
                maxHeight: 640,
                overflow: 'auto',
              }}
            >
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                rehypePlugins={[rehypeRaw]}
                components={{
                  h1: ({ children }) => (
                    <h1 style={{ fontSize: 22, fontWeight: 700, margin: '16px 0 10px' }}>{children}</h1>
                  ),
                  h2: ({ children }) => (
                    <h2 style={{ fontSize: 18, fontWeight: 700, margin: '14px 0 8px' }}>{children}</h2>
                  ),
                  h3: ({ children }) => (
                    <h3 style={{ fontSize: 16, fontWeight: 700, margin: '12px 0 6px' }}>{children}</h3>
                  ),
                  p: ({ children }) => <p style={{ margin: '8px 0', lineHeight: 1.75 }}>{children}</p>,
                  li: ({ children }) => <li style={{ margin: '4px 0', lineHeight: 1.75 }}>{children}</li>,
                  blockquote: ({ children }) => (
                    <blockquote
                      style={{
                        margin: '10px 0',
                        padding: '8px 12px',
                        borderLeft: '4px solid #e5e7eb',
                        background: '#fafafa',
                      }}
                    >
                      {children}
                    </blockquote>
                  ),
                  code: (props) => {
                    const { children, className } = props as { children?: React.ReactNode; className?: string };
                    const isBlock = Boolean(className);
                    if (!isBlock) {
                      return (
                        <code
                          style={{
                            background: '#f6f8fa',
                            border: '1px solid #e5e7eb',
                            borderRadius: 6,
                            padding: '0 6px',
                            fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
                            fontSize: 13,
                          }}
                        >
                          {children}
                        </code>
                      );
                    }
                    return (
                      <pre
                        style={{
                          background: '#0b1020',
                          color: '#e5e7eb',
                          padding: 12,
                          borderRadius: 8,
                          overflow: 'auto',
                        }}
                      >
                        <code className={className}>{children}</code>
                      </pre>
                    );
                  },
                  table: ({ children }) => (
                    <table style={{ borderCollapse: 'collapse', width: '100%', margin: '10px 0' }}>{children}</table>
                  ),
                  th: ({ children }) => (
                    <th style={{ border: '1px solid #e5e7eb', padding: '6px 8px', background: '#fafafa' }}>
                      {children}
                    </th>
                  ),
                  td: ({ children }) => <td style={{ border: '1px solid #e5e7eb', padding: '6px 8px' }}>{children}</td>,
                }}
              >
                {analysisMarkdown}
              </ReactMarkdown>
            </div>
          </Space>
        ) : (
          <Text type="secondary">暂无分析结果，先点击“抓取新闻”，再点击“AI 分析”。</Text>
        )}
      </Card>

      <Card title="最新新闻">
        <Table
          rowKey="id"
          dataSource={news}
          columns={columns}
          loading={loadingNews}
          pagination={{ defaultPageSize: 20, showSizeChanger: true }}
        />
      </Card>
    </Space>
  );
};

export default NewsPage;
