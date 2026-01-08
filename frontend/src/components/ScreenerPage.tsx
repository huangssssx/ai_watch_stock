import React, { useState, useEffect } from 'react';
import { Layout, List, Button, Input, Switch, Card, Table, Tabs, message, Modal, Space, Tag } from 'antd';
import { PlusOutlined, PlayCircleOutlined, SaveOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import axios from 'axios';
import { api } from '../api';
import type { TableProps } from 'antd';

const { Content, Sider } = Layout;
const { TabPane } = Tabs;

interface Screener {
  id: number;
  name: string;
  description: string;
  script_content: string;
  cron_expression: string;
  is_active: boolean;
  last_run_at: string;
  last_run_status: string;
  last_run_log?: string;
}

interface ScreenerResult {
  id: number;
  run_at: string;
  result_json: string;
  count: number;
}

type ScreenerRow = Record<string, unknown>;

const readFirstString = (row: ScreenerRow, keys: string[]) => {
  for (const key of keys) {
    const value = row[key];
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return undefined;
};

const readDetailFromErrorData = (data: unknown) => {
  if (typeof data !== 'object' || data === null) return undefined;
  if (!('detail' in data)) return undefined;
  const value = (data as Record<string, unknown>).detail;
  if (value === null || value === undefined) return undefined;
  return String(value);
};

const DEFAULT_SCRIPT = `# Write python code here.
# Variables available: ak (akshare), pd (pandas), np (numpy)
# Must define df (DataFrame) or result (list of dicts) as output.

import akshare as ak

# Example: Get A-share spot data
df = ak.stock_zh_a_spot_em()

# Filter P/E < 20
# df = df[df["市盈率-动态"] < 20]

# Keep top 10
df = df.head(10)
`;

const ScreenerPage: React.FC = () => {
  const [screeners, setScreeners] = useState<Screener[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [editingScreener, setEditingScreener] = useState<Partial<Screener>>({});
  const [results, setResults] = useState<ScreenerResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  
  const fetchScreeners = async () => {
    try {
        const res = await api.get<Screener[]>('/screeners/');
        setScreeners(res.data);
    } catch (err) {
        console.error(err);
        message.error("Failed to load screeners");
    }
  };

  useEffect(() => {
    fetchScreeners();
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => {
      void fetchScreeners();
    }, 15000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (selectedId) {
        const s = screeners.find(x => x.id === selectedId);
        if (s) {
            setEditingScreener({ ...s });
            fetchResults(selectedId);
        }
    } else {
        setEditingScreener({});
        setResults([]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId]);

  const fetchResults = async (id: number) => {
      try {
          const res = await api.get<ScreenerResult[]>(`/screeners/${id}/results`);
          setResults(res.data);
      } catch (err) {
          console.error(err);
      }
  };

  const handleSave = async () => {
      if (!editingScreener.name) return message.error("Name is required");
      
      setLoading(true);
      try {
          const payload = {
              name: editingScreener.name,
              description: editingScreener.description,
              script_content: editingScreener.script_content,
              cron_expression: editingScreener.cron_expression,
              is_active: editingScreener.is_active
          };
          
          if (selectedId) {
              await api.put(`/screeners/${selectedId}`, payload);
               message.success(`Successfully`);
          } else {
              const createdRes = await api.post<Screener>('/screeners/', payload);
              const created = createdRes.data;
              setSelectedId(created.id);
              setEditingScreener(created);
          }
          
      } catch (e) {
          console.error(e);
          message.error("Failed to save");
      } finally {
          fetchScreeners();
          if (selectedId) fetchResults(selectedId);
          setLoading(false);
      }
  };

  const handleRun = async () => {
      if (!selectedId) return;
      setRunning(true);
      try {
          const res = await api.post(`/screeners/${selectedId}/run`);
          const data = res.data as { success: boolean; log: string; count: number };
          if (data.success) {
              message.success(`Run success, found ${data.count} stocks`);
              fetchResults(selectedId);
              fetchScreeners();
          } else {
              message.error("Run failed");
              Modal.error({ title: "Error Log", content: <pre style={{maxHeight: 400, overflow: 'auto'}}>{data.log}</pre> });
              fetchScreeners();
          }
      } catch (e) {
          console.error(e);
          message.error("Run failed");
      } finally {
          setRunning(false);
      }
  };

  const handleDelete = async () => {
      if (!selectedId) return;
      if (!confirm("Delete this screener?")) return;
      await api.delete(`/screeners/${selectedId}`);
      setSelectedId(null);
      fetchScreeners();
  };

  const addToWatchlist = async (record: ScreenerRow) => {
      const symbol = readFirstString(record, ['symbol', '代码', '股票代码']);
      const name = readFirstString(record, ['name', '名称', '股票名称']) ?? "";
      
      if (!symbol) return message.error("No symbol found in record");
      
      try {
          await api.post('/stocks/', { symbol: String(symbol), name: String(name), is_monitoring: true });
          message.success(`Added ${symbol} to watchlist`);
      } catch (err) {
          console.error(err);
          if (axios.isAxiosError(err)) {
            const detail = readDetailFromErrorData(err.response?.data);
            if (detail) return message.warning(detail);
          }
          message.error("Failed to add to watchlist");
      }
  };

  const renderResultTable = () => {
      const lastStatus = editingScreener.last_run_status;
      const lastLog = editingScreener.last_run_log;
      const lastRunAt = editingScreener.last_run_at;

      if (results.length === 0 && lastStatus === 'failed') {
          return (
              <div>
                  <p style={{ color: 'red' }}>Last run failed, no results available.</p>
                  {lastRunAt && <p>Failed at: {new Date(lastRunAt).toLocaleString()}</p>}
                  {lastLog && (
                      <pre style={{ maxHeight: 300, overflow: 'auto', background: '#f8f8f8', padding: 8 }}>
                          {lastLog}
                      </pre>
                  )}
              </div>
          );
      }

      if (results.length === 0) return <p>No results yet</p>;
      const latest = results[0];
      let data: ScreenerRow[] = [];
      try {
          const parsed: unknown = JSON.parse(latest.result_json);
          if (!Array.isArray(parsed)) return <p>Empty result</p>;
          data = parsed.filter((x): x is ScreenerRow => typeof x === 'object' && x !== null);
      } catch {
          return <p>Error parsing JSON</p>;
      }

      if (!Array.isArray(data) || data.length === 0) return <p>Empty result</p>;

      const keys = Object.keys(data[0]);
      const columns: TableProps<ScreenerRow>['columns'] = [
          {
              title: 'Action',
              key: 'action',
              width: 80,
              fixed: 'left',
              render: (_, record) => (
                  <Button size="small" type="link" icon={<EyeOutlined />} onClick={() => addToWatchlist(record)}>
                      Watch
                  </Button>
              )
          },
          ...keys.map((k) => ({
              title: k,
              dataIndex: k,
              key: k,
              render: (value: unknown) => (
                  <div style={{maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}} title={String(value)}>
                      {String(value)}
                  </div>
              )
          }))
      ];

      return (
          <div>
              <p>Run at: {new Date(latest.run_at).toLocaleString()} (Count: {latest.count})</p>
              {lastStatus === 'failed' && lastLog && (
                  <p style={{ color: 'red' }}>
                      Last run failed, showing last successful result above. Use “View Log” for details.
                  </p>
              )}
              <Table
                dataSource={data}
                columns={columns}
                rowKey={(r, index) => readFirstString(r, ['symbol', '代码', '股票代码']) ?? `${latest.id}-${index}`}
                size="small"
                scroll={{ x: true }}
                pagination={{ pageSize: 20 }}
              />
          </div>
      );
  };

  return (
    <Layout style={{ height: '100%', background: '#fff' }}>
      <Sider width={250} theme="light" style={{ borderRight: '1px solid #f0f0f0' }}>
        <div style={{ padding: 10 }}>
            <Button type="primary" block icon={<PlusOutlined />} onClick={() => { setSelectedId(null); setEditingScreener({}); }}>
                New Strategy
            </Button>
        </div>
        <List
            dataSource={screeners}
            renderItem={item => (
                <List.Item 
                    style={{ padding: '10px 20px', cursor: 'pointer', background: selectedId === item.id ? '#e6f7ff' : 'transparent' }}
                    onClick={() => setSelectedId(item.id)}
                >
                    <div style={{ width: '100%' }}>
                        <div style={{ fontWeight: 'bold' }}>{item.name}</div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#999' }}>
                             <span>{item.is_active ? <Tag color="green">Active</Tag> : <Tag>Stopped</Tag>}</span>
                             <span>{item.last_run_status === 'success' ? <Tag color="blue">OK</Tag> : item.last_run_status === 'failed' ? <Tag color="red">Fail</Tag> : ''}</span>
                        </div>
                    </div>
                </List.Item>
            )}
        />
      </Sider>
      <Content style={{ padding: 20, overflow: 'auto' }}>
        <Space direction="vertical" style={{ width: '100%' }}>
            <Card>
                <Space wrap>
                    <Input placeholder="Strategy Name" value={editingScreener.name || ''} onChange={e => setEditingScreener({...editingScreener, name: e.target.value})} />
                    <Input placeholder="Cron (e.g., 0 15 * * *)" value={editingScreener.cron_expression || ''} onChange={e => setEditingScreener({...editingScreener, cron_expression: e.target.value})} style={{ width: 180 }} />
                    <Switch checkedChildren="Active" unCheckedChildren="Inactive" checked={editingScreener.is_active || false} onChange={v => setEditingScreener({...editingScreener, is_active: v})} />
                    <Button type="primary" icon={<SaveOutlined />} loading={loading} onClick={handleSave}>Save</Button>
                    {selectedId && (
                        <>
                            <Button type="default" icon={<PlayCircleOutlined />} loading={running} onClick={handleRun}>Run Now</Button>
                            {editingScreener.last_run_status === 'failed' && editingScreener.last_run_log ? (
                                <Button
                                    onClick={() =>
                                        Modal.error({
                                            title: "Last Run Log",
                                            content: (
                                                <pre style={{ maxHeight: 400, overflow: 'auto' }}>
                                                    {editingScreener.last_run_log}
                                                </pre>
                                            )
                                        })
                                    }
                                >
                                    View Log
                                </Button>
                            ) : null}
                            <Button danger icon={<DeleteOutlined />} onClick={handleDelete}>Delete</Button>
                        </>
                    )}
                </Space>
                <Input.TextArea 
                    placeholder="Description" 
                    value={editingScreener.description || ''} 
                    onChange={e => setEditingScreener({...editingScreener, description: e.target.value})} 
                    style={{ marginTop: 10 }} 
                    rows={2}
                />
            </Card>

            <Tabs defaultActiveKey="editor">
                <TabPane tab="Script Editor" key="editor">
                    <div style={{ height: 600, border: '1px solid #d9d9d9' }}>
                        <Editor 
                            defaultLanguage="python" 
                            value={editingScreener.script_content || DEFAULT_SCRIPT}
                            onChange={v => setEditingScreener({...editingScreener, script_content: v ?? ''})}
                            options={{ minimap: { enabled: false }, scrollBeyondLastLine: false }}
                        />
                    </div>
                </TabPane>
                <TabPane tab="Latest Results" key="results">
                    {renderResultTable()}
                </TabPane>
            </Tabs>
        </Space>
      </Content>
    </Layout>
  );
};

export default ScreenerPage;
