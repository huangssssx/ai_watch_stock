import React, { useMemo, useState, useEffect } from 'react';
import { Layout, Menu, Button, Input, Tabs, Table, message, Modal, Splitter, Typography, Space, Tooltip } from 'antd';
import { PlusOutlined, SaveOutlined, PlayCircleOutlined, DeleteOutlined, CodeOutlined, FullscreenOutlined, FullscreenExitOutlined, PlaySquareOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import { getResearchScripts, createResearchScript, updateResearchScript, deleteResearchScript, runResearchScript, runStreamlitScript } from '../api';
import type { ResearchScript } from '../types';
import { ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer } from 'recharts';
import type { ColumnsType } from 'antd/es/table';

const { Sider, Content } = Layout;

type ResearchChartSeries = {
  key: string;
  type?: 'line' | 'bar';
  color?: string;
};

type ResearchChartConfig = {
  xKey: string;
  series?: ResearchChartSeries[];
};

const stableStringify = (value: unknown): string => {
  if (value === null) return 'null';
  const t = typeof value;
  if (t === 'string') return JSON.stringify(value);
  if (t === 'number' || t === 'boolean') return String(value);
  if (t !== 'object') return JSON.stringify(value);

  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;

  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(',')}}`;
};

const fnv1a32Hex = (input: string): string => {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16);
};

const pickRowId = (record: Record<string, unknown>): string | null => {
  const candidates = ['id', 'key', 'symbol', '代码', '股票代码'];
  for (const k of candidates) {
    const v = record[k];
    if (typeof v === 'string' && v.trim()) return v;
    if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  }
  return null;
};

const ResearchPage: React.FC = () => {
  return (
    <>
      <style>
        {`
          .research-tabs {
            display: flex;
            flex-direction: column;
          }
          .research-tabs .ant-tabs-content-holder {
            flex: 1;
            display: flex;
            flex-direction: column;
            min-height: 0;
          }
          .research-tabs .ant-tabs-content {
            flex: 1;
            height: 100%;
          }
          .research-tabs .ant-tabs-tabpane {
            height: 100%;
          }
        `}
      </style>
      <ResearchPageContent />
    </>
  );
};

const ResearchPageContent: React.FC = () => {
  const [fullscreenPanel, setFullscreenPanel] = useState<'code' | 'result' | null>(null);
  const [scripts, setScripts] = useState<ResearchScript[]>([]);
  const [currentScript, setCurrentScript] = useState<Partial<ResearchScript>>({ title: 'New Script', script_content: '' });
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<Record<string, unknown>[]>([]);
  const [log, setLog] = useState('');
  const [chartData, setChartData] = useState<ResearchChartConfig | null>(null);
  const [activeTab, setActiveTab] = useState('log');
  const [streamlitUrl, setStreamlitUrl] = useState<string | null>(null);

  const tableData = useMemo(() => {
    const seen = new Map<string, number>();
    return result.map((r) => {
      const base = (() => {
        const picked = pickRowId(r);
        if (picked) return `id:${picked}`;
        return `h:${fnv1a32Hex(stableStringify(r))}`;
      })();

      const n = seen.get(base) ?? 0;
      seen.set(base, n + 1);

      const rowKey = n === 0 ? base : `${base}:${n}`;
      return { ...r, __rowKey: rowKey };
    });
  }, [result]);

  useEffect(() => {
    fetchScripts();
  }, []);

  useEffect(() => {
    if (!fullscreenPanel) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setFullscreenPanel(null);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [fullscreenPanel]);

  useEffect(() => {
    window.dispatchEvent(new Event('resize'));
  }, [fullscreenPanel]);

  const fetchScripts = async () => {
    try {
      const res = await getResearchScripts();
      setScripts(res.data);
    } catch {
      message.error('Failed to load scripts');
    }
  };

  const handleSelectScript = (script: ResearchScript) => {
    setCurrentScript(script);
    setResult([]);
    setLog('');
    setChartData(null);
  };

  const handleNewScript = () => {
    setCurrentScript({ title: 'New Script', script_content: '# Write your Python script here\n# Define "df" (DataFrame) or "result" (List) for table\n# Define "chart" (Dict) for visualization\n\nimport akshare as ak\nimport pandas as pd\n\nprint("Hello Research Lab")\n' });
    setResult([]);
    setLog('');
    setChartData(null);
  };

  const handleSave = async () => {
    if (!currentScript.title) return message.error('Title is required');
    setLoading(true);
    try {
      if (currentScript.id) {
        await updateResearchScript(currentScript.id, currentScript);
      } else {
        const res = await createResearchScript(currentScript);
        setCurrentScript(res.data);
      }
      message.success('Saved successfully');
      fetchScripts();
    } catch {
      message.error('Failed to save');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (id: number) => {
    Modal.confirm({
      title: 'Delete Script',
      content: 'Are you sure you want to delete this script?',
      onOk: async () => {
        try {
          await deleteResearchScript(id);
          message.success('Deleted successfully');
          fetchScripts();
          handleNewScript();
        } catch {
          message.error('Failed to delete');
        }
      }
    });
  };

  const handleRun = async () => {
    if (!currentScript.script_content) return;
    setRunning(true);
    setLog('Running...');
    try {
      const res = await runResearchScript(currentScript.script_content);
      const serverLog = res.data.log || 'No output';
      const serverError = res.data.error;
      const combinedLog =
        serverError && !serverLog.includes(serverError) ? `${serverLog}\n\n${serverError}` : serverLog;
      setLog(combinedLog);
      setResult(res.data.result || []);
      setChartData((res.data.chart as ResearchChartConfig | undefined) || null);
      
      if (res.data.error) {
        message.error('Script execution failed');
        setActiveTab('log');
      } else {
        message.success('Execution successful');
        if (res.data.chart) setActiveTab('chart');
        else if (res.data.result && res.data.result.length > 0) setActiveTab('table');
        else setActiveTab('log');
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setLog(`Error: ${msg}`);
      message.error('Execution error');
    } finally {
      setRunning(false);
    }
  };

  const handleRunStreamlit = async () => {
    if (!currentScript.script_content) return;
    setRunning(true);
    try {
      const res = await runStreamlitScript(currentScript.script_content);
      setStreamlitUrl(res.data.url);
      setActiveTab('streamlit');
      message.success('Streamlit updated');
    } catch (e) {
      message.error('Failed to run Streamlit');
    } finally {
      setRunning(false);
    }
  };

  const renderChart = () => {
    if (!chartData || result.length === 0) return <div style={{ padding: 20 }}>No chart data available. Define 'chart' variable in script.</div>;

    const { xKey, series = [] } = chartData;
    const dataLen = result.length;
    const maxLabelLength = result.reduce((max, row) => {
      const v = row?.[xKey];
      const s = v === undefined || v === null ? '' : String(v);
      return Math.max(max, s.length);
    }, 0);
    const shouldRotate = dataLen > 12 || maxLabelLength > 10;
    const tickAngle = shouldRotate ? -45 : 0;
    const xAxisHeight = shouldRotate ? 90 : 40;
    const enableScroll = dataLen > 20;
    const minChartWidth = 900;
    const perItemWidth = Math.min(100, Math.max(36, maxLabelLength * 6));
    const chartWidth = enableScroll ? Math.max(minChartWidth, dataLen * perItemWidth) : '100%';
    const chartHeight = 520;

    const AxisTick = (props: {
      x?: number;
      y?: number;
      payload?: { value?: unknown };
    }) => {
      const x = props.x ?? 0;
      const y = props.y ?? 0;
      const raw = props.payload?.value;
      const label = raw === undefined || raw === null ? '' : String(raw);
      return (
        <g transform={`translate(${x},${y})`}>
          <text
            x={0}
            y={0}
            dy={shouldRotate ? 16 : 10}
            textAnchor={shouldRotate ? 'end' : 'middle'}
            fill="#666"
            fontSize={12}
            transform={shouldRotate ? `rotate(${tickAngle})` : undefined}
          >
            <title>{label}</title>
            {label}
          </text>
        </g>
      );
    };

    return (
      <div style={{ width: '100%', height: chartHeight, overflowX: enableScroll ? 'auto' : 'hidden', overflowY: 'hidden' }}>
        <div style={{ width: chartWidth, height: chartHeight }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
              data={result}
              margin={{ top: 20, right: 20, left: 0, bottom: shouldRotate ? 70 : 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} height={xAxisHeight} interval={0} tick={AxisTick} />
              <YAxis />
              <RechartsTooltip />
              <Legend />
              {series.map((s, idx) => {
                if (s.type === 'bar') return <Bar key={idx} dataKey={s.key} fill={s.color || '#82ca9d'} />;
                return <Line key={idx} type="monotone" dataKey={s.key} stroke={s.color || '#8884d8'} />;
              })}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
  };

  const getColumns = (): ColumnsType<Record<string, unknown>> => {
    if (result.length === 0) return [];
    const keys = Object.keys(result[0] || {});
    return keys.map((key) => ({
      title: key,
      dataIndex: key,
      key: key,
      render: (val: unknown) => (typeof val === 'object' ? JSON.stringify(val) : String(val)),
    }));
  };

  const toggleFullscreen = (panel: 'code' | 'result') => {
    setFullscreenPanel((prev) => (prev === panel ? null : panel));
  };

  const codePanelSize =
    fullscreenPanel === 'code' ? '100%' : fullscreenPanel === 'result' ? 0 : undefined;
  const resultPanelSize =
    fullscreenPanel === 'result' ? '100%' : fullscreenPanel === 'code' ? 0 : undefined;

  return (
    <Layout style={{ height: 'calc(100vh - 64px)' }}>
      <Sider width={250} theme="light" style={{ borderRight: '1px solid #f0f0f0' }}>
        <div style={{ padding: '16px', borderBottom: '1px solid #f0f0f0' }}>
          <Button type="primary" block icon={<PlusOutlined />} onClick={handleNewScript}>New Script</Button>
        </div>
        <Menu
          mode="inline"
          selectedKeys={currentScript.id ? [String(currentScript.id)] : []}
          style={{ borderRight: 0 }}
          items={scripts.map(s => ({
            key: String(s.id),
            icon: <CodeOutlined />,
            label: s.title,
            onClick: () => handleSelectScript(s)
          }))}
        />
      </Sider>
      <Content style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        <div style={{ padding: '12px 24px', background: '#fff', borderBottom: '1px solid #f0f0f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <Input 
              value={currentScript.title} 
              onChange={e => setCurrentScript({ ...currentScript, title: e.target.value })} 
              style={{ width: 300, fontSize: 16, fontWeight: 500 }} 
              placeholder="Script Title"
            />
            {currentScript.id && <Typography.Text type="secondary" style={{ fontSize: 12 }}>Last run: {currentScript.last_run_at || 'Never'}</Typography.Text>}
          </Space>
          <Space>
            <Button type="primary" icon={<SaveOutlined />} onClick={handleSave} loading={loading}>Save</Button>
            <Button type="primary" danger icon={<PlayCircleOutlined />} onClick={handleRun} loading={running}>Run</Button>
            <Button icon={<PlaySquareOutlined />} onClick={handleRunStreamlit} loading={running}>Streamlit</Button>
            {currentScript.id && <Button icon={<DeleteOutlined />} onClick={() => handleDelete(currentScript.id!)} />}
          </Space>
        </div>
        
        <Splitter style={{ flex: 1, minHeight: 0 }}>
          <Splitter.Panel
            defaultSize="40%"
            min="20%"
            max="80%"
            size={codePanelSize}
            resizable={!fullscreenPanel}
          >
            <div style={{ height: '100%', display: 'flex', flexDirection: 'column', minHeight: 0, background: '#fff' }}>
              <div
                style={{
                  padding: '8px 12px',
                  borderBottom: '1px solid #f0f0f0',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  background: '#fafafa',
                }}
              >
                <Typography.Text strong>代码</Typography.Text>
                <Tooltip title={fullscreenPanel === 'code' ? '退出全屏 (Esc)' : '全屏代码'}>
                  <Button
                    type="text"
                    size="small"
                    icon={fullscreenPanel === 'code' ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
                    onClick={() => toggleFullscreen('code')}
                  />
                </Tooltip>
              </div>
              <div style={{ flex: 1, minHeight: 0 }}>
                <Editor
                  height="100%"
                  defaultLanguage="python"
                  value={currentScript.script_content}
                  onChange={(val) => setCurrentScript({ ...currentScript, script_content: val || '' })}
                  options={{ minimap: { enabled: false }, fontSize: 14 }}
                />
              </div>
            </div>
          </Splitter.Panel>
          <Splitter.Panel size={resultPanelSize} resizable={!fullscreenPanel}>
            <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#fff', minHeight: 0 }}>
              <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                className="research-tabs"
                style={{ padding: '0 16px', flex: 1, height: '100%', minHeight: 0 }}
                tabBarExtraContent={
                  <Tooltip title={fullscreenPanel === 'result' ? '退出全屏 (Esc)' : '全屏结果'}>
                    <Button
                      type="text"
                      size="small"
                      icon={fullscreenPanel === 'result' ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
                      onClick={() => toggleFullscreen('result')}
                    />
                  </Tooltip>
                }
                items={[
                  {
                    key: 'log',
                    label: 'Log',
                    children: (
                      <Input.TextArea
                        value={log}
                        readOnly
                        style={{ flex: 1, height: '100%', minHeight: 500, fontFamily: 'monospace' }}
                      />
                    ),
                  },
                  {
                    key: 'table',
                    label: `Data (${result?.length || 0})`,
                    children: (
                      <Table
                        dataSource={tableData}
                        columns={getColumns()}
                        size="small"
                        scroll={{ x: 'max-content', y: 400 }}
                        pagination={{ pageSize: 50 }}
                        rowKey="__rowKey"
                      />
                    ),
                  },
                  {
                    key: 'chart',
                    label: 'Chart',
                    children: renderChart(),
                  },
                  {
                    key: 'streamlit',
                    label: 'Streamlit',
                    children: streamlitUrl ? (
                      <iframe
                        src={streamlitUrl}
                        style={{ width: '100%', height: '100%', border: 'none' }}
                        title="Streamlit"
                      />
                    ) : (
                      <div style={{ padding: 20 }}>Run script as Streamlit to see result.</div>
                    ),
                  },
                ]}
              />
            </div>
          </Splitter.Panel>
        </Splitter>
      </Content>
    </Layout>
  );
};

export default ResearchPage;
