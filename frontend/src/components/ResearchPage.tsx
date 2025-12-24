import React, { useState, useEffect } from 'react';
import { Layout, Menu, Button, Input, Tabs, Table, message, Modal, Splitter, Typography, Space } from 'antd';
import { PlusOutlined, SaveOutlined, PlayCircleOutlined, DeleteOutlined, CodeOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import { getResearchScripts, createResearchScript, updateResearchScript, deleteResearchScript, runResearchScript } from '../api';
import type { ResearchScript } from '../types';
import { ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
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

const ResearchPage: React.FC = () => {
  const [scripts, setScripts] = useState<ResearchScript[]>([]);
  const [currentScript, setCurrentScript] = useState<Partial<ResearchScript>>({ title: 'New Script', script_content: '' });
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<Record<string, unknown>[]>([]);
  const [log, setLog] = useState('');
  const [chartData, setChartData] = useState<ResearchChartConfig | null>(null);
  const [activeTab, setActiveTab] = useState('log');

  useEffect(() => {
    fetchScripts();
  }, []);

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

  const renderChart = () => {
    if (!chartData || result.length === 0) return <div style={{ padding: 20 }}>No chart data available. Define 'chart' variable in script.</div>;

    const { xKey, series = [] } = chartData;

    return (
      <ResponsiveContainer width="100%" height={500}>
        <ComposedChart data={result}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} />
          <YAxis />
          <Tooltip />
          <Legend />
          {series.map((s, idx) => {
            if (s.type === 'bar') return <Bar key={idx} dataKey={s.key} fill={s.color || '#82ca9d'} />;
            return <Line key={idx} type="monotone" dataKey={s.key} stroke={s.color || '#8884d8'} />;
          })}
        </ComposedChart>
      </ResponsiveContainer>
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
            {currentScript.id && <Button icon={<DeleteOutlined />} onClick={() => handleDelete(currentScript.id!)} />}
          </Space>
        </div>
        
        <Splitter style={{ flex: 1 }}>
          <Splitter.Panel defaultSize="40%" min="20%" max="80%">
             <Editor
               height="100%"
               defaultLanguage="python"
               value={currentScript.script_content}
               onChange={val => setCurrentScript({ ...currentScript, script_content: val || '' })}
               options={{ minimap: { enabled: false }, fontSize: 14 }}
             />
          </Splitter.Panel>
          <Splitter.Panel>
            <div  style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#fff' }}>
              <Tabs 
                activeKey={activeTab} 
                onChange={setActiveTab} 
                style={{ padding: '0 16px', flex: 1,height: '100%' }}
                items={[
                  {
                    key: 'log',
                    label: 'Log',
                    children: <Input.TextArea value={log} readOnly style={{ flex:1,height: '100%', minHeight: 500, fontFamily: 'monospace' }} />
                  },
                  {
                    key: 'table',
                    label: `Data (${result?.length || 0})`,
                    children: <Table dataSource={result} columns={getColumns()} size="small" scroll={{ x: 'max-content', y: 400 }} pagination={{ pageSize: 50 }} rowKey={(_, i) => String(i)} />
                  },
                  {
                    key: 'chart',
                    label: 'Chart',
                    children: renderChart()
                  }
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
