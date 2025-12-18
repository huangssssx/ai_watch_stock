import React, { useState } from 'react';
import { Layout, Menu } from 'antd';
import { DashboardOutlined, SettingOutlined, HistoryOutlined, AppstoreOutlined, RobotOutlined, ToolOutlined } from '@ant-design/icons';
import StockTable from './components/StockTable.tsx';
import AISettings from './components/AISettings.tsx';
import LogsViewer from './components/LogsViewer.tsx';
import IndicatorLibrary from './components/IndicatorLibrary.tsx';
import Settings from './components/Settings.tsx';

const { Header, Content, Sider } = Layout;

const App: React.FC = () => {
  const [selectedKey, setSelectedKey] = useState('1');

  const renderContent = () => {
    switch (selectedKey) {
      case '1': return <StockTable />;
      case '2': return <AISettings />;
      case '3': return <LogsViewer />;
      case '4': return <IndicatorLibrary />;
      case '5': return <Settings />;
      default: return <StockTable />;
    }
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible>
        <div style={{ height: 32, margin: 16, background: 'rgba(255, 255, 255, 0.2)', textAlign: 'center', color: 'white', lineHeight: '32px' }}>智能盯盘</div>
        <Menu theme="dark" defaultSelectedKeys={['1']} mode="inline" onClick={(e) => setSelectedKey(e.key)}>
          <Menu.Item key="1" icon={<DashboardOutlined />}>看盘</Menu.Item>
          <Menu.Item key="2" icon={<RobotOutlined />}>AI 模型</Menu.Item>
          <Menu.Item key="4" icon={<AppstoreOutlined />}>指标库</Menu.Item>
          <Menu.Item key="3" icon={<HistoryOutlined />}>日志</Menu.Item>
          <Menu.Item key="5" icon={<ToolOutlined />}>系统设置</Menu.Item>
        </Menu>
      </Sider>
      <Layout className="site-layout">
        <Header style={{ padding: 0, background: '#fff' }} />
        <Content style={{ margin: '16px' }}>
          <div style={{ padding: 24, minHeight: 360, background: '#fff' }}>
            {renderContent()}
          </div>
        </Content>
      </Layout>
    </Layout>
  );
};

export default App;
