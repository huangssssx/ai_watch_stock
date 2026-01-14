import React, { useState } from 'react';
import { Layout, Menu } from 'antd';
import { DashboardOutlined, HistoryOutlined, AppstoreOutlined, RobotOutlined, ToolOutlined, FilterOutlined, ExperimentOutlined, NotificationOutlined } from '@ant-design/icons';
import StockTable from './components/StockTable.tsx';
import AISettings from './components/AISettings.tsx';
import LogsViewer from './components/LogsViewer.tsx';
import IndicatorLibrary from './components/IndicatorLibrary.tsx';
import Settings from './components/Settings.tsx';
import ScreenerPage from './components/ScreenerPage.tsx';
import ResearchPage from './components/ResearchPage.tsx';
import RuleLibrary from './components/RuleLibrary.tsx';
import NewsPage from './components/NewsPage.tsx';

const { Header, Content, Sider } = Layout;

const App: React.FC = () => {
  const [selectedKey, setSelectedKey] = useState('1');
  const [collapsed, setCollapsed] = useState(false);

  const renderContent = () => {
    switch (selectedKey) {
      case '1': return <StockTable />;
      case '2': return <AISettings />;
      case '3': return <LogsViewer />;
      case '4': return <IndicatorLibrary />;
      case '5': return <Settings />;
      case '6': return <ScreenerPage />;
      case '7': return <ResearchPage />;
      case '8': return <RuleLibrary />;
      case '9': return <NewsPage />;
      default: return <StockTable />;
    }
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible collapsed={collapsed} onCollapse={setCollapsed}>
        <div style={{ height: 32, margin: 16, background: 'rgba(255, 255, 255, 0.2)', textAlign: 'center', color: 'white', lineHeight: '32px', overflow: 'hidden', whiteSpace: 'nowrap' }}>
          {collapsed ? 'AI' : '智能盯盘'}
        </div>
        <Menu 
          theme="dark" 
          defaultSelectedKeys={['1']} 
          mode="inline" 
          onClick={(e) => setSelectedKey(e.key)}
          items={[
            { key: '1', icon: <DashboardOutlined />, label: '看盘' },
            { key: '2', icon: <RobotOutlined />, label: 'AI 模型' },
            { key: '6', icon: <FilterOutlined />, label: '选股' },
            { key: '8', icon: <ToolOutlined />, label: '规则库' },
            { key: '7', icon: <ExperimentOutlined />, label: '数据实验室' },
            { key: '9', icon: <NotificationOutlined />, label: '新闻与舆情' },
            { key: '4', icon: <AppstoreOutlined />, label: '指标库' },
            { key: '3', icon: <HistoryOutlined />, label: '日志' },
            { key: '5', icon: <ToolOutlined />, label: '系统设置' },
          ]}
        />
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
