import React, { useState, useEffect } from 'react';
import { Layout, Menu } from 'antd';
import { DashboardOutlined, HistoryOutlined, AppstoreOutlined, RobotOutlined, ToolOutlined, FilterOutlined, ExperimentOutlined, NotificationOutlined } from '@ant-design/icons';
import { HashRouter, Routes, Route, useNavigate, useLocation, Navigate } from 'react-router-dom';
import StockTable from './components/StockTable.tsx';
import AISettings from './components/AISettings.tsx';
import LogsViewer from './components/LogsViewer.tsx';
import IndicatorLibrary from './components/IndicatorLibrary.tsx';
import Settings from './components/Settings.tsx';
import ScreenerPage from './components/ScreenerPage.tsx';
import ResearchPage from './components/ResearchPage.tsx';
import RuleLibrary from './components/RuleLibrary.tsx';
import NewsPage from './components/NewsPage.tsx';
import StockDetail from './components/StockDetail.tsx';

const { Header, Content, Sider } = Layout;

const MENU_ITEMS = [
  { key: 'dashboard', path: '/dashboard', label: '看盘', icon: <DashboardOutlined />, component: <StockTable /> },
  { key: 'ai', path: '/ai', label: 'AI 模型', icon: <RobotOutlined />, component: <AISettings /> },
  { key: 'screener', path: '/screener', label: '选股', icon: <FilterOutlined />, component: <ScreenerPage /> },
  { key: 'rules', path: '/rules', label: '规则库', icon: <ToolOutlined />, component: <RuleLibrary /> },
  { key: 'research', path: '/research', label: '数据实验室', icon: <ExperimentOutlined />, component: <ResearchPage /> },
  { key: 'news', path: '/news', label: '新闻与舆情', icon: <NotificationOutlined />, component: <NewsPage /> },
  { key: 'indicators', path: '/indicators', label: '指标库', icon: <AppstoreOutlined />, component: <IndicatorLibrary /> },
  { key: 'logs', path: '/logs', label: '日志', icon: <HistoryOutlined />, component: <LogsViewer /> },
  { key: 'settings', path: '/settings', label: '系统设置', icon: <ToolOutlined />, component: <Settings /> },
];

const MainLayout: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const [selectedKey, setSelectedKey] = useState('dashboard');

  useEffect(() => {
    const currentPath = location.pathname.substring(1) || 'dashboard';
    const activeItem = MENU_ITEMS.find(item => item.path === `/${currentPath}`);
    if (activeItem) {
      setSelectedKey(activeItem.key);
    }
  }, [location]);

  const handleMenuClick = (e: { key: string }) => {
    const item = MENU_ITEMS.find(item => item.key === e.key);
    if (item) {
      navigate(item.path);
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
          selectedKeys={[selectedKey]} 
          mode="inline" 
          onClick={handleMenuClick}
          items={MENU_ITEMS.map(item => ({
            key: item.key,
            icon: item.icon,
            label: item.label,
          }))}
        />
      </Sider>
      <Layout className="site-layout">
        <Header style={{ padding: 0, background: '#fff' }} />
        <Content style={{ margin: '16px' }}>
          <div style={{ padding: 24, minHeight: 360, background: '#fff' }}>
            <Routes>
              <Route path="/" element={<Navigate to="/dashboard" replace />} />
              {MENU_ITEMS.map(item => (
                <Route key={item.key} path={item.path} element={item.component} />
              ))}
              <Route path="/stock/:symbol" element={<StockDetail />} />
              <Route path="*" element={<Navigate to="/dashboard" replace />} />
            </Routes>
          </div>
        </Content>
      </Layout>
    </Layout>
  );
};

const App: React.FC = () => {
  return (
    <HashRouter>
      <MainLayout />
    </HashRouter>
  );
};

export default App;
