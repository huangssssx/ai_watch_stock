import { useMemo } from 'react';
import { HashRouter as Router, Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import { AlertOutlined, LineChartOutlined } from '@ant-design/icons';
import AlertRulesPage from './components/AlertRulesPage';
import IndicatorPage from './components/IndicatorPage';
import './App.css';

const { Header, Content, Footer } = Layout;

const AppShell = () => {
  const location = useLocation();
  const selectedKey = useMemo(() => (location.pathname.startsWith('/indicators') ? '2' : '1'), [location.pathname]);

  return (
    <Layout className="layout" style={{ minHeight: '100vh', boxSizing: 'border-box', width: '100%' }}>
      <Header>
        <div className="logo" style={{ float: 'left', color: 'white', fontSize: '20px', marginRight: '20px' }}>
          AI Watch Stock
        </div>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[selectedKey]}
          items={[
            {
              key: '1',
              icon: <AlertOutlined />,
              label: <Link to="/alert-rules">规则管理</Link>,
            },
            {
              key: '2',
              icon: <LineChartOutlined />,
              label: <Link to="/indicators">数据指标</Link>,
            },
          ]}
        />
      </Header>
      <Content style={{ padding: '0 24px', marginTop: 20, boxSizing: 'border-box' }}>
        <div className="site-layout-content" style={{ background: '#fff', padding: 24, minHeight: 280, boxSizing: 'border-box' }}>
          <Routes>
            <Route path="/" element={<Navigate to="/alert-rules" replace />} />
            <Route path="/alert-rules" element={<AlertRulesPage />} />
            <Route path="/indicators" element={<IndicatorPage />} />
            <Route path="*" element={<Navigate to="/alert-rules" replace />} />
          </Routes>
        </div>
      </Content>
      <Footer style={{ textAlign: 'center' }}>AI Watch Stock ©2025 Created by Trae AI</Footer>
    </Layout>
  );
};

const App = () => {
  return (
    <Router>
      <AppShell />
    </Router>
  );
};

export default App;
