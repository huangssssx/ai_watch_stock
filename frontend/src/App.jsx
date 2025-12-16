import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import Dashboard from './components/Dashboard';
import StrategyEditor from './components/StrategyEditor';
import { DesktopOutlined, PlusCircleOutlined, AlertOutlined } from '@ant-design/icons';
import ChartPage from './components/ChartPage';
import AlertRulesPage from './components/AlertRulesPage';

const { Header, Content, Footer } = Layout;

const App = () => {
  return (
    <Router>
      <Layout className="layout" style={{ minHeight: '100vh' }}>
        <Header>
          <div className="logo" style={{ float: 'left', color: 'white', fontSize: '20px', marginRight: '20px' }}>
            AI Watch Stock
          </div>
          <Menu
            theme="dark"
            mode="horizontal"
            defaultSelectedKeys={['1']}
            items={[
              {
                key: '1',
                icon: <DesktopOutlined />,
                label: <Link to="/">Dashboard</Link>,
              },
              {
                key: '2',
                icon: <PlusCircleOutlined />,
                label: <Link to="/create">New Strategy</Link>,
              },
              {
                key: '3',
                icon: <AlertOutlined />,
                label: <Link to="/alert-rules">规则管理</Link>,
              },
            ]}
          />
        </Header>
        <Content style={{ padding: '0 50px', marginTop: 20 }}>
          <div className="site-layout-content" style={{ background: '#fff', padding: 24, minHeight: 280 }}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/create" element={<StrategyEditor />} />
              <Route path="/edit/:id" element={<StrategyEditor />} />
              <Route path="/chart/:id" element={<ChartPage />} />
              <Route path="/alert-rules" element={<AlertRulesPage />} />
            </Routes>
          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>AI Watch Stock ©2025 Created by Trae AI</Footer>
      </Layout>
    </Router>
  );
};

export default App;
