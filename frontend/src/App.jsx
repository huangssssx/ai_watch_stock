import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, Navigate } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import { AlertOutlined } from '@ant-design/icons';
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
                icon: <AlertOutlined />,
                label: <Link to="/alert-rules">规则管理</Link>,
              },
            ]}
          />
        </Header>
        <Content style={{ padding: '0 50px', marginTop: 20 }}>
          <div className="site-layout-content" style={{ background: '#fff', padding: 24, minHeight: 280 }}>
            <Routes>
              <Route path="/" element={<Navigate to="/alert-rules" replace />} />
              <Route path="/alert-rules" element={<AlertRulesPage />} />
              <Route path="*" element={<Navigate to="/alert-rules" replace />} />
            </Routes>
          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>AI Watch Stock ©2025 Created by Trae AI</Footer>
      </Layout>
    </Router>
  );
};

export default App;
