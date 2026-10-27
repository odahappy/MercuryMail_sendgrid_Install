import {
  ApiOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  LinkOutlined,
  LogoutOutlined,
  MailOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  RobotOutlined,
  SendOutlined,
  SettingOutlined,
  TeamOutlined,
  ThunderboltOutlined,
  UnorderedListOutlined,
} from '@ant-design/icons';
import { Button, Form, Input, Layout, Menu, Modal, Typography, message } from 'antd';
import { changePassword } from '../api/auth.js';
import { cancelRouteRequests, getErrorMessage } from '../api/index.js';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useEffect, useMemo, useRef, useState } from 'react';

const { Header, Sider, Content } = Layout;

/** 按邮件发送业务流程排序的侧边栏菜单 */
const menuItems = [
  { key: '/', icon: <DashboardOutlined />, label: '仪表盘' },
  { key: '/urls', icon: <LinkOutlined />, label: 'URL 池' },
  { key: '/recipients', icon: <TeamOutlined />, label: '收件人池' },
  { key: '/templates', icon: <MailOutlined />, label: '模板管理' },
  { key: '/channels', icon: <CloudServerOutlined />, label: '通道管理' },
  { key: '/tasks', icon: <SendOutlined />, label: '发送任务' },
  { key: '/warmup', icon: <ThunderboltOutlined />, label: 'AI 养号' },
  { key: '/logs', icon: <UnorderedListOutlined />, label: '日志查看' },
  { key: '/settings/ai', icon: <RobotOutlined />, label: 'AI 配置' },
  { key: '/data-center', icon: <DatabaseOutlined />, label: '数据中心' },
];

const routeTitles = {
  '/': '仪表盘',
  '/urls': 'URL 池',
  '/recipients': '收件人池',
  '/templates': '模板管理',
  '/channels': '通道管理',
  '/tasks': '发送任务',
  '/warmup': 'AI 养号',
  '/logs': '日志查看',
  '/settings/ai': 'AI 配置',
  '/data-center': '数据中心',
};

function resolvePageTitle(pathname) {
  if (pathname.startsWith('/tasks/')) return '任务详情';
  if (pathname.startsWith('/warmup/create')) return '创建养号计划';
  if (pathname.startsWith('/warmup/')) return '养号详情';
  const match = menuItems.find((item) => item.key !== '/' && pathname.startsWith(item.key));
  return match ? routeTitles[match.key] : routeTitles['/'];
}

export default function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const previousLocationKeyRef = useRef(location.key);
  const [collapsed, setCollapsed] = useState(false);
  const [passwordModalOpen, setPasswordModalOpen] = useState(false);
  const [changingPassword, setChangingPassword] = useState(false);
  const [passwordForm] = Form.useForm();
  const selectedKeys = useMemo(() => {
    if (location.pathname.startsWith('/tasks')) return ['/tasks'];
    if (location.pathname.startsWith('/warmup')) return ['/warmup'];
    if (location.pathname.startsWith('/settings/ai')) return ['/settings/ai'];
    const match = menuItems.find((item) => item.key !== '/' && location.pathname.startsWith(item.key));
    return [match?.key || '/'];
  }, [location.pathname]);

  const pageTitle = useMemo(() => resolvePageTitle(location.pathname), [location.pathname]);

  useEffect(() => {
    if (previousLocationKeyRef.current !== location.key) {
      cancelRouteRequests();
      previousLocationKeyRef.current = location.key;
    }
  }, [location.key]);

  const logout = () => {
    window.localStorage.removeItem('mercury_token');
    window.localStorage.removeItem('mercury_token_expire_at');
    navigate('/login', { replace: true });
  };

  const submitPasswordChange = async () => {
    try {
      const values = await passwordForm.validateFields();
      setChangingPassword(true);
      const result = await changePassword(values.old_password, values.new_password);
      message.success(result.message || '密码已修改');
      passwordForm.resetFields();
      setPasswordModalOpen(false);
      window.localStorage.removeItem('mercury_token');
      window.localStorage.removeItem('mercury_token_expire_at');
      navigate('/login', { replace: true });
    } catch (error) {
      if (!error.errorFields) message.error(getErrorMessage(error, '修改密码失败'));
    } finally {
      setChangingPassword(false);
    }
  };

  return (
    <Layout className="mm-layout">
      <Sider
        className="mm-sider"
        collapsible
        collapsed={collapsed}
        trigger={null}
        width={240}
        collapsedWidth={72}
        breakpoint="lg"
      >
        <div className="mm-sider-inner">
          <div className="mm-brand">
            <div className="mm-brand__icon">
              <ApiOutlined />
            </div>
            <div className="mm-brand__text">
              <span className="mm-brand__name">MercuryMail</span>
              <span className="mm-brand__tagline">高并发邮件发送平台</span>
            </div>
          </div>
          <Menu
            className="mm-menu"
            theme="dark"
            mode="inline"
            selectedKeys={selectedKeys}
            items={menuItems}
            onClick={({ key }) => navigate(key)}
          />
          <div className="mm-sider-footer">
            <Typography.Text style={{ fontSize: 12, color: '#8c8c8c', lineHeight: 1.5 }}>
              联系作者：TG @NibabaKU
            </Typography.Text>
          </div>
        </div>
      </Sider>
      <Layout className="mm-main">
        <Header className="mm-header">
          <div className="mm-header__left">
            <Button
              className="mm-header__toggle"
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed((value) => !value)}
            />
            <span className="mm-header__breadcrumb">{pageTitle}</span>
          </div>
          <div className="mm-header__actions">
            <span className="mm-header__user">
              <span className="mm-header__user-dot" />
              管理员
            </span>
            <Button icon={<SettingOutlined />} onClick={() => setPasswordModalOpen(true)}>
              修改密码
            </Button>
            <Button icon={<LogoutOutlined />} onClick={logout}>
              退出登录
            </Button>
          </div>
        </Header>
        <Content className="mm-content">
          <div className="mm-content-inner">
            <Outlet />
          </div>
        </Content>
        <Modal
          title="修改管理员密码"
          open={passwordModalOpen}
          onCancel={() => setPasswordModalOpen(false)}
          onOk={submitPasswordChange}
          confirmLoading={changingPassword}
          okText="保存"
          cancelText="取消"
          destroyOnHidden
        >
          <Form form={passwordForm} layout="vertical" disabled={changingPassword}>
            <Form.Item name="old_password" label="旧密码" rules={[{ required: true, message: '请输入旧密码' }]}>
              <Input.Password autoComplete="current-password" />
            </Form.Item>
            <Form.Item
              name="new_password"
              label="新密码"
              rules={[
                { required: true, message: '请输入新密码' },
                { min: 6, message: '密码至少 6 位' },
              ]}
              extra="密码至少 6 位"
            >
              <Input.Password autoComplete="new-password" />
            </Form.Item>
            <Form.Item
              name="confirm_password"
              label="确认新密码"
              dependencies={['new_password']}
              rules={[
                { required: true, message: '请再次输入新密码' },
                ({ getFieldValue }) => ({
                  validator(_, value) {
                    if (!value || getFieldValue('new_password') === value) return Promise.resolve();
                    return Promise.reject(new Error('两次输入的新密码不一致'));
                  },
                }),
              ]}
            >
              <Input.Password autoComplete="new-password" />
            </Form.Item>
          </Form>
        </Modal>
      </Layout>
    </Layout>
  );
}
