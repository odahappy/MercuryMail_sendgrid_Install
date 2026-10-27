import { getErrorMessage } from '../api/index.js';
import { Button, Card, Form, Input, Typography, message } from 'antd';
import { ApiOutlined, LockOutlined } from '@ant-design/icons';
import { useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { login, saveAuthToken } from '../api/auth.js';

const { Title, Text } = Typography;

export default function Login() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [loading, setLoading] = useState(false);
  const [form] = Form.useForm();

  const handleSubmit = async (values) => {
    setLoading(true);
    try {
      const data = await login(values.password);
      saveAuthToken(data.access_token, data.expires_in);
      message.success('登录成功');
      const redirect = searchParams.get('redirect');
      const safeRedirect = redirect && redirect.startsWith('/') && !redirect.startsWith('//') ? redirect : '/';
      navigate(safeRedirect, { replace: true });
    } catch (error) {
      message.error(getErrorMessage(error, '密码错误'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mm-login">
      <Card className="mm-login__card" bordered={false}>
        <div className="mm-login__brand">
          <div className="mm-login__logo">
            <ApiOutlined />
          </div>
          <div>
            <Title level={2} style={{ margin: 0 }}>MercuryMail</Title>
            <Text type="secondary" style={{ fontSize: 13 }}>高并发群发邮件系统</Text>
          </div>
        </div>
        <Text type="secondary" style={{ display: 'block', marginTop: 16 }}>
          请输入登录密码。
        </Text>
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
          style={{ marginTop: 24 }}
          disabled={loading}
        >
          <Form.Item
            name="password"
            label="密码"
            rules={[{ required: true, message: '请输入密码' }]}
          >
            <Input.Password
              prefix={<LockOutlined />}
              autoComplete="current-password"
              size="large"
              placeholder="请输入密码"
            />
          </Form.Item>
          <Button type="primary" htmlType="submit" block size="large" loading={loading}>
            登录
          </Button>
        </Form>
      </Card>
    </div>
  );
}
