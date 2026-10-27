import { Typography } from 'antd';

const { Title, Text } = Typography;

/** 纯视觉页头组件，不改变任何业务行为 */
export default function PageHeader({ title, description, extra }) {
  return (
    <div className="mm-page-header">
      <div>
        <Title level={2} className="mm-page-header__title">{title}</Title>
        {description ? <Text type="secondary" className="mm-page-header__desc">{description}</Text> : null}
      </div>
      {extra ? <div className="mm-page-header__extra">{extra}</div> : null}
    </div>
  );
}
