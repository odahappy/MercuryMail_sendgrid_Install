import { Navigate, Route, Routes } from 'react-router-dom';
import AppLayout from './components/Layout.jsx';
import Dashboard from './pages/Dashboard.jsx';
import Tasks from './pages/Tasks.jsx';
import TaskDetail from './pages/TaskDetail.jsx';
import WarmupList from './pages/WarmupList.jsx';
import WarmupCreate from './pages/WarmupCreate.jsx';
import WarmupDetail from './pages/WarmupDetail.jsx';
import AISettings from './pages/AISettings.jsx';
import Templates from './pages/Templates.jsx';
import Recipients from './pages/Recipients.jsx';
import Channels from './pages/Channels.jsx';
import Urls from './pages/Urls.jsx';
import Logs from './pages/Logs.jsx';
import DataCenter from './pages/DataCenter.jsx';
import Login from './pages/Login.jsx';

function RequireAuth({ children }) {
  const token = window.localStorage.getItem('mercury_token');
  const expireAt = Number(window.localStorage.getItem('mercury_token_expire_at') || 0);
  if (!token || !expireAt || Date.now() >= expireAt) {
    const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    const loginTarget = current && current !== '/' ? `/login?redirect=${encodeURIComponent(current)}` : '/login';
    return <Navigate to={loginTarget} replace />;
  }
  return children;
}

export default function App() {
  return <Routes>
    <Route path="/login" element={<Login />} />
    <Route path="/" element={<RequireAuth><AppLayout /></RequireAuth>}>
      <Route index element={<Dashboard />} />
      <Route path="tasks" element={<Tasks />} />
      <Route path="tasks/:id" element={<TaskDetail />} />
      <Route path="warmup" element={<WarmupList />} />
      <Route path="warmup/create" element={<WarmupCreate />} />
      <Route path="warmup/:id" element={<WarmupDetail />} />
      <Route path="settings/ai" element={<AISettings />} />
      <Route path="templates" element={<Templates />} />
      <Route path="recipients" element={<Recipients />} />
      <Route path="channels" element={<Channels />} />
      <Route path="urls" element={<Urls />} />
      <Route path="logs" element={<Logs />} />
      <Route path="data-center" element={<DataCenter />} />
    </Route>
    <Route path="*" element={<Navigate to="/" replace />} />
  </Routes>;
}
