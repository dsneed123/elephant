import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Traders from './pages/Traders'
import Signals from './pages/Signals'
import Trades from './pages/Trades'
import Settings from './pages/Settings'
import { WebSocketProvider } from './contexts/WebSocketContext'

export default function App() {
  return (
    <WebSocketProvider>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="traders" element={<Traders />} />
          <Route path="signals" element={<Signals />} />
          <Route path="trades" element={<Trades />} />
          <Route path="settings" element={<Settings />} />
        </Route>
      </Routes>
    </BrowserRouter>
    </WebSocketProvider>
  )
}
