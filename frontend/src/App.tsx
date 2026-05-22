import { Routes, Route, Link } from 'react-router-dom'
import UploadPage from './pages/ingestion/UploadPage'
import TaskListPage from './pages/ingestion/TaskListPage'
import ChatPage from './pages/chat/ChatPage'

export default function App() {
  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 flex gap-6 py-3">
          <Link to="/" className="font-semibold text-blue-600 hover:text-blue-800">上传文档</Link>
          <Link to="/tasks" className="font-semibold text-gray-600 hover:text-blue-600">任务列表</Link>
          <Link to="/chat" className="font-semibold text-gray-600 hover:text-blue-600">智能问答</Link>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-4 py-8">
        <Routes>
          <Route path="/" element={<UploadPage />} />
          <Route path="/tasks" element={<TaskListPage />} />
          <Route path="/chat" element={<ChatPage />} />
        </Routes>
      </main>
    </div>
  )
}
