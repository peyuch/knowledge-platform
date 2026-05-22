import { useQuery } from '@tanstack/react-query'
import { listTasks, retryTask, type TaskStatus } from '../../api/ingestion'

const STATUS_LABELS: Record<string, string> = {
  pending: '等待中',
  parsing: '解析中',
  chunking: '分块中',
  storing: '存储中',
  done: '完成',
  failed: '失败',
  cancelled: '已取消',
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-gray-100 text-gray-700',
  parsing: 'bg-blue-100 text-blue-700',
  chunking: 'bg-indigo-100 text-indigo-700',
  storing: 'bg-purple-100 text-purple-700',
  done: 'bg-green-100 text-green-700',
  failed: 'bg-red-100 text-red-700',
  cancelled: 'bg-yellow-100 text-yellow-700',
}

export default function TaskListPage() {
  const { data, isLoading, refetch } = useQuery({
    queryKey: ['tasks'],
    queryFn: () => listTasks({ page: 1, page_size: 50 }),
    refetchInterval: 5000,
  })

  const handleRetry = async (taskId: string) => {
    await retryTask(taskId)
    refetch()
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold">任务列表</h1>
        <button onClick={() => refetch()} className="px-4 py-2 bg-gray-100 rounded hover:bg-gray-200">
          刷新
        </button>
      </div>

      {isLoading ? (
        <p className="text-gray-500">加载中...</p>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="bg-gray-50 text-left text-sm text-gray-600">
                <th className="px-4 py-3">文件名</th>
                <th className="px-4 py-3">类型</th>
                <th className="px-4 py-3">状态</th>
                <th className="px-4 py-3">进度</th>
                <th className="px-4 py-3">创建时间</th>
                <th className="px-4 py-3">操作</th>
              </tr>
            </thead>
            <tbody>
              {(data?.items ?? []).map((task: TaskStatus) => (
                <tr key={task.task_id} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm">{task.original_filename}</td>
                  <td className="px-4 py-3 text-sm text-gray-500">{task.file_type}</td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${STATUS_COLORS[task.status] || ''}`}>
                      {STATUS_LABELS[task.status] || task.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <div className="flex items-center gap-2">
                      <div className="w-20 bg-gray-200 rounded-full h-2">
                        <div className="bg-blue-600 rounded-full h-2" style={{ width: `${task.progress}%` }} />
                      </div>
                      <span className="text-xs text-gray-500">{task.progress}%</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-500">
                    {new Date(task.created_at).toLocaleString('zh-CN')}
                  </td>
                  <td className="px-4 py-3">
                    {task.status === 'failed' && (
                      <button
                        onClick={() => handleRetry(task.task_id)}
                        className="px-3 py-1 text-xs bg-blue-50 text-blue-600 rounded hover:bg-blue-100"
                      >
                        重试
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {(!data?.items || data.items.length === 0) && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-gray-400">
                    暂无任务
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
