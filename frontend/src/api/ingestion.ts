import axios from 'axios'

const api = axios.create({
  baseURL: '/api/v1',
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token') || 'dev-token'
  config.headers.Authorization = `Bearer ${token}`
  return config
})

export interface TaskStatus {
  task_id: string
  original_filename: string
  file_type: string
  file_hash: string
  status: string
  progress: number
  current_step: string | null
  error_message: string | null
  retry_count: number
  created_at: string
  updated_at: string
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
}

export async function uploadDocument(file: File, metadata?: Record<string, unknown>) {
  const form = new FormData()
  form.append('file', file)
  if (metadata) form.append('metadata', JSON.stringify(metadata))
  const { data } = await api.post('/documents/upload', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return data
}

export async function getTask(taskId: string): Promise<TaskStatus> {
  const { data } = await api.get(`/tasks/${taskId}`)
  return data
}

export async function listTasks(params?: Record<string, unknown>): Promise<PaginatedResponse<TaskStatus>> {
  const { data } = await api.get('/tasks/', { params })
  return data
}

export async function cancelTask(taskId: string) {
  const { data } = await api.post(`/tasks/${taskId}/cancel`)
  return data
}

export async function retryTask(taskId: string) {
  const { data } = await api.post(`/tasks/${taskId}/retry`)
  return data
}

export async function getDocument(docId: string) {
  const { data } = await api.get(`/documents/${docId}`)
  return data
}
