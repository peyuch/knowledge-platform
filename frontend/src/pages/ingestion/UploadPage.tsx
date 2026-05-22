import { useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { uploadDocument } from '../../api/ingestion'

export default function UploadPage() {
  const [uploading, setUploading] = useState(false)
  const [result, setResult] = useState<{ task_id: string; duplicate: boolean } | null>(null)
  const [error, setError] = useState<string | null>(null)
  const navigate = useNavigate()

  const handleDrop = useCallback(async (e: React.DragEvent) => {
    e.preventDefault()
    const file = e.dataTransfer.files[0]
    if (!file) return
    setUploading(true)
    setError(null)
    try {
      const res = await uploadDocument(file, { department: '技术部' })
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
    }
  }, [])

  const handleFileInput = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setUploading(true)
    setError(null)
    try {
      const res = await uploadDocument(file, { department: '技术部' })
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
    }
  }, [])

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">文档上传</h1>
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        className="border-2 border-dashed border-gray-300 rounded-lg p-12 text-center hover:border-blue-400 transition-colors cursor-pointer"
        onClick={() => document.getElementById('file-input')?.click()}
      >
        <input id="file-input" type="file" className="hidden" onChange={handleFileInput}
          accept=".pdf,.docx,.pptx,.xlsx,.txt,.md,.png,.jpg,.mp4,.mp3" />
        {uploading ? (
          <p className="text-blue-600">上传中...</p>
        ) : (
          <div>
            <p className="text-gray-500 text-lg mb-2">拖拽文件到此处，或点击选择</p>
            <p className="text-gray-400 text-sm">支持 PDF, DOCX, PPTX, XLSX, TXT, MD, PNG, JPG, MP4, MP3 (≤100MB)</p>
          </div>
        )}
      </div>

      {error && <div className="mt-4 p-4 bg-red-50 text-red-700 rounded">{error}</div>}

      {result && (
        <div className="mt-4 p-4 bg-green-50 text-green-700 rounded">
          <p>上传成功！任务 ID: {result.task_id}</p>
          {result.duplicate && <p className="text-yellow-600">该文件已存在，跳过解析</p>}
          <button
            onClick={() => navigate(`/tasks`)}
            className="mt-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            查看任务列表
          </button>
        </div>
      )}
    </div>
  )
}
