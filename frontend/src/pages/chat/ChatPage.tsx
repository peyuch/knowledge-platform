import { useState, useRef, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

interface Message {
  role: 'user' | 'assistant'
  content: string
  citations?: Array<{chunk_id: string; doc_id: string; page_start?: number; quote: string}>
  isFallback?: boolean
  timestamp: Date
}

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const navigate = useNavigate()

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleSend = async () => {
    if (!input.trim() || loading) return
    const userMsg: Message = { role: 'user', content: input, timestamp: new Date() }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)

    try {
      const resp = await fetch('/api/v1/search/answer', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('token') || 'dev-token'}`,
        },
        body: JSON.stringify({ query: userMsg.content, top_k: 10 }),
      })
      const data = await resp.json()
      const assistantMsg: Message = {
        role: 'assistant',
        content: data.answer || '未找到相关信息',
        citations: data.citations,
        isFallback: data.is_fallback,
        timestamp: new Date(),
      }
      setMessages(prev => [...prev, assistantMsg])
    } catch {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: '请求失败，请检查服务是否运行',
        timestamp: new Date(),
      }])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 && (
          <div className="text-center text-gray-400 mt-20">
            <p className="text-2xl mb-4">企业知识中台</p>
            <p>输入问题开始智能问答，例如：</p>
            <div className="mt-4 space-y-2">
              {['请假流程需要谁审批？', '合同审查流程是什么？', '数据安全制度有哪些要求？'].map(q => (
                <button key={q} onClick={() => { setInput(q); handleSend() }}
                  className="block mx-auto px-4 py-2 bg-gray-100 rounded-lg hover:bg-blue-50 text-sm text-gray-600">
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-2xl rounded-xl px-4 py-3 ${
              msg.role === 'user' ? 'bg-blue-600 text-white' : 'bg-white border shadow-sm'
            }`}>
              <div className="whitespace-pre-wrap text-sm">{msg.content}</div>
              {msg.citations && msg.citations.length > 0 && (
                <div className="mt-3 pt-2 border-t border-gray-200">
                  <p className="text-xs text-gray-500 mb-1">引用来源：</p>
                  {msg.citations.map((c, j) => (
                    <div key={j} className="text-xs text-gray-500 mb-1">
                      [{j + 1}] {c.quote?.slice(0, 80)}...
                      {c.page_start && <span className="ml-1">(第{c.page_start}页)</span>}
                    </div>
                  ))}
                </div>
              )}
              {msg.isFallback && (
                <div className="mt-2 text-xs text-orange-600">⚠ AI 生成超时，已切换为精准搜索</div>
              )}
            </div>
          </div>
        ))}
        {loading && (
          <div className="flex justify-start">
            <div className="bg-white border shadow-sm rounded-xl px-4 py-3">
              <div className="flex gap-1">
                <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{animationDelay: '0ms'}}/>
                <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{animationDelay: '150ms'}}/>
                <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{animationDelay: '300ms'}}/>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="border-t bg-white p-4">
        <div className="max-w-4xl mx-auto flex gap-3">
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSend()}
            placeholder="输入问题，按 Enter 发送..."
            className="flex-1 px-4 py-3 border rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500"
            disabled={loading}
          />
          <button onClick={handleSend} disabled={loading || !input.trim()}
            className="px-6 py-3 bg-blue-600 text-white rounded-xl hover:bg-blue-700 disabled:opacity-50 font-medium">
            发送
          </button>
        </div>
      </div>
    </div>
  )
}
