import { createContext, useContext, useEffect, useRef, useState } from 'react'

export interface WsEvent {
  type: string
  payload: unknown
}

interface WebSocketContextValue {
  latestEvent: WsEvent | null
  connected: boolean
}

const WebSocketContext = createContext<WebSocketContextValue>({
  latestEvent: null,
  connected: false,
})

const BASE_BACKOFF_MS = 1_000
const MAX_BACKOFF_MS = 60_000

function getWsUrl(): string {
  const apiUrl = import.meta.env.VITE_API_URL as string | undefined
  if (apiUrl) {
    return apiUrl.replace(/^http/, 'ws') + '/ws'
  }
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${proto}://${window.location.host}/ws`
}

export function WebSocketProvider({ children }: { children: React.ReactNode }) {
  const [latestEvent, setLatestEvent] = useState<WsEvent | null>(null)
  const [connected, setConnected] = useState(false)
  const attemptRef = useRef(0)
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const unmountedRef = useRef(false)

  useEffect(() => {
    unmountedRef.current = false

    function connect() {
      if (unmountedRef.current) return

      const ws = new WebSocket(getWsUrl())
      wsRef.current = ws

      ws.onopen = () => {
        if (unmountedRef.current) {
          ws.close()
          return
        }
        setConnected(true)
        attemptRef.current = 0
      }

      ws.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data as string) as WsEvent
          setLatestEvent(msg)
        } catch {
          // ignore malformed messages
        }
      }

      ws.onclose = () => {
        if (unmountedRef.current) return
        setConnected(false)
        const backoff = Math.min(BASE_BACKOFF_MS * 2 ** attemptRef.current, MAX_BACKOFF_MS)
        attemptRef.current += 1
        timeoutRef.current = setTimeout(connect, backoff)
      }

      ws.onerror = () => {
        ws.close()
      }
    }

    connect()

    return () => {
      unmountedRef.current = true
      if (timeoutRef.current !== null) clearTimeout(timeoutRef.current)
      wsRef.current?.close()
    }
  }, [])

  return (
    <WebSocketContext.Provider value={{ latestEvent, connected }}>
      {children}
    </WebSocketContext.Provider>
  )
}

export function useWebSocket() {
  return useContext(WebSocketContext)
}
