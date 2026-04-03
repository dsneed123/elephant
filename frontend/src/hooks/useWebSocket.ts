import { useEffect, useRef } from 'react'
import type { TradeSignal, CopiedTrade } from '../types'

export type WSEvent =
  | { type: 'signal_created'; payload: TradeSignal }
  | { type: 'trade_updated'; payload: CopiedTrade }

type WSHandlers = {
  onSignalCreated?: (signal: TradeSignal) => void
  onTradeUpdated?: (trade: CopiedTrade) => void
}

const WS_URL = 'ws://localhost:8000/ws'
const MAX_RECONNECT_DELAY = 30_000

export function useWebSocket(handlers: WSHandlers) {
  const handlersRef = useRef(handlers)
  handlersRef.current = handlers

  useEffect(() => {
    let ws: WebSocket | null = null
    let reconnectDelay = 1_000
    let stopped = false

    function connect() {
      ws = new WebSocket(WS_URL)

      ws.onopen = () => {
        reconnectDelay = 1_000
      }

      ws.onmessage = (event) => {
        let msg: WSEvent
        try {
          msg = JSON.parse(event.data as string) as WSEvent
        } catch {
          return
        }
        if (msg.type === 'signal_created') {
          handlersRef.current.onSignalCreated?.(msg.payload)
        } else if (msg.type === 'trade_updated') {
          handlersRef.current.onTradeUpdated?.(msg.payload)
        }
      }

      ws.onclose = () => {
        if (stopped) return
        setTimeout(connect, reconnectDelay)
        reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY)
      }

      ws.onerror = () => {
        ws?.close()
      }
    }

    connect()

    return () => {
      stopped = true
      ws?.close()
    }
  }, [])
}
