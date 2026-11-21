import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { RpcRequest } from '../../core'

export function isSelectiveAckRequest({ method }: RpcRequest): boolean {
  return method === 'orchestration.ackMessages'
}

/** The CLI probes its local caller without publishing an interaction before a selective command. */
export function isCallerProbe({ method, params }: RpcRequest): boolean {
  return (
    method === 'orchestration.callerShow' &&
    params !== null &&
    typeof params === 'object' &&
    'readOnly' in params &&
    params.readOnly === true
  )
}

export function selectiveAckPeerError(): OrchestrationError {
  return new OrchestrationError(
    'invalid_argument',
    'Selective acknowledgment is unavailable to peer-Orca federation callers because retry receipts cannot be bound safely.',
    { effectsApplied: false }
  )
}

export function selectiveAckStartupUnavailable(): OrchestrationError {
  return new OrchestrationError(
    'runtime_unavailable',
    'Startup mailbox recovery or terminal restore is still running. No messages were acknowledged.',
    { effectsApplied: false }
  )
}
