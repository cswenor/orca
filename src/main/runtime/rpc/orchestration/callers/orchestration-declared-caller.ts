import {
  isOrcaSessionId,
  parseOrcaSessionAddress
} from '../../../../../shared/agent-session/identity/orca-session-address'
import type { OrchestrationSessionCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import {
  resolveDeclaredCallerParty,
  resolveOrchestrationParty
} from '../../../orchestration/addressing/orchestration-party'
import { canonicalOrcaSessionId } from '../../../orchestration/addressing/session-lineage/canonical-orca-session-id'
import type { OrchestrationDb } from '../../../orchestration/db/orchestration-db'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { RpcRequest } from '../../core'
import type { OrchestrationCallerField } from '../../methods/orchestration/access/orchestration-method-access'

const NO_EFFECTS = { effectsApplied: false } as const

export function declaredSessionAddress(
  request: RpcRequest,
  name: OrchestrationCallerField | undefined
): string | undefined {
  const params: unknown = request.params
  if (!name || !params || typeof params !== 'object' || Array.isArray(params)) {
    return undefined
  }
  const values: Record<string, unknown> = { ...params }
  const declared = values[name]
  return typeof declared === 'string' && parseOrcaSessionAddress(declared) ? declared : undefined
}

/** A request with no session id that declares an `orca_session_id:` caller: a worker's is its handle. */
export function bindDeclaredSessionAddress(
  db: OrchestrationDb,
  request: RpcRequest,
  name: OrchestrationCallerField | undefined,
  readOnly: boolean
): RpcRequest {
  const declared = declaredSessionAddress(request, name)
  const params: unknown = request.params
  if (!name || declared === undefined || !params || typeof params !== 'object') {
    return request
  }
  const party = resolveDeclaredCallerParty(declared, db, readOnly)
  return { ...request, params: { ...params, [name]: party.address } }
}

/** Whether a declared caller names this session: any spelling that resolves to its party. */
function declaredNamesCaller(
  declared: unknown,
  caller: OrchestrationSessionCaller,
  db: OrchestrationDb,
  readOnly: boolean
): boolean {
  if (typeof declared !== 'string') {
    return false
  }
  if (isOrcaSessionId(declared)) {
    return canonicalOrcaSessionId(declared) === caller.orcaSessionId
  }
  try {
    return resolveOrchestrationParty(declared, db, readOnly).address === caller.address
  } catch {
    return false
  }
}

/** The declared caller, if any, must name this session; it is then replaced by its address. */
export function bindDeclaredCaller(
  name: OrchestrationCallerField | undefined,
  params: unknown,
  caller: OrchestrationSessionCaller,
  db: OrchestrationDb,
  readOnly: boolean
): unknown {
  if (!name || !params || typeof params !== 'object' || Array.isArray(params)) {
    return params
  }
  const values: Record<string, unknown> = { ...params }
  const declared = values[name]
  if (declared !== undefined && !declaredNamesCaller(declared, caller, db, readOnly)) {
    throw consumerFenced(caller, String(declared))
  }
  // Check restart fallback may use only the session's own pane key.
  if (values.terminalPaneKey !== undefined && values.terminalPaneKey !== caller.paneKey) {
    throw consumerFenced(caller, `pane ${String(values.terminalPaneKey)}`)
  }
  values[name] = caller.address
  return values
}

function consumerFenced(caller: OrchestrationSessionCaller, declared: string): OrchestrationError {
  return new OrchestrationError(
    'consumer_fenced',
    `This caller is agent session ${caller.sessionId} and cannot act as ${declared}. No effects were applied.`,
    NO_EFFECTS
  )
}
