import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { getMessagesByIds } from '../../../../orchestration/db/messages/messages-by-id'
import {
  activeDispatchOwnsAddressSql,
  runOwnsDirectMessageSql
} from '../../../../orchestration/db/runs/run-coordinator-mail-routing'
import { OrchestrationError } from '../../../../orchestration/orchestration-error'
import { ORCHESTRATION_LEGACY_RUN_ID } from '../../../../../../shared/orchestration/orchestration-rpc-contract'
import type { MessageRow } from '../../../../orchestration/db/orchestration-records'

export function isSelectedDispatchMessage(
  message: MessageRow,
  dispatch: { mailbox: string; runId: string; caller: string; assignee: string | null }
): boolean {
  return (
    message.run_id === dispatch.runId &&
    (message.to_handle === dispatch.mailbox ||
      message.to_handle === dispatch.caller ||
      message.to_handle === dispatch.assignee)
  )
}

export function acknowledgeSelectedMessages(args: {
  db: OrchestrationDb
  mailbox: string
  ids: string[]
  direct: boolean
  runId?: string
  recordReceipt?: (receipt: unknown) => void
  mailboxIdentity?: { runId: string; dispatchId?: string }
  routeSource?: {
    caller: string
    assignee: string | null
    excludeDispatchOwned?: boolean
  }
  residual?: Parameters<typeof isSelectedDispatchMessage>[1] & { revalidate: () => void }
}) {
  const { db, mailbox, residual } = args
  const { routeSource } = args
  const ids = [...new Set(args.ids)]
  const isRunOwned = args.direct
    ? db.db.prepare(
        `SELECT 1 FROM messages WHERE id IN (SELECT value FROM json_each(?)) AND ${runOwnsDirectMessageSql('messages')} LIMIT 1`
      )
    : undefined
  const directIdsJson = args.direct ? JSON.stringify(ids) : undefined
  const ownedByDispatch = routeSource?.excludeDispatchOwned
    ? db.db.prepare(
        `SELECT 1 FROM messages WHERE id = ? AND ${activeDispatchOwnsAddressSql(
          'messages.run_id',
          'messages.to_handle'
        )}`
      )
    : undefined
  const validateSelection = () => {
    const rows = getMessagesByIds(db, ids)
    let needsResidualRevalidation = false
    const messages = ids.map((id) => {
      const message = rows.get(id)
      const residualMatch = Boolean(
        message && residual && isSelectedDispatchMessage(message, residual)
      )
      needsResidualRevalidation ||= residualMatch
      const runId = args.mailboxIdentity?.runId ?? args.runId
      if (
        !message ||
        (!residualMatch &&
          ((message.to_handle !== mailbox &&
            !(
              routeSource &&
              (message.to_handle === routeSource.caller ||
                message.to_handle === routeSource.assignee) &&
              !ownedByDispatch?.get(message.id)
            )) ||
            (runId !== undefined && message.run_id !== runId)))
      ) {
        throw new OrchestrationError(
          'invalid_argument',
          'Every --ack-ids ID must exist and be addressed to the current mailbox. No messages were acknowledged.',
          { effectsApplied: false }
        )
      }
      if (
        message.delivery_contract !== 'current_delivery' ||
        message.run_id === ORCHESTRATION_LEGACY_RUN_ID ||
        (message.run_id && db.getPeerRun(message.run_id))
      ) {
        throw new OrchestrationError(
          'legacy_read_only',
          'Legacy and peer Run messages do not support selective acknowledgment.',
          { effectsApplied: false }
        )
      }
      if (message.type === 'worker_done' || message.type === 'heartbeat') {
        throw new OrchestrationError(
          'invalid_argument',
          'Selected mail is a lifecycle report. Use a normal check for that mail. No messages were acknowledged.',
          { effectsApplied: false }
        )
      }
      return message
    })
    if (directIdsJson !== undefined && isRunOwned?.get(directIdsJson)) {
      throw new OrchestrationError(
        'consumer_fenced',
        'Selected mail belongs to a Run. Acknowledge it from that Run consumer.',
        { effectsApplied: false }
      )
    }
    if (needsResidualRevalidation) {
      residual?.revalidate()
    }
    return messages
  }
  const messages = validateSelection()
  if (db.getStructuredMailSendsForMessages(ids).length) {
    throw new OrchestrationError(
      'invalid_argument',
      'Selected mail is held by a structured chat. Use a normal check for that mail. No messages were acknowledged.',
      { effectsApplied: false }
    )
  }
  const markedIds = messages.filter((message) => message.read === 0).map((message) => message.id)
  if (markedIds.length) {
    db.db
      .prepare('UPDATE messages SET read = 1 WHERE id IN (SELECT value FROM json_each(?))')
      .run(JSON.stringify(markedIds))
  }
  const marked = new Set(markedIds)
  const response = {
    ...args.mailboxIdentity,
    count: markedIds.length,
    acknowledged: null,
    markedIds,
    alreadyReadIds: ids.filter((id) => !marked.has(id))
  }
  args.recordReceipt?.(response)
  return response
}
