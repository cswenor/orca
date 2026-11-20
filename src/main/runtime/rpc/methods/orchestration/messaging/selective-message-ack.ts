import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { getMessagesByIds } from '../../../../orchestration/db/messages/messages-by-id'
import type { CheckMailReconciliation } from './check-mail-reconciliation'
import { runOwnsDirectMessageSql } from '../../../../orchestration/db/runs/run-coordinator-mail-routing'
import { OrchestrationError } from '../../../../orchestration/orchestration-error'
import { exposeMessages } from './mailbox-message-receipt'
import { ORCHESTRATION_LEGACY_RUN_ID } from '../../../../../../shared/orchestration/orchestration-rpc-contract'

export async function acknowledgeSelectedMessages(args: {
  db: OrchestrationDb
  reconciliation: CheckMailReconciliation
  mailbox: string
  ids: string[]
  direct: boolean
  runId?: string
  recordReceipt?: (receipt: unknown) => void
  mailboxIdentity?: { runId: string; dispatchId?: string }
  residual?: { mailbox: string; runId: string; revalidate: () => void }
  revalidateConsumer: () => void
}) {
  const { db, mailbox, residual } = args
  const ids = [...new Set(args.ids)]
  const isRunOwned = args.direct
    ? db.db.prepare(
        `SELECT 1 FROM messages WHERE id IN (SELECT value FROM json_each(?)) AND ${runOwnsDirectMessageSql('messages')} LIMIT 1`
      )
    : undefined
  const directIdsJson = args.direct ? JSON.stringify(ids) : undefined
  const validateSelection = () => {
    args.revalidateConsumer()
    const rows = getMessagesByIds(db, ids)
    const messages = ids.map((id) => {
      const message = rows.get(id)
      const runId =
        residual && message?.to_handle === residual.mailbox
          ? residual.runId
          : (args.mailboxIdentity?.runId ?? args.runId)
      if (
        !message ||
        (message.to_handle !== mailbox && message.to_handle !== residual?.mailbox) ||
        (runId !== undefined && message.run_id !== runId)
      ) {
        throw new OrchestrationError(
          'invalid_argument',
          'Every --ack-ids ID must exist and be addressed to the current mailbox. No messages were acknowledged.',
          { effectsApplied: false }
        )
      }
      if (
        message.delivery_contract !== 'current_delivery' ||
        message.run_id === ORCHESTRATION_LEGACY_RUN_ID
      ) {
        throw new OrchestrationError(
          'legacy_read_only',
          'Legacy orchestration messages are inspect-only. No messages were acknowledged.',
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
    if (residual && messages.some((message) => message.to_handle === residual.mailbox)) {
      residual.revalidate()
    }
    return messages
  }
  const selected = validateSelection()
  return args.reconciliation.takeSelected({
    selected: selected.filter((message) => message.read === 0),
    direct: args.direct,
    revalidate: () => {
      const current = validateSelection()
      // A routed row no longer belongs to the observed custody source.
      if (
        current.some(
          (row, i) => row.to_handle !== selected[i]?.to_handle || row.run_id !== selected[i]?.run_id
        )
      ) {
        throw new OrchestrationError(
          'consumer_fenced',
          'Selected mail moved while acknowledging it. No messages were acknowledged.',
          { effectsApplied: false }
        )
      }
    },
    respond: (messages) => {
      const marked = new Set(messages.map((message) => message.id))
      return {
        ...args.mailboxIdentity,
        messages: exposeMessages(messages.map((message) => ({ ...message, read: 1 }))),
        count: messages.length,
        acknowledged: null,
        markedIds: [...marked],
        alreadyReadIds: ids.filter((id) => !marked.has(id))
      }
    },
    record: args.recordReceipt
  })
}
