import { afterEach, expect, it, vi } from 'vitest'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'
import { RpcDispatcher } from '../../../dispatcher'
import { ORCHESTRATION_METHODS } from '../orchestration'
import { ORCHESTRATION_CONTRACT_VERSION } from '../../../../../../shared/orchestration/orchestration-runtime-capabilities'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it('validates the selection only after pending startup mail recovery has completed', async () => {
  const { db, runtime } = harness.setup(false)
  const selected = db.insertMessage({
    from: 'term_sender',
    to: 'term_coord',
    subject: 'leave unread when moved during recovery'
  })
  const gate = Promise.withResolvers<void>()
  const entered = Promise.withResolvers<void>()
  vi.spyOn(runtime, 'finishOrchestrationStartupMailRecovery').mockImplementation(async () => {
    entered.resolve()
    await gate.promise
    db.db.prepare('UPDATE messages SET to_handle = ? WHERE id = ?').run('term_other', selected.id)
  })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const result = dispatcher.dispatch({
    id: 'rpc_recover_mail',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: 'term_coord', ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'recover_mail'
  })
  await Promise.race([entered.promise, result])
  gate.resolve()
  await expect(result).resolves.toMatchObject({
    ok: false,
    error: { code: 'invalid_argument' }
  })
  expect(db.getMessageById(selected.id)).toMatchObject({ to_handle: 'term_other', read: 0 })
})

it('does not take Run mail reserved by its scheduled startup repoint', async () => {
  const { db, runtime, activeRunId } = harness.setup()
  const mailbox = `run:${activeRunId}`
  const selected = db.insertMessage({
    from: 'term_sender',
    to: mailbox,
    runId: activeRunId,
    subject: 'restored mail reserved for chat'
  })
  vi.useFakeTimers()
  try {
    vi.spyOn(runtime, 'deliverPendingMessagesForHandle').mockImplementation((handle) => {
      if (handle === mailbox) {
        db.reserveStructuredMailSend({
          mailbox_handle: mailbox,
          message_id: selected.id,
          session_id: 'session-restored',
          operation_id: 'op-restored',
          minted_at_ms: Date.now()
        })
      }
    })
    runtime.setOrchestrationDb(db)
    const result = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS }).dispatch({
      id: 'rpc_restored_repoint',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: 'term_coord', ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'restored_repoint'
    })
    await vi.advanceTimersByTimeAsync(2_001)
    await expect(result).resolves.toMatchObject({
      ok: false,
      error: {
        code: 'invalid_argument',
        message: expect.stringContaining('held by a structured chat')
      }
    })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
  } finally {
    vi.useRealTimers()
  }
})

it('acknowledges Run mail on a later page of restored mailbox routing', async () => {
  const { db, runtime, activeRunId } = harness.setup()
  const terminal = 'term_coord'
  const messages = Array.from({ length: 103 }, (_, index) =>
    db.insertMessage({
      from: 'term_sender',
      to: `run:${activeRunId}`,
      runId: activeRunId,
      subject: `Restored status ${index}`
    })
  )
  db.db.prepare('UPDATE messages SET to_handle = ?').run(terminal)
  runtime.setOrchestrationDb(db)
  const stillDirect = messages.filter(
    (message) => db.getMessageById(message.id)?.to_handle === terminal
  )
  expect(stillDirect).toHaveLength(53)
  const selected = stillDirect[0]!
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  await expect(
    dispatcher.dispatch({
      id: 'rpc_restored_run_mail',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal, ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'restored_run_mail'
    })
  ).resolves.toMatchObject({ ok: true, result: { markedIds: [selected.id] } })
  expect(db.getMessageById(selected.id)).toMatchObject({ to_handle: `run:${activeRunId}`, read: 1 })
  expect(messages.filter((message) => db.getMessageById(message.id)?.read === 0)).toHaveLength(102)
  expect(db.getUnreadMessages(terminal)).toEqual([])
})
