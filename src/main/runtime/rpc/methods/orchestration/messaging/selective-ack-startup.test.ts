import { afterEach, expect, it, vi } from 'vitest'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'
import { RpcDispatcher } from '../../../dispatcher'
import { ORCHESTRATION_METHODS } from '../orchestration'
import { ORCHESTRATION_CONTRACT_VERSION } from '../../../../../../shared/orchestration/orchestration-runtime-capabilities'
import { OrchestrationMailboxOwner } from '../../../../orchestration/mailbox/routing/mailbox-owner'
import { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { MAILBOX_POINTER_ENTER_ATTEMPTED } from '../../../../orchestration/db/messages/mailbox-pointer-enter-state'
import { createRootDispatch } from '../../../../orchestration/db/root-dispatch-test-fixture'
import { markUnreadFollowUpMail } from '../../../../orchestration/db/dispatch-context/worker-follow-up-lineage'
import { continueAllUnreadWorkerMail } from './settled-worker-follow-through'
import {
  createBoundRun,
  createRuntime,
  driveToLiveIdle,
  pointerCount,
  PANE_KEY,
  PTY_ID,
  registerSecondPane,
  SECOND_PANE_KEY,
  SECOND_PTY_ID,
  SECOND_TERMINAL_HANDLE,
  TERMINAL_HANDLE
} from '../../../../orchestration/mailbox/terminal-pointer/orchestration-mailbox-notification-test-harness'
import { WRITE_ACCEPTED } from '../../../../../../shared/terminal/pty/lifecycle/pty-write-settlement'

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
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockImplementation(async () => {
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

it('refuses an unknown retry without waiting for scheduled mailbox recovery', async () => {
  const { db, runtime } = harness.setup(false)
  const selected = db.insertMessage({
    from: 'term_sender',
    to: 'term_coord',
    subject: 'still unread'
  })
  const recovery = Promise.withResolvers<void>()
  const waiting = Promise.withResolvers<'waiting'>()
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockImplementation(() => {
    waiting.resolve('waiting')
    return recovery.promise
  })
  const result = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS }).dispatch({
    id: 'rpc_unknown_retry_startup',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: 'term_coord', ackIds: [selected.id], replayOnly: true },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'unknown_retry_startup'
  })
  try {
    await expect(Promise.race([result, waiting.promise])).resolves.toMatchObject({
      ok: false,
      error: { code: 'operation_unknown' }
    })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
  } finally {
    recovery.resolve()
    await result
  }
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

it('waits for every PTY routing page before acknowledging direct-addressed Run mail', async () => {
  const { db, runtime, activeRunId } = harness.setup()
  const mailbox = `run:${activeRunId}`
  const terminal = {
    ptyId: 'restored-pty',
    handle: 'term_coord',
    paneKeys: [harness.coordinatorPaneKey],
    lastAgentStatus: null,
    lastAgentStatusObservedLive: false
  }
  const owner = new OrchestrationMailboxOwner({
    getDb: () => db,
    getTerminal: () => terminal,
    getTerminalProcessIncarnation: () => null,
    onRoutedMessageTypes: () => {},
    onForeignMailboxRouted: () => {}
  })
  const messages = Array.from({ length: 103 }, (_, index) =>
    db.insertMessage({
      from: 'term_sender',
      to: mailbox,
      runId: activeRunId,
      subject: `PTY restore ${index}`
    })
  )
  db.db.prepare('UPDATE messages SET to_handle = ?').run(terminal.handle)
  owner.resolve(terminal, terminal.handle)
  const selected = messages.findLast(
    (message) => db.getMessageById(message.id)?.to_handle === terminal.handle
  )!
  const completion = owner.pendingDirectReconciliation(
    terminal.ptyId,
    db.getMessageSequenceWatermark()
  )
  const liveMail = Array.from({ length: 100 }, (_, index) => {
    const message = db.insertMessage({
      from: 'term_sender',
      to: mailbox,
      runId: activeRunId,
      subject: `Arrived after the PTY recovery started ${index}`
    })
    db.db.prepare('UPDATE messages SET to_handle = ? WHERE id = ?').run(terminal.handle, message.id)
    return message
  })
  const pendingLiveMail = completion?.then(() =>
    liveMail.some((message) => db.getMessageById(message.id)?.to_handle === terminal.handle)
  )
  const allRouting = owner.pendingDirectReconciliation(
    terminal.ptyId,
    db.getMessageSequenceWatermark()
  )
  expect(allRouting).toBeDefined()
  expect(completion).toBeDefined()
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockImplementation(() =>
    completion?.then(() => undefined)
  )
  await expect(
    new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS }).dispatch({
      id: 'rpc_restored_pty_mail',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: terminal.handle, ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'restored_pty_mail'
    })
  ).resolves.toMatchObject({ ok: true, result: { markedIds: [selected.id] } })
  expect(db.getMessageById(selected.id)).toMatchObject({ to_handle: mailbox, read: 1 })
  await expect(pendingLiveMail).resolves.toBe(true)
  await allRouting
  expect(db.getUnreadMessages(terminal.handle)).toEqual([])
})

it.each([false, true])('settles a restored PTY pointer before ACK (chat: %s)', async (fromChat) => {
  vi.useFakeTimers()
  const db = new OrchestrationDb(':memory:')
  const probe = Promise.withResolvers<boolean>()
  const withdrawn = {
    epoch: '',
    state: { kind: 'withdrawn' },
    body: null,
    withdrawal: 'system'
  } as const
  const handback = Promise.withResolvers<typeof withdrawn>()
  const text = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  const enter = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  const liveText = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  try {
    const { runtime } = await createRuntime(db)
    await driveToLiveIdle(runtime)
    const write = vi.fn((_ptyId: string, _data: string) => true)
    runtime.setPtyController({
      write,
      writeWithSettlement: (ptyId, data) => {
        write(ptyId, data)
        return data === '\r'
          ? enter.promise
          : pointerCount(write) > 1
            ? liveText.promise
            : text.promise
      },
      kill: vi.fn(),
      probePtyLiveness: () => probe.promise,
      getForegroundProcess: async () => null
    })
    const run = createBoundRun(db, 'Restored PTY')
    const selected = db.insertMessage({
      from: 'term_sender',
      to: `run:${run.id}`,
      runId: run.id,
      subject: 'Restored while terminal is idle'
    })
    const alreadyRead = db.insertMessage({
      from: 'term_sender',
      to: `run:${run.id}`,
      runId: run.id,
      subject: 'Read independently before the staged check is submitted'
    })
    if (fromChat) {
      const host = runtime['orchestrationStructuredMailboxPointerDelivery']['deps'].host
      vi.spyOn(host, 'observeMail').mockReturnValue(handback.promise)
      for (const message of [selected, alreadyRead]) {
        expect(
          db.reserveStructuredMailSend({
            mailbox_handle: `run:${run.id}`,
            message_id: message.id,
            session_id: 'previous-chat',
            operation_id: `send-${message.id}`,
            minted_at_ms: Date.now()
          })
        ).toBe(true)
      }
    }
    runtime.setOrchestrationDb(db)
    const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
    const result = dispatcher.dispatch({
      id: 'rpc_restored_pointer',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: TERMINAL_HANDLE, ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'restored_pointer'
    })
    await vi.advanceTimersByTimeAsync(2_001)
    expect(pointerCount(write)).toBe(0)
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    handback.resolve(withdrawn)
    await vi.advanceTimersByTimeAsync(25)
    expect(db.getStructuredMailSends(`run:${run.id}`)).toEqual([])
    expect(pointerCount(write)).toBe(0)
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    probe.resolve(true)
    await vi.advanceTimersByTimeAsync(2_001)
    expect(pointerCount(write)).toBe(1)
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    const later = db.insertMessage({
      from: 'term_sender',
      to: `run:${run.id}`,
      runId: run.id,
      subject: 'Arrived after startup while the restored write was pending'
    })
    runtime.deliverPendingMessagesForHandle(`run:${run.id}`)
    db.markAsRead([alreadyRead.id])
    text.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(500)
    expect(write.mock.calls.filter(([, data]) => data === '\r')).toHaveLength(1)
    expect(db.getMessageById(selected.id)).toMatchObject({
      read: 0,
      pointer_enter_pending: MAILBOX_POINTER_ENTER_ATTEMPTED
    })
    enter.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(25)
    expect(pointerCount(write)).toBe(2)
    expect(db.getMessageById(later.id)?.read).toBe(0)
    const stillWaiting = Symbol()
    const observed = await Promise.race([result, Promise.resolve(stillWaiting)])
    expect(observed).not.toBe(stillWaiting)
    await expect(result).resolves.toMatchObject({
      ok: true,
      result: { markedIds: [selected.id] }
    })
    expect(db.getMessageById(selected.id)).toMatchObject({ read: 1, pointer_enter_pending: 0 })
  } finally {
    handback.resolve(withdrawn)
    probe.resolve(true)
    text.resolve(WRITE_ACCEPTED)
    enter.resolve(WRITE_ACCEPTED)
    liveText.resolve(WRITE_ACCEPTED)
    vi.useRealTimers()
    db.close()
  }
})

it('waits for the startup pointer retry when the Run moves to a different PTY', async () => {
  vi.useFakeTimers()
  const db = new OrchestrationDb(':memory:')
  const replacementText = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  try {
    const { runtime } = await createRuntime(db)
    await registerSecondPane(runtime)
    await driveToLiveIdle(runtime)
    runtime.onPtyData(SECOND_PTY_ID, '\x1b]0;Codex done\x07', 1)
    await runtime.serializeMainTerminalBuffer(SECOND_PTY_ID)
    const write = vi.fn((_ptyId: string, _data: string) => true)
    runtime.setPtyController({
      write,
      writeWithSettlement: (ptyId, data) => {
        write(ptyId, data)
        return ptyId === SECOND_PTY_ID && data !== '\r' ? replacementText.promise : WRITE_ACCEPTED
      },
      kill: vi.fn(),
      probePtyLiveness: async () => true,
      getForegroundProcess: async () => null
    })
    const run = createBoundRun(db, 'Restored PTY changes owner')
    const selected = db.insertMessage({
      from: 'term_sender',
      to: `run:${run.id}`,
      runId: run.id,
      subject: 'Retry on the replacement terminal'
    })
    runtime.setOrchestrationDb(db)
    await vi.advanceTimersByTimeAsync(2_001)
    expect(pointerCount(write)).toBe(1)
    db.bindRun({
      runId: run.id,
      coordinatorHandle: SECOND_TERMINAL_HANDLE,
      coordinatorPaneKey: SECOND_PANE_KEY
    })
    const result = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS }).dispatch({
      id: 'rpc_restored_pointer_retry',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: SECOND_TERMINAL_HANDLE, ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'restored_pointer_retry'
    })
    await vi.advanceTimersByTimeAsync(1_000)
    expect(pointerCount(write)).toBe(2)
    expect(write.mock.calls.map(([ptyId]) => ptyId)).toEqual([PTY_ID, SECOND_PTY_ID])
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    replacementText.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(1_000)
    await expect(result).resolves.toMatchObject({
      ok: true,
      result: { markedIds: [selected.id] }
    })
    expect(write.mock.calls.at(-1)).toEqual([SECOND_PTY_ID, '\r'])
  } finally {
    replacementText.resolve(WRITE_ACCEPTED)
    vi.useRealTimers()
    db.close()
  }
})

it('joins the PTY notice created by restored worker recovery', async () => {
  vi.useFakeTimers()
  const db = new OrchestrationDb(':memory:')
  const probe = Promise.withResolvers<boolean>()
  try {
    const { runtime } = await createRuntime(db)
    await driveToLiveIdle(runtime)
    const write = vi.fn((_ptyId: string, _data: string) => true)
    runtime.setPtyController({
      write,
      writeWithSettlement: (ptyId, data) => {
        write(ptyId, data)
        return WRITE_ACCEPTED
      },
      probePtyLiveness: () => probe.promise,
      kill: vi.fn(),
      getForegroundProcess: async () => null
    })
    const run = createBoundRun(db, 'Interrupted worker')
    const task = db.createTask({ runId: run.id, spec: 'Restore worker' })
    const worker = createRootDispatch(db, task.id, 'term_worker')
    db.db
      .prepare(
        `INSERT INTO worker_dispatches (dispatch_id, runtime_epoch, state, stage, agent_terminal_handle)
         VALUES (?, 'previous-epoch', 'starting', 'authority_attached', 'term_worker')`
      )
      .run(worker.id)
    db.insertMessage({
      from: TERMINAL_HANDLE,
      to: `dispatch:${worker.id}`,
      runId: run.id,
      subject: 'Mail already assigned to the worker'
    })
    markUnreadFollowUpMail(db, worker.id)
    const done = vi.fn()
    const recovery = continueAllUnreadWorkerMail(runtime, db).then(done)
    await vi.advanceTimersByTimeAsync(2_001)
    expect(db.getUnreadMessages(`run:${run.id}`)).toMatchObject([
      { subject: 'Mail did not reach the finished worker' }
    ])
    expect(done).not.toHaveBeenCalled()
    probe.resolve(true)
    await vi.advanceTimersByTimeAsync(2_001)
    await recovery
    expect(pointerCount(write)).toBe(1)
    expect(write.mock.calls.filter(([, data]) => data === '\r')).toHaveLength(1)
  } finally {
    probe.resolve(true)
    vi.useRealTimers()
    db.close()
  }
})

it('joins a recovery notice queued for automatic redrive behind an earlier PTY pointer', async () => {
  vi.useFakeTimers()
  const db = new OrchestrationDb(':memory:')
  const precedingWrite = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  try {
    const { runtime } = await createRuntime(db)
    await driveToLiveIdle(runtime)
    const write = vi.fn((_ptyId: string, _data: string) => true)
    const precedingRun = createBoundRun(db, 'Preceding mailbox on the same PTY')
    const noticeRun = createBoundRun(db, 'Recovery notice mailbox')
    const precedingMailbox = `run:${precedingRun.id}`
    const noticeMailbox = `run:${noticeRun.id}`
    const precedingFlag = `--run ${precedingRun.id}`
    runtime.setPtyController({
      write,
      writeWithSettlement: (ptyId, data) => {
        write(ptyId, data)
        return data.includes(precedingFlag) ? precedingWrite.promise : WRITE_ACCEPTED
      },
      kill: vi.fn(),
      getForegroundProcess: async () => null
    })
    db.insertMessage({ from: 'term_sender', to: precedingMailbox, subject: 'Earlier mail' })
    runtime.deliverPendingMessagesForHandle(precedingMailbox)
    await vi.advanceTimersByTimeAsync(10)
    expect(pointerCount(write)).toBe(1)

    const notice = db.insertMessage({
      from: 'term_sender',
      to: noticeMailbox,
      subject: 'Recovery notice'
    })
    const recovered = vi.fn()
    const recovery = runtime.notifyRestoredMessageArrived(noticeMailbox, notice.type, notice.id)
    void recovery.then(recovered)
    await vi.advanceTimersByTimeAsync(2_001)
    expect(pointerCount(write)).toBe(1)
    expect(recovered).not.toHaveBeenCalled()

    precedingWrite.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(1_500)
    await recovery
    expect(pointerCount(write)).toBe(2)
    expect(write.mock.calls.some(([, data]) => data.includes(`--run ${noticeRun.id}`))).toBe(true)
    expect(write.mock.calls.filter(([, data]) => data === '\r')).toHaveLength(2)
  } finally {
    precedingWrite.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(1_500)
    vi.useRealTimers()
    db.close()
  }
})

it('does not join a different mailbox pointer when repointing a settled recovery notice', async () => {
  vi.useFakeTimers()
  const db = new OrchestrationDb(':memory:')
  const unrelatedWrite = Promise.withResolvers<typeof WRITE_ACCEPTED>()
  try {
    const { runtime } = await createRuntime(db)
    await driveToLiveIdle(runtime)
    const write = vi.fn((_ptyId: string, _data: string) => true)
    const unrelatedRun = createBoundRun(db, 'Independent mailbox on the same PTY')
    const restoredRun = createBoundRun(db, 'Recovery notice mailbox')
    const unrelatedMailbox = `run:${unrelatedRun.id}`
    const unrelatedFlag = `--run ${unrelatedRun.id}`
    const noticeMailbox = `run:${restoredRun.id}`
    runtime.setPtyController({
      write,
      writeWithSettlement: (ptyId, data) => {
        write(ptyId, data)
        return data.includes(unrelatedFlag) ? unrelatedWrite.promise : WRITE_ACCEPTED
      },
      kill: vi.fn(),
      getForegroundProcess: async () => null
    })
    db.insertMessage({
      from: 'term_sender',
      to: unrelatedMailbox,
      subject: 'Independent later mail'
    })
    const notice = db.insertMessage({
      from: 'term_sender',
      to: noticeMailbox,
      subject: 'Recovery notice'
    })
    const recovered = vi.fn()
    void runtime.notifyRestoredMessageArrived(noticeMailbox, notice.type, notice.id).then(recovered)
    await vi.advanceTimersByTimeAsync(750)
    expect(pointerCount(write)).toBe(1)
    expect(write.mock.calls.at(-1)).toEqual([PTY_ID, '\r'])
    runtime.deliverPendingMessagesForHandle(unrelatedMailbox)
    await vi.advanceTimersByTimeAsync(1_300)
    expect(pointerCount(write)).toBe(2)
    expect(write.mock.calls.at(-1)?.[1]).toContain(unrelatedFlag)
    expect(recovered).toHaveBeenCalledOnce()
  } finally {
    unrelatedWrite.resolve(WRITE_ACCEPTED)
    await vi.advanceTimersByTimeAsync(500)
    vi.useRealTimers()
    db.close()
  }
})

it('leaves worker follow-up markers created after startup to ordinary delivery', async () => {
  const { db, runtime } = harness.setup(false)
  const run = createBoundRun(db, 'Report arrives after startup was scheduled')
  const task = db.createTask({ runId: run.id, spec: 'Worker that reported after startup' })
  const worker = createRootDispatch(db, task.id, 'term_worker')
  db.db
    .prepare(
      `INSERT INTO worker_dispatches (dispatch_id, runtime_epoch, state, stage, agent_terminal_handle)
       VALUES (?, 'previous-epoch', 'starting', 'authority_attached', 'term_worker')`
    )
    .run(worker.id)
  const recovery = continueAllUnreadWorkerMail(runtime, db, () => true)
  const later = db.insertMessage({
    from: 'term_coord',
    to: `dispatch:${worker.id}`,
    runId: run.id,
    subject: 'First mail for a separately marked worker'
  })
  markUnreadFollowUpMail(db, worker.id)
  await recovery
  expect(db.getUnreadMessages(`run:${run.id}`)).toEqual([])
  expect(db.getMessageById(later.id)).toMatchObject({
    to_handle: `dispatch:${worker.id}`,
    read: 0
  })
})

it.each(['headless', 'window', 'reload', 'explicit'])('marks %s mail', async (mode) => {
  const db = new OrchestrationDb(':memory:')
  try {
    const { runtime, write } = await createRuntime(db, {
      launchAgent: null,
      headless: mode === 'headless'
    })
    const handle =
      mode === 'window' ? runtime.getTerminalHandleForPaneKey(PANE_KEY) : TERMINAL_HANDLE
    expect(handle).toBeTruthy()
    const reloading = mode === 'reload' || mode === 'explicit'
    if (reloading) {
      expect(runtime.markRendererReloading(1)).not.toBeNull()
      expect(runtime['handles'].has(TERMINAL_HANDLE)).toBe(false)
      vi.spyOn(runtime.startupWorkerAuthority, 'isPending').mockReturnValue(true)
    }
    expect(runtime.getExistingTerminalHandleForPaneKey(PANE_KEY)).toBe(handle)
    const run = createBoundRun(db, 'Pane-only caller')
    const selected = db.insertMessage({
      from: 'term_sender',
      to: `run:${run.id}`,
      runId: run.id,
      subject: 'Selected by pane'
    })
    const response = await new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS }).dispatch({
      id: 'rpc_pane_caller',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: {
        ...(mode === 'explicit' ? { terminal: handle } : { terminalPaneKey: PANE_KEY }),
        ackIds: [selected.id]
      },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'pane_caller'
    })
    if (!response.ok) {
      throw new Error(`${response.error.code}: ${response.error.message}`)
    }
    expect(response.result).toMatchObject({ markedIds: [selected.id] })
    expect(db.getMessageById(selected.id)?.read).toBe(1)
    if (reloading) {
      expect(runtime['handles'].has(TERMINAL_HANDLE)).toBe(false)
    }
    expect(write).not.toHaveBeenCalled()
  } finally {
    db.close()
  }
})

it('an ordinary Run check takes the next batch if a selective ACK empties its claim', async () => {
  const { db, ctx, activeRunId } = harness.setup()
  const terminal = 'term_coord'
  const params = { terminal, run: activeRunId }
  const insert = (subject: string) =>
    db.insertMessage({
      from: 'term_sender',
      to: `run:${activeRunId}`,
      runId: activeRunId,
      subject
    })
  const selected = insert('Stale status')
  const original = db.getOrCreateMailboxDelivery.bind(db)
  let marked: Promise<unknown> | undefined
  let newerId: string | undefined
  vi.spyOn(db, 'getOrCreateMailboxDelivery').mockImplementationOnce((options) => {
    const claim = original(options)
    expect(claim?.messages.map(({ id }) => id)).toEqual([selected.id])
    newerId = insert('Keep this for the ordinary check').id
    marked = harness.call('orchestration.ackMessages', { ...params, ackIds: [selected.id] }, ctx)
    return claim
  })
  const checked = await harness.call('orchestration.check', { ...params, wait: true }, ctx)
  await expect(marked).resolves.toMatchObject({ markedIds: [selected.id] })
  expect(checked).toMatchObject({ count: 1, messages: [{ id: newerId }] })
})
