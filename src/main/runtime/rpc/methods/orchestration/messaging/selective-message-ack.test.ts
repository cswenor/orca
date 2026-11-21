import { afterEach, expect, it, vi } from 'vitest'
import {
  createRootDispatch,
  reattachDispatchConsumer
} from '../../../../orchestration/db/root-dispatch-test-fixture'
import { MUTATION_RECEIPT_MAX_ROWS } from '../../../../orchestration/db/mutation-receipts/mutation-receipt-capacity'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'
import { RpcDispatcher } from '../../../dispatcher'
import { ORCHESTRATION_METHODS } from '../orchestration'
import { ORCHESTRATION_CONTRACT_VERSION } from '../../../../../../shared/orchestration/orchestration-runtime-capabilities'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it('acknowledges only selected recipient IDs, atomically rejects mixed recipients, and tolerates retries', async () => {
  const { db, ctx } = harness.setup(false)
  const check = (params: Record<string, unknown>) =>
    harness.call(
      params.ackIds ? 'orchestration.ackMessages' : 'orchestration.check',
      { terminal: 'term_coord', ...params },
      ctx
    )
  db.insertMessage({ id: 'msg_stale', from: 'sender', to: 'term_coord', subject: 'old status' })
  await check({ peek: true })
  db.insertMessage({
    id: 'msg_question',
    from: 'sender',
    to: 'term_coord',
    subject: 'new question',
    type: 'question'
  })
  db.insertMessage({ id: 'msg_foreign', from: 'sender', to: 'term_other', subject: 'other inbox' })

  await expect(check({ ackIds: ['msg_stale', 'msg_foreign'] })).rejects.toMatchObject({
    code: 'invalid_argument',
    data: { effectsApplied: false }
  })
  await expect(check({ ackIds: ['msg_stale', 'msg_missing'] })).rejects.toMatchObject({
    code: 'invalid_argument',
    data: { effectsApplied: false }
  })
  expect(db.getUnreadMessages('term_coord').map((message) => message.id)).toEqual([
    'msg_stale',
    'msg_question'
  ])
  await expect(check({ ackIds: ['msg_stale', 'msg_stale'] })).resolves.toMatchObject({
    markedIds: ['msg_stale'],
    alreadyReadIds: []
  })
  await expect(check({ ackIds: ['msg_stale'] })).resolves.toMatchObject({
    markedIds: [],
    alreadyReadIds: ['msg_stale']
  })
  expect(db.getUnreadMessages('term_coord').map((message) => message.id)).toEqual(['msg_question'])
  expect(db.getUnreadMessages('term_other').map((message) => message.id)).toEqual(['msg_foreign'])
})

it('keeps a bound Run selection separate from explicit unbound direct mail', async () => {
  const { db, ctx } = harness.setup(false)
  const { id: activeRunId } = db.createRun({
    objective: 'Test Run',
    coordinatorHandle: 'term_coord',
    coordinatorPaneKey: harness.coordinatorPaneKey
  })
  const acknowledge = (ackIds: string[], run?: string) =>
    harness.call('orchestration.ackMessages', { terminal: 'term_coord', ackIds, run }, ctx)
  db.insertMessage({
    id: 'msg_run_selected',
    from: 'term_child',
    to: `run:${activeRunId}`,
    runId: activeRunId,
    subject: 'selected Run status'
  })
  db.insertMessage({
    id: 'msg_run_routed',
    from: 'term_child',
    to: 'term_coord',
    runId: activeRunId,
    subject: 'unselected message routed to the Run at insert'
  })
  db.insertMessage({
    id: 'msg_unbound',
    from: 'term_child',
    to: 'term_coord',
    subject: 'unbound direct mail'
  })

  await expect(acknowledge(['msg_run_selected', 'msg_unbound'])).rejects.toMatchObject({
    code: 'invalid_argument'
  })
  expect(db.getMessageById('msg_run_selected')?.read).toBe(0)
  await expect(acknowledge(['msg_run_selected'])).resolves.toMatchObject({
    markedIds: ['msg_run_selected']
  })
  expect(db.getMessageById('msg_run_routed')).toMatchObject({
    to_handle: `run:${activeRunId}`,
    read: 0
  })
  expect(db.getMessageById('msg_unbound')?.read).toBe(0)
  await expect(acknowledge(['msg_unbound', 'msg_run_routed'], 'run_unbound')).rejects.toMatchObject(
    { code: 'invalid_argument' }
  )
  expect(db.getMessageById('msg_unbound')?.read).toBe(0)
  await expect(acknowledge(['msg_unbound'], 'run_unbound')).resolves.toMatchObject({
    markedIds: ['msg_unbound']
  })
})

it('marks a selected Run-addressed terminal row without routing other mail', async () => {
  const { db, ctx, activeRunId } = harness.setup()
  const terminal = 'term_coord'
  const seed = () =>
    db.insertMessage({
      from: 'term_sender',
      to: `run:${activeRunId}`,
      runId: activeRunId,
      subject: 'Run status'
    })
  const selected = seed()
  const remaining = seed()
  db.db
    .prepare('UPDATE messages SET to_handle = ? WHERE id IN (?, ?)')
    .run(terminal, selected.id, remaining.id)
  const params = { terminal, run: activeRunId, ackIds: [selected.id] }
  await expect(harness.call('orchestration.ackMessages', params, ctx)).resolves.toMatchObject({
    markedIds: [selected.id]
  })
  expect(db.getMessageById(selected.id)).toMatchObject({ read: 1, to_handle: terminal })
  expect(db.getMessageById(remaining.id)).toMatchObject({ read: 0, to_handle: terminal })
})

it('refuses a selected read message still in chat custody without marking the rest', async () => {
  const { db, ctx } = harness.setup(false)
  const terminal = 'term_coord'
  const seed = () => db.insertMessage({ from: 'term_sender', to: terminal, subject: 'status' })
  const held = seed()
  const remaining = seed()
  db.reserveStructuredMailSend({
    mailbox_handle: terminal,
    message_id: held.id,
    session_id: 'session-owned-mail',
    operation_id: 'held-after-read',
    minted_at_ms: 1
  })
  db.db.prepare('UPDATE messages SET read = 1 WHERE id = ?').run(held.id)
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [held.id, remaining.id] }, ctx)
  ).rejects.toMatchObject({
    code: 'invalid_argument',
    message: expect.stringContaining('held by a structured chat')
  })
  expect(db.getMessageById(remaining.id)?.read).toBe(0)
})

it.each([{ run: '' }, { run: ' ' }, { run: null }, { run: 17 }, { run: {} }, { run: [] }])(
  'refuses malformed explicit Run scope $run before marking any mail',
  async (scope) => {
    const { db, runtime, activeRunId } = harness.setup()
    const selected = db.insertMessage({
      from: 'term_sender',
      to: `run:${activeRunId}`,
      runId: activeRunId,
      subject: 'leave unread'
    })
    const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
    const dispatch = (scope: { run?: unknown }) =>
      dispatcher.dispatch({
        id: 'rpc_selective_scope',
        authToken: 'test',
        method: 'orchestration.ackMessages',
        params: { terminal: 'term_coord', ackIds: [selected.id], ...scope },
        orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
        orchestrationRequestId: 'selective_scope_request'
      })
    await expect(dispatch(scope)).resolves.toMatchObject({
      ok: false,
      error: { code: 'invalid_argument' }
    })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    await expect(dispatch({})).resolves.toMatchObject({
      ok: true,
      result: { markedIds: [selected.id], mutation: { replayed: false } }
    })
  }
)

it('uses the same stable pane requirement as an ordinary consuming check', async () => {
  const { db, runtime } = harness.setup(false)
  const terminal = 'term_coord'
  const selected = db.insertMessage({ from: 'term_sender', to: terminal, subject: 'leave unread' })
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(null)
  vi.mocked(runtime.getTerminalProcessIncarnation).mockReturnValue(null)
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const request = {
    id: 'rpc_missing_pane',
    authToken: 'test',
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'missing_pane'
  }
  for (const [method, params] of [
    ['orchestration.check', { terminal }],
    ['orchestration.ackMessages', { terminal, ackIds: [selected.id] }]
  ] as const) {
    await expect(dispatcher.dispatch({ ...request, method, params })).resolves.toMatchObject({
      ok: false,
      error: { code: 'stable_pane_required' }
    })
  }
  expect(db.getMessageById(selected.id)?.read).toBe(0)
})

it.each(['worker_done', 'heartbeat', 'escalation', 'decision_gate'] as const)(
  'refuses a mixed selection containing a %s lifecycle report without taking mail',
  async (type) => {
    const { db, ctx, activeRunId } = harness.setup()
    const to = `run:${activeRunId}`
    const status = db.insertMessage({ from: 'term_sender', to, subject: 'plain status' })
    const report = db.insertMessage({ from: 'term_sender', to, subject: 'lifecycle report', type })
    await expect(
      harness.call(
        'orchestration.ackMessages',
        { terminal: 'term_coord', ackIds: [status.id, report.id] },
        ctx
      )
    ).rejects.toMatchObject({
      code: 'invalid_argument',
      message: expect.stringContaining('Use a normal check')
    })
    expect(db.getMessageById(status.id)).toEqual(status)
    expect(db.getMessageById(report.id)).toEqual(report)
  }
)

it('uses the ordinary Dispatch pane fence without adding a launch check', async () => {
  const { db, runtime, ctx } = harness.setup()
  const terminal = 'term_worker'
  const pane = 'tab_worker:33333333-3333-4333-9333-333333333333'
  const task = db.createTask({ spec: 'worker before binding' })
  const dispatch = createRootDispatch(db, task.id, terminal, pane, undefined, 'pty:prior')
  const selected = db.insertMessage({
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: dispatch.run_id,
    subject: 'pre-bind status'
  })
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(
    'tab_replacement:44444444-4444-4444-9444-444444444444'
  )
  for (const [method, params] of [
    ['orchestration.check', { terminal }],
    ['orchestration.ackMessages', { terminal, ackIds: [selected.id] }]
  ] as const) {
    await expect(harness.call(method, params, ctx)).rejects.toMatchObject({
      code: 'consumer_fenced'
    })
  }
  expect(db.getMessageById(selected.id)?.read).toBe(0)
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(pane)
  const direct = db.insertMessage({ from: 'term_coord', to: terminal, subject: 'unbound status' })
  const unbound = { terminal, run: 'run_unbound' }
  for (const [method, params] of [
    ['orchestration.check', unbound],
    ['orchestration.ackMessages', { ...unbound, ackIds: [direct.id] }]
  ] as const) {
    await expect(harness.call(method, params, ctx)).rejects.toMatchObject({
      code: 'consumer_fenced'
    })
  }
  expect(db.getMessageById(direct.id)?.read).toBe(0)
  const scoped = db.insertMessage({
    from: 'term_coord',
    to: terminal,
    runId: dispatch.run_id,
    subject: 'Dispatch status still addressed to the worker'
  })
  expect(db.getMessageById(scoped.id)?.to_handle).toBe(terminal)
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [selected.id, scoped.id] }, ctx)
  ).resolves.toMatchObject({ dispatchId: dispatch.id, markedIds: [selected.id, scoped.id] })
  expect(db.getMessageById(scoped.id)).toMatchObject({ to_handle: terminal, read: 1 })
  expect(db.getMessageById(direct.id)?.read).toBe(0)
})

it('shares the ordinary legacy fence for the implicit direct mailbox', async () => {
  const { db, ctx } = harness.setup(false)
  const terminal = 'term_coord'
  db.insertMessage({
    from: 'term_sender',
    to: terminal,
    runId: 'run_legacy_local',
    subject: 'old contract on the same address'
  })
  const selected = db.insertMessage({ from: 'term_sender', to: terminal, subject: 'current mail' })
  await expect(harness.call('orchestration.check', { terminal }, ctx)).rejects.toMatchObject({
    code: 'legacy_read_only'
  })
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [selected.id] }, ctx)
  ).rejects.toMatchObject({ code: 'legacy_read_only' })
  expect(db.getMessageById(selected.id)?.read).toBe(0)
})

it('only writes read state and a receipt, including on dispatcher replay', async () => {
  const { db, runtime } = harness.setup(false)
  const fingerprint = db.readLocalMutationCallerFingerprint()
  if (!fingerprint) {
    throw new Error('Expected a local caller fingerprint before any request')
  }
  const selected = db.insertMessage({ from: 'sender', to: 'term_coord', subject: 'unread' })
  db.db
    .prepare(
      `UPDATE messages
       SET pointer_enter_pending = 1, pointer_pty_id = 'pty-original',
           pointer_process_incarnation = 'process-original'
       WHERE id = ?`
    )
    .run(selected.id)
  const original = db.getMessageById(selected.id)
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const refreshSenders = vi
    .spyOn(runtime, 'refreshAgentSenderAssociations')
    .mockResolvedValue(undefined)
  const recordInteraction = vi.spyOn(runtime, 'recordFeatureInteraction')
  await expect(
    dispatcher.dispatch({
      id: 'rpc_ack_caller_probe',
      authToken: 'test',
      method: 'orchestration.callerShow',
      params: { readOnly: true }
    })
  ).resolves.toMatchObject({ ok: true, result: { caller: null, terminal: false } })
  const request = {
    id: 'rpc_ack',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: 'term_coord', ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION
  }
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject({
    ok: false,
    error: { code: 'invalid_argument' }
  })
  const ack = { ...request, orchestrationRequestId: 'selected-request' }
  db.db
    .prepare(
      `INSERT INTO mutation_receipts
       (caller_fingerprint, request_id, method, payload_hash, state, receipt, updated_at)
       VALUES (?, 'expired-request', 'orchestration.send', 'payload', 'completed', '{}', '2000-01-01')`
    )
    .run(fingerprint)
  const expired = db.getMutationReceipt(fingerprint, 'expired-request')
  const retry = { ...ack, params: { ...ack.params, replayOnly: true } }
  await expect(dispatcher.dispatch(retry)).resolves.toMatchObject({
    ok: false,
    error: { code: 'operation_unknown', data: { effectsApplied: false } }
  })
  expect(db.getMutationReceipt(fingerprint, ack.orchestrationRequestId)).toBeUndefined()
  expect(db.getMessageById(selected.id)).toEqual(original)
  await expect(
    dispatcher.dispatch({
      ...ack,
      params: { ...ack.params, ackIds: [selected.id, 'msg_missing'] }
    })
  ).resolves.toMatchObject({ ok: false, error: { code: 'invalid_argument' } })
  expect(db.getMutationReceipt(fingerprint, 'expired-request')).toEqual(expired)
  expect(db.getMutationReceipt(fingerprint, ack.orchestrationRequestId)).toBeUndefined()
  expect(db.getMessageById(selected.id)).toEqual(original)

  const results = await Promise.all([dispatcher.dispatch(ack), dispatcher.dispatch(ack)])
  for (const replayed of [true, false]) {
    expect(results).toContainEqual(
      expect.objectContaining({
        ok: true,
        result: expect.objectContaining({
          markedIds: [selected.id],
          mutation: expect.objectContaining({ replayed })
        })
      })
    )
  }
  // Even a valid replay discloses no stored mail contents.
  for (const reply of results) {
    expect(reply).not.toHaveProperty('result.messages')
  }
  await expect(dispatcher.dispatch(retry)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: true } }
  })
  expect(db.getMessageById(selected.id)).toEqual({ ...original, read: 1 })
  expect(db.getMutationReceipt(fingerprint, ack.orchestrationRequestId)?.state).toBe('completed')
  expect(db.getMutationReceipt(fingerprint, 'expired-request')).toEqual(expired)
  expect(db.readLocalMutationCallerFingerprint()).toBe(fingerprint)
  expect(refreshSenders).not.toHaveBeenCalled()
  expect(recordInteraction).not.toHaveBeenCalled()
  await expect(
    dispatcher.dispatch({
      ...request,
      method: 'orchestration.check',
      params: { terminal: 'term_coord', peek: true }
    })
  ).resolves.toMatchObject({ ok: true })
  expect(refreshSenders).toHaveBeenCalledOnce()
  expect(recordInteraction).toHaveBeenCalledWith('agent-orchestration')
  const status = await dispatcher.dispatch({
    id: 'rpc_selective_status',
    authToken: 'test',
    method: 'orchestration.requestShow',
    params: { request: ack.orchestrationRequestId },
    orchestrationCompatibilityEvidence: { terminalHandle: 'term_other' }
  })
  expect(status).toMatchObject({
    ok: true,
    result: { state: 'completed', method: 'orchestration.ackMessages' }
  })
  expect(status).not.toHaveProperty('result.receipt')
})

it('refuses a new selection at capacity without pruning and replays the original receipt', async () => {
  const { db, runtime } = harness.setup(false)
  const selected = db.insertMessage({ from: 'sender', to: 'term_coord', subject: 'mark once' })
  const unread = db.insertMessage({ from: 'sender', to: 'term_coord', subject: 'leave unread' })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const request = {
    id: 'rpc_full_ledger',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: 'term_coord', ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'original-selection'
  }
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: false } }
  })
  db.db
    .prepare(
      `WITH RECURSIVE receipt_numbers(value) AS (
         VALUES (1)
         UNION ALL
         SELECT value + 1 FROM receipt_numbers WHERE value < ?
       )
       INSERT INTO mutation_receipts
         (caller_fingerprint, request_id, method, payload_hash, state, receipt, updated_at)
       SELECT 'other-caller', printf('request-%d', value), 'orchestration.send',
              'hash', 'completed', '{}', '2000-01-01'
       FROM receipt_numbers`
    )
    .run(MUTATION_RECEIPT_MAX_ROWS - 1)

  await expect(
    dispatcher.dispatch({
      ...request,
      params: { ...request.params, ackIds: [unread.id] },
      orchestrationRequestId: 'refused-selection'
    })
  ).resolves.toMatchObject({ ok: false, error: { code: 'mutation_ledger_full' } })
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: true } }
  })
  expect(db.getMessageById(unread.id)?.read).toBe(0)
  expect(
    db.db
      .prepare(
        "SELECT COUNT(*) AS count FROM mutation_receipts WHERE caller_fingerprint = 'other-caller'"
      )
      .get()
  ).toEqual({ count: MUTATION_RECEIPT_MAX_ROWS - 1 })
})

it.each([undefined, 'paired-device'])(
  'admits the same execution mailbox as ordinary check (paired device: %s)',
  async (pairedDeviceId) => {
    const { db, runtime } = harness.setup(false)
    const terminal = 'term_coord'
    const makeMessage = (subject: string) =>
      db.insertMessage({ from: 'sender', to: terminal, subject })
    const selected = makeMessage('selected')
    const remaining = makeMessage('remaining')
    const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
    const transport = pairedDeviceId
      ? {
          pairedDeviceId,
          authenticatedCallerFingerprint: 'test-fingerprint',
          clientKind: 'runtime' as const
        }
      : undefined
    const request = {
      id: 'rpc_selective_parity',
      authToken: 'test',
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION
    }
    await expect(
      dispatcher.dispatch(
        {
          ...request,
          method: 'orchestration.ackMessages',
          params: { terminal, ackIds: [selected.id] },
          orchestrationRequestId: 'selective-parity'
        },
        transport
      )
    ).resolves.toMatchObject({ ok: true, result: { markedIds: [selected.id] } })
    expect(db.getMessageById(remaining.id)?.read).toBe(0)
    await expect(
      dispatcher.dispatch(
        {
          ...request,
          method: 'orchestration.check',
          params: { terminal },
          orchestrationRequestId: 'ordinary-parity'
        },
        transport
      )
    ).resolves.toMatchObject({
      ok: true,
      result: { messages: [{ id: remaining.id }] }
    })
  }
)

it('replays only on the original process and implicit Run', async () => {
  const { db, runtime, activeRunId } = harness.setup()
  const selected = db.insertMessage({
    from: 'term_sender',
    to: `run:${activeRunId}`,
    subject: 'selected status'
  })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  let incarnation = 'pty:original'
  vi.mocked(runtime.getTerminalProcessIncarnation).mockImplementation(() => incarnation)
  const leaf = '44444444-4444-4444-8444-444444444444'
  const acknowledge = (terminalPaneKey = `tab_test:${leaf}`) =>
    dispatcher.dispatch({
      id: 'rpc_selected_ack',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: 'term_coord', terminalPaneKey, ackIds: [selected.id] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'selected_ack_retry'
    })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: false } }
  })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: true } }
  })
  await expect(acknowledge(`tab_split:${leaf}`)).resolves.toMatchObject({
    ok: true,
    result: { mutation: { replayed: true } }
  })
  await expect(acknowledge(`peer:${leaf}`)).resolves.toMatchObject({
    ok: false,
    error: { code: 'forbidden', data: { effectsApplied: false } }
  })
  vi.mocked(runtime.getLiveTerminalPaneKey).mockReturnValueOnce(null)
  await expect(acknowledge()).resolves.toMatchObject({
    ok: false,
    error: { code: 'request_mismatch' }
  })
  incarnation = 'pty:replacement'
  await expect(acknowledge()).resolves.toMatchObject({
    ok: false,
    error: { code: 'request_mismatch' }
  })
  incarnation = 'pty:original'
  db.createRun({
    objective: 'replacement Run',
    coordinatorHandle: 'term_coord',
    coordinatorPaneKey: harness.coordinatorPaneKey
  })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: false,
    error: { code: 'request_mismatch' }
  })
})

it.each(['run:example', 'dispatch:example'])(
  'rejects %s as a selective terminal',
  async (terminal) => {
    const { ctx } = harness.setup(false)
    await expect(
      harness.call('orchestration.ackMessages', { terminal, ackIds: ['msg_example'] }, ctx)
    ).rejects.toMatchObject({
      code: 'invalid_argument',
      message: expect.stringContaining('not a role mailbox address')
    })
  }
)

it('allows a fresh acknowledgment with ordinary pane fallback but cannot replay without a process', async () => {
  const { db, runtime } = harness.setup(false)
  const terminal = 'term_coord'
  vi.mocked(runtime.getTerminalProcessIncarnation).mockReturnValue(null)
  const selected = db.insertMessage({ from: 'term_sender', to: terminal, subject: 'recovered' })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const request = {
    id: 'rpc_ack_without_process',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal, ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'ack_without_process'
  }
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id] }
  })
  await expect(
    dispatcher.dispatch({ ...request, params: { ...request.params, replayOnly: true } })
  ).resolves.toMatchObject({
    ok: false,
    error: { code: 'operation_unknown', data: { effectsApplied: false } }
  })
})

it('enforces ordinary residual precedence while selecting only eligible messages', async () => {
  const { db, runtime, ctx } = harness.setup()
  const pane = 'tab_lead:22222222-2222-4222-9222-222222222222'
  vi.mocked(runtime.getTerminalPaneKey).mockImplementation((handle) =>
    handle === 'term_lead' ? pane : harness.coordinatorPaneKey
  )
  const task = db.createTask({ spec: 'nested lead' })
  const dispatch = createRootDispatch(
    db,
    task.id,
    'term_lead',
    pane,
    undefined,
    'runtime_test:term_lead:1'
  )
  const run = db.createRun({
    objective: 'child Run',
    coordinatorHandle: 'term_lead',
    coordinatorPaneKey: pane
  })
  db.insertMessage({
    id: 'msg_prebind',
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: dispatch.run_id,
    subject: 'pre-bind status'
  })
  expect(
    await harness.call('orchestration.check', { terminal: 'term_lead', peek: true }, ctx)
  ).toMatchObject({ messages: [{ id: 'msg_prebind' }] })
  db.db.prepare('UPDATE messages SET to_handle = ? WHERE id = ?').run('term_lead', 'msg_prebind')
  db.insertMessage({
    id: 'msg_child_status',
    from: 'term_child',
    to: `run:${run.id}`,
    runId: run.id,
    subject: 'child status'
  })
  db.insertMessage({
    id: 'msg_child_question',
    from: 'term_child',
    to: `run:${run.id}`,
    runId: run.id,
    subject: 'new question',
    type: 'question'
  })
  const delivery = db.getOrCreateRunDelivery({
    runId: run.id,
    consumerGeneration: run.consumer_generation
  })!
  expect(delivery.messages.map((message) => message.id)).toEqual([
    'msg_child_status',
    'msg_child_question'
  ])
  const receiptIdentity = {
    callerFingerprint: 'caller_run',
    requestId: 'selected_run_ack',
    method: 'orchestration.ackMessages',
    payloadHash: 'selected_run_payload'
  }
  db.beginMutationReceipt(receiptIdentity)
  ctx.recordMutationReceipt = (receipt) => {
    db.completeMutationReceipt({ ...receiptIdentity, receipt: JSON.stringify(receipt) })
  }
  const mixed = { terminal: 'term_lead', ackIds: ['msg_prebind', 'msg_child_status'] }
  await expect(harness.call('orchestration.ackMessages', mixed, ctx)).rejects.toMatchObject({
    code: 'invalid_argument'
  })
  expect(db.getMessageById('msg_prebind')?.read).toBe(0)
  expect(db.getMessageById('msg_child_status')?.read).toBe(0)
  const selection = { terminal: 'term_lead', ackIds: ['msg_child_status'] }
  await expect(harness.call('orchestration.ackMessages', selection, ctx)).resolves.toMatchObject({
    markedIds: ['msg_child_status']
  })
  expect(
    JSON.parse(db.getMutationReceipt('caller_run', 'selected_run_ack')!.receipt!)
  ).toMatchObject({
    runId: run.id,
    markedIds: ['msg_child_status']
  })
  await expect(harness.call('orchestration.ackMessages', selection, ctx)).resolves.toMatchObject({
    alreadyReadIds: ['msg_child_status']
  })
  expect(db.getMessageById('msg_child_question')?.read).toBe(0)
  expect(
    db.getOrCreateRunDelivery({ runId: run.id, consumerGeneration: run.consumer_generation })
  ).toMatchObject({
    delivery: { id: delivery.delivery.id },
    messages: [{ id: 'msg_child_status' }, { id: 'msg_child_question' }]
  })
  ctx.recordMutationReceipt = undefined
  await harness.call(
    'orchestration.check',
    { terminal: 'term_lead', ack: delivery.delivery.id },
    ctx
  )
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const residualAck = (replayOnly = false) =>
    dispatcher.dispatch({
      id: 'rpc_residual_ack',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: 'term_lead', ackIds: ['msg_prebind'], replayOnly },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'residual_ack'
    })
  await expect(residualAck()).resolves.toMatchObject({
    ok: true,
    result: { markedIds: ['msg_prebind'] }
  })
  const later = db.insertMessage({
    from: 'term_child',
    to: `run:${run.id}`,
    runId: run.id,
    subject: 'later child status'
  })
  expect(
    db.getOrCreateRunDelivery({ runId: run.id, consumerGeneration: run.consumer_generation })
      ?.messages
  ).toMatchObject([{ id: later.id }])
  await expect(residualAck(true)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: ['msg_prebind'], mutation: { replayed: true } }
  })
  expect(db.getMessageById(later.id)?.read).toBe(0)
})

it('rejects the wrong owning Run and keeps a receipt on the original Dispatch generation', async () => {
  const { db, runtime, ctx, activeRunId } = harness.setup()
  const pane = 'tab_worker:33333333-3333-4333-9333-333333333333'
  vi.mocked(runtime.getTerminalPaneKey).mockImplementation((handle) =>
    handle === 'term_worker' ? pane : harness.coordinatorPaneKey
  )
  const task = db.createTask({ spec: 'worker receipt' })
  const dispatch = createRootDispatch(
    db,
    task.id,
    'term_worker',
    pane,
    undefined,
    'runtime_test:term_worker:1'
  )
  db.insertMessage({
    id: 'msg_worker_selected',
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: dispatch.run_id,
    subject: 'selected status'
  })
  const { id: wrongRunId } = db.createRun({
    objective: 'unrelated Run',
    coordinatorHandle: null,
    coordinatorPaneKey: null
  })
  const wrongRun = db.insertMessage({
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: wrongRunId,
    subject: 'wrong Run, same address'
  })
  await expect(
    harness.call(
      'orchestration.ackMessages',
      { terminal: 'term_worker', ackIds: ['msg_worker_selected', wrongRun.id] },
      ctx
    )
  ).rejects.toMatchObject({ code: 'invalid_argument' })
  expect(db.getMessageById('msg_worker_selected')?.read).toBe(0)
  expect(db.getMessageById(wrongRun.id)?.read).toBe(0)
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const acknowledge = () =>
    dispatcher.dispatch({
      id: 'rpc_worker_ack',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: 'term_worker', ackIds: ['msg_worker_selected'] },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'selected_worker_ack'
    })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: true,
    result: {
      runId: activeRunId,
      dispatchId: dispatch.id,
      markedIds: ['msg_worker_selected']
    }
  })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: true,
    result: { mutation: { replayed: true } }
  })
  reattachDispatchConsumer(db, {
    dispatchId: dispatch.id,
    paneKey: pane,
    processIncarnation: 'runtime_test:term_worker:1'
  })
  await expect(acknowledge()).resolves.toMatchObject({
    ok: false,
    error: { code: 'request_mismatch' }
  })
  expect(db.getMessageById('msg_worker_selected')?.read).toBe(1)
})
