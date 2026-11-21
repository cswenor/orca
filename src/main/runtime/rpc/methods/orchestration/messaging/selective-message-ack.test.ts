import { afterEach, expect, it, vi } from 'vitest'
import {
  createRootDispatch,
  reattachDispatchConsumer
} from '../../../../orchestration/db/root-dispatch-test-fixture'
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

it('refuses a stale terminal even when a saved pane and request ID are supplied', async () => {
  const { db, runtime, activeRunId } = harness.setup()
  const selected = db.insertMessage({
    from: 'term_sender',
    to: `run:${activeRunId}`,
    subject: 'leave unread'
  })
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(null)
  vi.mocked(runtime.getTerminalProcessIncarnation).mockReturnValue(null)
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  await expect(
    dispatcher.dispatch({
      id: 'rpc_stale_selective_ack',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: {
        terminal: 'term_coord',
        terminalPaneKey: harness.coordinatorPaneKey,
        ackIds: [selected.id]
      },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'stale_terminal_request'
    })
  ).resolves.toMatchObject({ ok: false, error: { code: 'consumer_fenced' } })
  expect(db.getMessageById(selected.id)?.read).toBe(0)
})

it.each([null, 'different-pane'])(
  'refuses a hosted caller whose mailbox routing pane is %s',
  async (routingPane) => {
    const { db, runtime, activeRunId } = harness.setup()
    const selected = db.insertMessage({
      from: 'term_sender',
      to: `run:${activeRunId}`,
      subject: 'leave unread'
    })
    const hostedAuthority = runtime.getOrchestrationDispatchAuthority('term_coord')
    vi.mocked(runtime.getOrchestrationDispatchAuthority).mockReturnValue(hostedAuthority)
    vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(routingPane)
    const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
    await expect(
      dispatcher.dispatch({
        id: 'rpc_unverified_routing_pane',
        authToken: 'test',
        method: 'orchestration.ackMessages',
        params: {
          terminal: 'term_coord',
          terminalPaneKey: harness.coordinatorPaneKey,
          ackIds: [selected.id]
        },
        orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
        orchestrationRequestId: 'unverified_routing_pane_request'
      })
    ).resolves.toMatchObject({
      ok: false,
      error: { code: 'consumer_fenced', data: { effectsApplied: false } }
    })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
  }
)

it.each(['worker_done', 'heartbeat'] as const)(
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

it.each(['pty:retained', 'structured:released'])(
  'does not treat %s as live mailbox authority',
  async (process) => {
    const { db, runtime, ctx } = harness.setup(false)
    const selected = db.insertMessage({ from: 'sender', to: 'term_coord', subject: 'unread' })
    vi.mocked(runtime.getLiveTerminalPaneKey).mockReturnValue(
      process.startsWith('structured:') ? harness.coordinatorPaneKey : null
    )
    vi.mocked(runtime.getTerminalProcessIncarnation).mockReturnValue(process)
    await expect(
      harness.call(
        'orchestration.ackMessages',
        { terminal: 'term_coord', ackIds: [selected.id] },
        ctx
      )
    ).rejects.toMatchObject({ code: 'consumer_fenced', data: { effectsApplied: false } })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
  }
)

it.each([undefined, 'pty:prior'])(
  'requires the current Dispatch process (recorded: %s)',
  async (process) => {
    const { db, runtime, ctx } = harness.setup()
    const pane = 'tab_worker:33333333-3333-4333-9333-333333333333'
    vi.mocked(runtime.getTerminalPaneKey).mockImplementation((handle) =>
      handle === 'term_worker' ? pane : harness.coordinatorPaneKey
    )
    const task = db.createTask({ spec: 'worker before binding' })
    const dispatch = createRootDispatch(db, task.id, 'term_worker', pane, undefined, process)
    const selected = db.insertMessage({
      from: 'term_coord',
      to: `dispatch:${dispatch.id}`,
      runId: dispatch.run_id,
      subject: 'pre-bind question'
    })
    const check = () =>
      harness.call(
        'orchestration.ackMessages',
        { terminal: 'term_worker', ackIds: [selected.id] },
        ctx
      )
    await expect(check()).rejects.toMatchObject({ code: 'consumer_fenced' })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    reattachDispatchConsumer(db, {
      dispatchId: dispatch.id,
      paneKey: pane,
      processIncarnation: 'runtime_test:term_worker:1'
    })
    await expect(check()).resolves.toMatchObject({ markedIds: [selected.id] })
  }
)

it('only writes read state and a receipt, including on dispatcher replay', async () => {
  const { db, runtime } = harness.setup(false)
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
  const ack = {
    ...request,
    orchestrationRequestId: 'selected-request',
    orchestrationCompatibilityEvidence: {
      terminalHandle: 'term_coord',
      paneKey: harness.coordinatorPaneKey
    }
  }
  await expect(
    dispatcher.dispatch({
      ...ack,
      orchestrationCompatibilityEvidence: { terminalHandle: 'term_other' }
    })
  ).resolves.toMatchObject({ ok: false, error: { code: 'consumer_fenced' } })
  expect(db.readLocalMutationCallerFingerprint()).toBeUndefined()
  const fingerprint = db.getOrCreateLocalMutationCallerFingerprint()
  db.db
    .prepare(
      `INSERT INTO mutation_receipts
       (caller_fingerprint, request_id, method, payload_hash, state, receipt, updated_at)
       VALUES (?, 'expired-request', 'orchestration.send', 'payload', 'completed', '{}', '2000-01-01')`
    )
    .run(fingerprint)
  const expired = db.getMutationReceipt(fingerprint, 'expired-request')
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
  expect(db.getMessageById(selected.id)).toEqual({ ...original, read: 1 })
  expect(db.getMutationReceipt(fingerprint, ack.orchestrationRequestId)?.state).toBe('completed')
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

it('requires current launch proof for local selective calls to a token-launched pane', async () => {
  const { db, runtime } = harness.setup(false)
  const handle = 'term_coord'
  const launchToken = 'current-launch'
  vi.mocked(runtime.getSelectiveAckPtyLaunchToken).mockReturnValue(launchToken)
  const selected = db.insertMessage({ from: 'sender', to: handle, subject: 'selective' })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const request = {
    id: 'rpc_selective_agent_pane',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: handle, ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'agent-pane-selective'
  }
  const evidence = { terminalHandle: handle, paneKey: harness.coordinatorPaneKey }
  const fenced = {
    ok: false,
    error: {
      code: 'consumer_fenced',
      message: expect.stringContaining('Run it from that pane or use an ordinary check'),
      data: { effectsApplied: false }
    }
  }
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject(fenced)
  await expect(
    dispatcher.dispatch({
      ...request,
      orchestrationCompatibilityEvidence: { ...evidence, launchToken: 'previous-launch' }
    })
  ).resolves.toMatchObject(fenced)
  expect(db.getMessageById(selected.id)?.read).toBe(0)
  expect(db.readLocalMutationCallerFingerprint()).toBeUndefined()

  await expect(
    dispatcher.dispatch({
      ...request,
      orchestrationCompatibilityEvidence: { ...evidence, launchToken }
    })
  ).resolves.toMatchObject({ ok: true, result: { markedIds: [selected.id] } })
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject(fenced)
  expect(db.getMessageById(selected.id)?.read).toBe(1)

  const remote = db.insertMessage({ from: 'sender', to: handle, subject: 'paired selective' })
  vi.mocked(runtime.getSelectiveAckPtyLaunchToken).mockReturnValue(undefined)
  await expect(
    dispatcher.dispatch({
      ...request,
      orchestrationRequestId: 'unverified-restored-pane',
      params: { ...request.params, ackIds: [remote.id] }
    })
  ).resolves.toMatchObject({
    ok: false,
    error: {
      code: 'consumer_fenced',
      message: expect.stringContaining('cannot verify this restored pane'),
      data: { effectsApplied: false }
    }
  })
  expect(db.getMessageById(remote.id)?.read).toBe(0)
  const pairedRequest = { ...request, params: { ...request.params, ackIds: [remote.id] } }
  const paired = {
    pairedDeviceId: 'test-device',
    authenticatedCallerFingerprint: 'test-fingerprint',
    clientKind: 'runtime' as const
  }
  await expect(dispatcher.dispatch(pairedRequest, paired)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [remote.id] }
  })
  const authority = runtime.getOrchestrationDispatchAuthority(handle)!
  vi.mocked(runtime.getOrchestrationDispatchAuthority).mockReturnValue({
    ...authority,
    launchTokenHash: 'different-target-launch'
  })
  await expect(dispatcher.dispatch(pairedRequest, paired)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [remote.id], mutation: { replayed: true } }
  })
})

it('accepts matching host evidence for a confirmed tokenless pane and rejects stale evidence', async () => {
  const { db, runtime } = harness.setup(false)
  const handle = 'term_coord'
  const host = { kind: 'wsl', hostId: 'local', distro: 'Ubuntu' } as const
  const authority = runtime.getOrchestrationDispatchAuthority(handle)!
  vi.mocked(runtime.getOrchestrationDispatchAuthority).mockReturnValue({
    ...authority,
    hostScope: host
  })
  const selected = db.insertMessage({ from: 'sender', to: handle, subject: 'selective' })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const request = {
    id: 'rpc_selective_hosted_shell',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: handle, ackIds: [selected.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'hosted-shell-selective'
  }
  const evidence = { terminalHandle: handle, paneKey: harness.coordinatorPaneKey, host }
  for (const stale of [
    { ...evidence, host: { ...host, distro: 'old-distro' } },
    { ...evidence, launchToken: 'retired-launch' }
  ]) {
    await expect(
      dispatcher.dispatch({ ...request, orchestrationCompatibilityEvidence: stale })
    ).resolves.toMatchObject({ ok: false, error: { code: 'consumer_fenced' } })
  }
  expect(db.getMessageById(selected.id)?.read).toBe(0)
  await expect(
    dispatcher.dispatch({ ...request, orchestrationCompatibilityEvidence: evidence })
  ).resolves.toMatchObject({ ok: true, result: { markedIds: [selected.id] } })
})

it('rejects a relay-only SSH attachment as a selective mailbox caller', async () => {
  const { db, runtime } = harness.setup(false)
  vi.mocked(runtime.getOrchestrationDispatchAuthority).mockRestore()
  vi.mocked(runtime.getTerminalPaneKey).mockRestore()
  vi.mocked(runtime.getLiveTerminalPaneKey).mockRestore()
  vi.mocked(runtime.getTerminalProcessIncarnation).mockRestore()
  const handle = 'term_bridge'
  const attachment = runtime.registerOrchestrationCompatibilitySshAttachment('test-ssh', 'live', {
    handle,
    paneKey: harness.coordinatorPaneKey,
    processIncarnation: 'relay:worker'
  })
  try {
    const selected = db.insertMessage({ from: 'sender', to: handle, subject: 'leave unread' })
    const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
    expect(runtime.getLiveTerminalPaneKey(handle)).toBe(harness.coordinatorPaneKey)
    expect(runtime.getTerminalProcessIncarnation(handle)).toBe('relay:worker')
    await expect(
      dispatcher.dispatch(
        {
          id: 'rpc_selective_ssh',
          authToken: 'test',
          method: 'orchestration.ackMessages',
          params: { terminal: handle, ackIds: [selected.id] },
          orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
          orchestrationRequestId: 'relay_attachment_selective'
        },
        {
          pairedDeviceId: 'test-device',
          authenticatedCallerFingerprint: 'test-fingerprint',
          clientKind: 'runtime'
        }
      )
    ).resolves.toMatchObject({
      ok: false,
      error: { code: 'consumer_fenced', data: { effectsApplied: false } }
    })
    expect(db.getMessageById(selected.id)?.read).toBe(0)
    expect(db.getMutationReceipt('test-fingerprint', 'relay_attachment_selective')).toBeUndefined()
  } finally {
    runtime.releaseOrchestrationCompatibilitySshAttachment(attachment.attachmentId)
  }
})

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
  const acknowledge = () =>
    dispatcher.dispatch({
      id: 'rpc_selected_ack',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal: 'term_coord', ackIds: [selected.id] },
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

it('selectively acknowledges pre-bind Dispatch mail without disturbing an outstanding Run delivery', async () => {
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
  const selection = { terminal: 'term_lead', ackIds: ['msg_prebind', 'msg_child_status'] }
  await expect(harness.call('orchestration.ackMessages', selection, ctx)).resolves.toMatchObject({
    markedIds: ['msg_prebind', 'msg_child_status']
  })
  expect(
    JSON.parse(db.getMutationReceipt('caller_run', 'selected_run_ack')!.receipt!)
  ).toMatchObject({
    runId: run.id,
    markedIds: ['msg_prebind', 'msg_child_status']
  })
  await expect(harness.call('orchestration.ackMessages', selection, ctx)).resolves.toMatchObject({
    alreadyReadIds: ['msg_prebind', 'msg_child_status']
  })
  expect(db.getMessageById('msg_prebind')?.read).toBe(1)
  expect(db.getMessageById('msg_child_question')?.read).toBe(0)
  expect(
    db.getOrCreateRunDelivery({ runId: run.id, consumerGeneration: run.consumer_generation })
  ).toMatchObject({
    delivery: { id: delivery.delivery.id },
    messages: [{ id: 'msg_child_status' }, { id: 'msg_child_question' }]
  })
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
