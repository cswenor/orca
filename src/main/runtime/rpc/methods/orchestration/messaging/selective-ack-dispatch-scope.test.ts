import { afterEach, expect, it, vi } from 'vitest'
import { createRootDispatch } from '../../../../orchestration/db/root-dispatch-test-fixture'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'
import { createOrcaSessionId } from '../../../../../../shared/agent-session/identity/orca-session-address'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it.each([
  'caller pane',
  'recorded pane',
  'recorded handle',
  'recorded session',
  'caller session'
] as const)('refuses conflicting Dispatch owners on %s', async (conflict) => {
  const { db, runtime, ctx } = harness.setup()
  const leaf = '33333333-3333-4333-9333-333333333333'
  const terminal = conflict === 'recorded pane' ? 'term_second' : 'term_reminted'
  const currentPane = `tab_current:${leaf}`
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(
    conflict === 'recorded pane' ? null : currentPane
  )
  const seed = (suffix: string, paneKey: string) => {
    const task = db.createTask({ spec: `task ${suffix}` })
    const dispatch = createRootDispatch(db, task.id, `term_${suffix}`, paneKey)
    return db.insertMessage({
      from: 'term_coord',
      to: `dispatch:${dispatch.id}`,
      runId: dispatch.run_id,
      subject: `mail ${suffix}`
    })
  }
  const other = seed(
    'first',
    conflict === 'recorded handle' || conflict.endsWith('session')
      ? 'tab_first:55555555-5555-4555-8555-555555555555'
      : `tab_first:${leaf}`
  )
  const newest = seed('second', 'tab_second:44444444-4444-4444-8444-444444444444')
  // Model conflicting restored records, which current Dispatch creation already prevents.
  db.db
    .prepare('UPDATE dispatch_contexts SET assignee_pane_key = ? WHERE assignee_handle = ?')
    .run(currentPane, 'term_second')
  if (conflict === 'recorded handle') {
    db.db
      .prepare('UPDATE dispatch_contexts SET assignee_handle = ? WHERE assignee_handle = ?')
      .run('term_second', 'term_first')
  }
  if (conflict === 'recorded session' || conflict === 'caller session') {
    const sessionId = createOrcaSessionId(() => '88888888-8888-4888-8888-888888888888')
    db.db
      .prepare(
        'UPDATE dispatch_contexts SET assignee_orca_session_id = ? WHERE assignee_handle = ? OR ?'
      )
      .run(sessionId, 'term_first', conflict === 'recorded session' ? 1 : 0)
    if (conflict === 'caller session') {
      ctx.orchestrationCaller = {
        address: terminal,
        terminalHandle: terminal,
        paneKey: currentPane,
        orcaSessionId: sessionId,
        sessionId,
        workspaceId: 'workspace-test'
      }
    }
  }
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [newest.id] }, ctx)
  ).rejects.toMatchObject({
    code: 'consumer_fenced',
    message: expect.stringContaining('unambiguous Dispatch')
  })
  expect(db.getMessageById(other.id)?.read).toBe(0)
  expect(db.getMessageById(newest.id)?.read).toBe(0)
})

it('refuses selective ack for a local worker whose Dispatch belongs to a peer Run', async () => {
  const { db, runtime, ctx } = harness.setup(false)
  const terminal = 'term_worker'
  const pane = 'tab_worker:33333333-3333-4333-9333-333333333333'
  vi.mocked(runtime.getTerminalPaneKey).mockReturnValue(pane)
  const peer = db.admitPeerRun({
    peerFingerprint: 'test-peer',
    peerRun: { conversationId: 'conv_test', runId: 'run_home', label: 'test home' }
  })
  const task = db.createTask({ runId: peer.run_id, spec: 'peer worker' })
  const dispatch = createRootDispatch(db, task.id, terminal, pane)
  const selected = db.insertMessage({
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: peer.run_id,
    subject: 'peer worker status'
  })
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [selected.id] }, ctx)
  ).rejects.toMatchObject({ code: 'legacy_read_only', data: { effectsApplied: false } })
  expect(db.getMessageById(selected.id)?.read).toBe(0)
})

it('rejects a peer Run row even in a direct mailbox', async () => {
  const { db, ctx } = harness.setup(false)
  const terminal = 'term_coord'
  const peer = db.admitPeerRun({
    peerFingerprint: 'test-peer',
    peerRun: { conversationId: 'conv_direct', runId: 'run_home', label: 'test home' }
  })
  const foreign = db.insertMessage({
    from: 'term_worker',
    to: terminal,
    runId: peer.run_id,
    subject: 'peer Run status'
  })
  const local = db.insertMessage({ from: 'term_sender', to: terminal, subject: 'local status' })
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [local.id, foreign.id] }, ctx)
  ).rejects.toMatchObject({ code: 'legacy_read_only', data: { effectsApplied: false } })
  expect(db.getMessageById(local.id)?.read).toBe(0)
  expect(db.getMessageById(foreign.id)?.read).toBe(0)
})

it('selects peer mail consumable in a local Run without taking unselected local mail', async () => {
  const { db, ctx, activeRunId } = harness.setup()
  const terminal = 'term_coord'
  const to = `run:${activeRunId}`
  const local = db.insertMessage({
    from: 'term_sender',
    to,
    runId: activeRunId,
    subject: 'local status'
  })
  const imported = db.insertPeerMailMessage({
    conversationId: 'conv_local_mail',
    peerFingerprint: 'test-peer',
    remoteParty: null,
    message: {
      id: 'msg_imported_status',
      from: 'peer-worker',
      to,
      runId: activeRunId,
      subject: 'imported status'
    }
  }).message!
  await expect(
    harness.call('orchestration.ackMessages', { terminal, ackIds: [imported.id] }, ctx)
  ).resolves.toMatchObject({ markedIds: [imported.id] })
  expect(db.getMessageById(local.id)?.read).toBe(0)
  expect(db.getMessageById(imported.id)?.read).toBe(1)
})
