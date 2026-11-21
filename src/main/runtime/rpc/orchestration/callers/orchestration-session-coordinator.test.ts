import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { testOrcaSessionId } from '../../../../../shared/agent-session/identity/orca-session-address-test-fixture'
import {
  mintStructuredWorkerHandle,
  mintStructuredWorkerPaneKey,
  structuredWorkerIdentities,
  structuredWorkerProcessIncarnation
} from '../../../orchestration/terminal-authority/structured-sessions/structured-worker-identity'
import {
  ADDRESS_Y,
  createSessionCallerHarness,
  orchestrationRequest,
  idOf,
  resultOf,
  SESSION_X,
  sessionRecord,
  SESSION_Y,
  type SessionCallerHarness
} from './session.test-fixture'

const hostRef = vi.hoisted((): { current: unknown } => ({ current: null }))
vi.mock(
  '../../../../native-chat/agent-session-wire/host/structured-agent-session-registry',
  () => ({
    getStructuredAgentSessionHost: () => hostRef.current
  })
)

function acknowledgment(sessionId: string, id: string, requestId: string) {
  return orchestrationRequest(
    'orchestration.ackMessages',
    { ackIds: [id] },
    { sessionId, requestId }
  )
}

describe('a structured worker that names itself by session id', () => {
  let h: SessionCallerHarness
  const workerSession = SESSION_Y
  const handle = mintStructuredWorkerHandle()
  const paneKey = mintStructuredWorkerPaneKey(workerSession)

  beforeEach(() => {
    h = createSessionCallerHarness(hostRef)
    structuredWorkerIdentities.register({
      handle,
      sessionId: workerSession,
      agent: 'claude',
      paneKey,
      processIncarnation: structuredWorkerProcessIncarnation(workerSession),
      worktreeId: 'wt_1',
      hostScope: { kind: 'local', hostId: 'local' }
    })
  })

  afterEach(() => {
    h.close()
    vi.restoreAllMocks()
  })

  it('sends mail at either of its spellings to its Dispatch once assigned in a Run it coordinated before', async () => {
    const { run } = resultOf(
      await h.dispatch(
        orchestrationRequest(
          'orchestration.runCreate',
          { objective: 'o' },
          { sessionId: workerSession }
        )
      )
    )
    const runId = idOf(run)
    // A chat takes the Run over; the worker's addresses stay remembered as a former coordinator's.
    resultOf(
      await h.dispatch(
        orchestrationRequest('orchestration.runUse', { id: runId }, { sessionId: SESSION_X })
      )
    )
    expect(h.db.getRunMailboxOwnerIdsForHandle(ADDRESS_Y)).toEqual([runId])
    const dispatch = h.db.createDispatchContext({
      taskId: h.db.createTask({ runId, spec: 'work' }).id,
      assigneeHandle: handle,
      assigneePaneKey: paneKey,
      processIncarnation: structuredWorkerProcessIncarnation(workerSession),
      creator: { kind: 'session', orcaSessionId: SESSION_X },
      maxDepth: Number.MAX_SAFE_INTEGER
    })

    // Both spellings name one party, so both land in the one mailbox it reads, not the Run's.
    for (const to of [ADDRESS_Y, handle]) {
      const { message } = resultOf(
        await h.dispatch(
          orchestrationRequest('orchestration.send', { to, subject: to }, { sessionId: SESSION_X })
        )
      )
      expect(message).toMatchObject({ to_handle: `dispatch:${dispatch.id}` })
    }
    const read = resultOf(
      await h.dispatch(
        orchestrationRequest('orchestration.check', { peek: true }, { sessionId: workerSession })
      )
    )
    expect(read).toMatchObject({
      count: 2,
      messages: expect.arrayContaining([
        expect.objectContaining({ subject: handle }),
        expect.objectContaining({ subject: ADDRESS_Y })
      ])
    })
  })

  it('lets a cleared worker acknowledge its original Dispatch but fences a new lease', async () => {
    const { run } = resultOf(
      await h.dispatch(
        orchestrationRequest(
          'orchestration.runCreate',
          { objective: 'delegate' },
          { sessionId: SESSION_X }
        )
      )
    )
    const runId = idOf(run)
    const dispatch = h.db.createDispatchContext({
      taskId: h.db.createTask({ runId, spec: 'continue after clear' }).id,
      assigneeHandle: handle,
      assigneePaneKey: paneKey,
      processIncarnation: structuredWorkerProcessIncarnation(workerSession),
      creator: { kind: 'session', orcaSessionId: SESSION_X },
      maxDepth: Number.MAX_SAFE_INTEGER
    })
    expect(dispatch.assignee_orca_session_id).toBe(workerSession)
    const message = h.db.insertMessage({
      from: 'term_sender',
      to: `dispatch:${dispatch.id}`,
      runId,
      subject: 'status for continued worker'
    })
    const successor = testOrcaSessionId('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d')
    const original = h.records.get(workerSession)!
    original.lease = { ...original.lease, claimStatus: 'released' }
    original.conversationCommand = {
      command: 'clear',
      runtimeFence: original.lease.runtimeFence,
      operationId: 'clear-worker',
      callerKey: 'renderer',
      phase: 'committed',
      state: 'completed',
      replacementSessionId: successor
    }
    const current = sessionRecord(successor)
    h.records.set(successor, current)
    const acknowledge = () => h.dispatch(acknowledgment(successor, message.id, 'clear-worker-ack'))
    expect(resultOf(await acknowledge())).toMatchObject({ markedIds: [message.id] })
    expect(resultOf(await acknowledge())).toMatchObject({ mutation: { replayed: true } })
    current.lease = { ...current.lease, runtimeFence: current.lease.runtimeFence + 1 }
    // The current session can act, but a different live lease cannot replay an older receipt.
    expect(await acknowledge()).toMatchObject({ ok: false, error: { code: 'request_mismatch' } })
  })

  it('preserves selected wake-up mail until a released session is running again', async () => {
    const { run } = resultOf(
      await h.dispatch(
        orchestrationRequest(
          'orchestration.runCreate',
          { objective: 'sleeping coordinator' },
          { sessionId: workerSession }
        )
      )
    )
    const runId = idOf(run)
    const message = h.db.insertMessage({
      from: 'term_sender',
      to: `run:${runId}`,
      runId,
      subject: 'wake-up question',
      type: 'question'
    })
    const record = h.records.get(workerSession)!
    record.lease = { ...record.lease, claimStatus: 'released' }
    const acknowledge = () =>
      h.dispatch(acknowledgment(workerSession, message.id, 'selective_wake_up_ack'))

    const check = orchestrationRequest('orchestration.check', {}, { sessionId: workerSession })
    for (const result of [await h.dispatch(check), await acknowledge()]) {
      expect(result).toMatchObject({
        ok: false,
        error: { code: 'session_caller_not_live', data: { effectsApplied: false } }
      })
    }
    expect(h.db.getMessageById(message.id)?.read).toBe(0)

    record.lease = { ...record.lease, claimStatus: 'live' }
    expect(resultOf(await acknowledge())).toMatchObject({ markedIds: [message.id] })
    expect(resultOf(await acknowledge())).toMatchObject({ mutation: { replayed: true } })
    record.lease = { ...record.lease, runtimeFence: record.lease.runtimeFence + 1 }
    expect(await acknowledge()).toMatchObject({
      ok: false,
      error: { code: 'request_mismatch' }
    })
  })
})
