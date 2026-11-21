import '../../../dispatch/testing/unused-default-rpc-methods.test-fixture'
import { once } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import { createConnection } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, expect, it, vi } from 'vitest'
import { ORCHESTRATION_CONTRACT_VERSION } from '../../../../../../shared/orchestration/orchestration-runtime-capabilities'
import { ORCHESTRATION_MAIL_RECONCILIATION_TIMEOUT_MS } from '../../../../../../shared/orchestration/orchestration-timing-budgets'
import { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { OrcaRuntimeService } from '../../../../orca-runtime'
import type { RpcResponse } from '../../../core'
import { RpcDispatcher } from '../../../dispatcher'
import { readRuntimeMetadata } from '../../../server/runtime-metadata'
import { OrcaRuntimeRpcServer } from '../../../server/runtime-rpc'
import type { RpcMessageContext } from '../../../transport'
import { ORCHESTRATION_METHODS } from '../orchestration'
import { COORDINATOR_PANE_KEY, createOrchestrationRpcHarness } from '../rpc-test-harness'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

function setupAck() {
  const { db, runtime } = harness.setup(false)
  const message = db.insertMessage({ from: 'term_sender', to: 'term_coord', subject: 'selected' })
  const request = {
    id: 'rpc_ack_recovery',
    authToken: 'test',
    method: 'orchestration.ackMessages',
    params: { terminal: 'term_coord', ackIds: [message.id] },
    orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
    orchestrationRequestId: 'ack_recovery'
  }
  return { db, runtime, message, request }
}

it('times out without marking or recording the request while startup recovery still runs', async () => {
  const { db, runtime, message, request } = setupAck()
  const recovery = Promise.withResolvers<void>()
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockReturnValue(recovery.promise)
  vi.useFakeTimers()
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const pending = dispatcher.dispatch(request)
  try {
    await vi.advanceTimersByTimeAsync(ORCHESTRATION_MAIL_RECONCILIATION_TIMEOUT_MS)
    const result = await Promise.race([pending, Promise.resolve('still waiting')])
    expect(result).toMatchObject({
      ok: false,
      error: { code: 'runtime_unavailable', data: { effectsApplied: false } }
    })
    recovery.resolve()
    await recovery.promise
    expect(db.getMessageById(message.id)?.read).toBe(0)
    await expect(
      dispatcher.dispatch({ ...request, params: { ...request.params, replayOnly: true } })
    ).resolves.toMatchObject({ ok: false, error: { code: 'operation_unknown' } })
  } finally {
    recovery.resolve()
    await pending
    vi.useRealTimers()
  }
})

it('replays a completed receipt without joining unrelated mailbox recovery', async () => {
  const { db, runtime, message, request } = setupAck()
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  await expect(dispatcher.dispatch(request)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [message.id], mutation: { replayed: false } }
  })
  const recovery = Promise.withResolvers<void>()
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockReturnValue(recovery.promise)
  vi.useFakeTimers()
  const replay = dispatcher.dispatch({
    ...request,
    params: { ...request.params, replayOnly: true }
  })
  try {
    await vi.advanceTimersByTimeAsync(0)
    expect(await Promise.race([replay, Promise.resolve('still waiting')])).toMatchObject({
      ok: true,
      result: { markedIds: [message.id], mutation: { replayed: true } }
    })
    expect(db.getMessageById(message.id)?.read).toBe(1)
  } finally {
    recovery.resolve()
    await replay
    vi.useRealTimers()
  }
})

it('a cold retry leaves no receipt while shared startup can fail independently', async () => {
  const profile = await mkdtemp(join(tmpdir(), 'orca-cold-ack-'))
  const databasePath = join(profile, 'orchestration.sqlite')
  const seed = new OrchestrationDb(databasePath)
  const message = seed.insertMessage({ from: 'term_sender', to: 'term_coord', subject: 'selected' })
  seed.close()

  const routing = Promise.withResolvers<void>()
  const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})
  const runtime = new (class extends OrcaRuntimeService {
    protected override orchestrationDbPath() {
      return databasePath
    }

    protected override scheduleRestoredMessageRepoints() {
      return routing.promise
    }
  })(null)
  vi.spyOn(runtime, 'getTerminalPaneKey').mockReturnValue(COORDINATOR_PANE_KEY)
  vi.spyOn(runtime, 'getLiveTerminalPaneKey').mockReturnValue(COORDINATOR_PANE_KEY)
  vi.spyOn(runtime, 'getTerminalProcessIncarnation').mockReturnValue('runtime_test:term_coord:1')
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })

  try {
    await expect(
      dispatcher.dispatch({
        id: 'rpc_cold_retry',
        authToken: 'test',
        method: 'orchestration.ackMessages',
        params: { terminal: 'term_coord', ackIds: [message.id], replayOnly: true },
        orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
        orchestrationRequestId: 'cold_retry'
      })
    ).resolves.toMatchObject({ ok: false, error: { code: 'operation_unknown' } })

    const failure = new Error('restored mailbox routing failed during a later page')
    routing.reject(failure)
    await vi.waitFor(() =>
      expect(warning).toHaveBeenCalledWith(
        '[orchestration] restored mailbox routing failed',
        failure
      )
    )
    const db = runtime.getOrchestrationDb()
    expect(db.getMessageById(message.id)?.read).toBe(0)
    expect(
      db.getMutationReceipt(db.readLocalMutationCallerFingerprint()!, 'cold_retry')
    ).toBeUndefined()
  } finally {
    routing.resolve()
    runtime.getOrchestrationDb().close()
    warning.mockRestore()
    await rm(profile, { recursive: true, force: true })
  }
})

it('a disconnected local socket cannot mark or record mail when recovery later finishes', async () => {
  const { db, runtime, message, request } = setupAck()
  const userDataPath = await mkdtemp(join(tmpdir(), 'orca-disconnected-ack-'))
  const entered = Promise.withResolvers<void>()
  const recovery = Promise.withResolvers<void>()
  const completed = Promise.withResolvers<RpcResponse>()
  vi.spyOn(runtime, 'waitForOrchestrationStartupMailRecovery').mockImplementation(() => {
    entered.resolve()
    return recovery.promise
  })
  const server = new (class extends OrcaRuntimeRpcServer {
    protected override async handleMessage(rawMessage: string, context?: RpcMessageContext) {
      const response = await super.handleMessage(rawMessage, context)
      completed.resolve(response)
      return response
    }
  })({ runtime, userDataPath, methods: ORCHESTRATION_METHODS })
  await server.start()
  const metadata = readRuntimeMetadata(userDataPath)!
  const socket = createConnection(metadata.transports[0]!.endpoint)
  try {
    await once(socket, 'connect')
    socket.write(`${JSON.stringify({ ...request, authToken: metadata.authToken })}\n`)
    await expect(
      Promise.race([entered.promise.then(() => 'entered'), completed.promise])
    ).resolves.toBe('entered')
    socket.destroy()
    await expect(completed.promise).resolves.toMatchObject({
      ok: false,
      error: { code: 'request_aborted', data: { effectsApplied: false } }
    })
    recovery.resolve()
    await recovery.promise
    expect(db.getMessageById(message.id)?.read).toBe(0)
    expect(
      db.getMutationReceipt(
        db.readLocalMutationCallerFingerprint()!,
        request.orchestrationRequestId
      )
    ).toBeUndefined()
  } finally {
    socket.destroy()
    recovery.resolve()
    await server.stop()
    await rm(userDataPath, { recursive: true, force: true })
  }
})
