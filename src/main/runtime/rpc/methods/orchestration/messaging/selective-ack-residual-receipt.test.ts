import { afterEach, expect, it, vi } from 'vitest'
import { ORCHESTRATION_CONTRACT_VERSION } from '../../../../../../shared/orchestration/orchestration-runtime-capabilities'
import {
  createRootDispatch,
  reattachDispatchConsumer
} from '../../../../orchestration/db/root-dispatch-test-fixture'
import { RpcDispatcher } from '../../../dispatcher'
import { ORCHESTRATION_METHODS } from '../orchestration'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it('replays pre-bind mail after message reset only under the original Dispatch authority', async () => {
  const { db, runtime } = harness.setup()
  const terminal = 'term_lead'
  const pane = 'tab_lead:22222222-2222-4222-9222-222222222222'
  const processIncarnation = 'runtime_test:term_lead:1'
  vi.mocked(runtime.getTerminalPaneKey).mockImplementation((handle) =>
    handle === terminal ? pane : harness.coordinatorPaneKey
  )
  const dispatch = createRootDispatch(
    db,
    db.createTask({ spec: 'worker before child binding' }).id,
    terminal,
    pane,
    undefined,
    processIncarnation
  )
  db.createRun({ objective: 'child Run', coordinatorHandle: terminal, coordinatorPaneKey: pane })
  const selected = db.insertMessage({
    from: 'term_coord',
    to: `dispatch:${dispatch.id}`,
    runId: dispatch.run_id,
    subject: 'pre-bind mail'
  })
  const dispatcher = new RpcDispatcher({ runtime, methods: ORCHESTRATION_METHODS })
  const ack = (replayOnly: boolean) =>
    dispatcher.dispatch({
      id: 'rpc_prebind_reset',
      authToken: 'test',
      method: 'orchestration.ackMessages',
      params: { terminal, ackIds: [selected.id], replayOnly },
      orchestrationContractVersion: ORCHESTRATION_CONTRACT_VERSION,
      orchestrationRequestId: 'prebind_reset'
    })
  await expect(ack(false)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: false } }
  })
  db.resetMessages()
  expect(db.getMessageById(selected.id)).toBeUndefined()
  await expect(ack(true)).resolves.toMatchObject({
    ok: true,
    result: { markedIds: [selected.id], mutation: { replayed: true } }
  })
  reattachDispatchConsumer(db, { dispatchId: dispatch.id, paneKey: pane, processIncarnation })
  await expect(ack(true)).resolves.toMatchObject({
    ok: false,
    error: { code: 'request_mismatch' }
  })
})
