import { afterEach, expect, it, vi } from 'vitest'
import { runLifecycleWriteTransaction } from '../../../../orchestration/db/lifecycle-write-transaction-runner'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it.each([true, false])(
  'keeps the exact competing Delivery only when it finalized empty (empty: %s)',
  async (empty) => {
    const { db, runtime, ctx, activeRunId } = harness.setup()
    const address = `run:${activeRunId}`
    const insert = (subject: string) =>
      db.insertMessage({ from: 'term_sender', to: address, subject })
    const check = (wait = false) =>
      harness.call(
        'orchestration.check',
        { terminal: 'term_coord', ...(wait ? { wait: true, timeoutMs: 5_000 } : {}) },
        ctx
      )
    const waits = vi.spyOn(runtime, 'waitForMessage')
    const waiting = [check(true), check(true)]
    await vi.waitFor(() => expect(waits).toHaveBeenCalledTimes(2))
    const original = insert('wake message')
    const select = db.getOrCreateMailboxDelivery.bind(db)
    let deliveryId: string | undefined
    let newerId: string | undefined
    vi.spyOn(db, 'getOrCreateMailboxDelivery').mockImplementationOnce((params) => {
      const claim = select(params)!
      deliveryId = claim.delivery.id
      if (empty) {
        db.markAsRead([original.id])
      }
      // Model an independent ordinary take committing this same claim before the waiters resume.
      runLifecycleWriteTransaction(db.db, 'competing_take', () =>
        db.finalizeMailboxDelivery({ ...params, claim, messages: empty ? [] : claim.messages })
      )
      newerId = insert('next message').id
      return claim
    })
    runtime.notifyMessageArrived(address, 'status')

    for (const response of await Promise.all(waiting)) {
      expect(response).toMatchObject({
        deliveryId: empty ? deliveryId : null,
        count: 0,
        messages: []
      })
    }
    expect(db.getDeliveryRaw(deliveryId!)?.message_ids).toBe(
      JSON.stringify(empty ? [] : [original.id])
    )
    expect(db.getMessageById(newerId!)?.read).toBe(0)
    expect(await check()).toMatchObject({ messages: [{ subject: 'next message' }] })
  }
)
