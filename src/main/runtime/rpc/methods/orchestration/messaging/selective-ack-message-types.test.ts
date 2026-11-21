import { afterEach, expect, it } from 'vitest'
import { createOrchestrationRpcHarness } from '../rpc-test-harness'

const harness = createOrchestrationRpcHarness()
afterEach(() => harness.cleanup())

it.each(['worker_done', 'heartbeat'] as const)(
  'refuses a mixed selection containing a %s lifecycle report without taking mail',
  async (type) => {
    const { db, ctx, activeRunId } = harness.setup()
    const to = `run:${activeRunId}`
    const escalation = db.insertMessage({
      from: 'term_sender',
      to,
      subject: 'Escalation',
      type: 'escalation'
    })
    const report = db.insertMessage({ from: 'term_sender', to, subject: 'lifecycle report', type })
    await expect(
      harness.call(
        'orchestration.ackMessages',
        { terminal: 'term_coord', ackIds: [escalation.id, report.id] },
        ctx
      )
    ).rejects.toMatchObject({
      code: 'invalid_argument',
      message: expect.stringContaining('Use a normal check')
    })
    expect(db.getMessageById(escalation.id)).toEqual(escalation)
    expect(db.getMessageById(report.id)).toEqual(report)
  }
)

it('marks explicitly selected coordination messages that do not reconcile lifecycle state', async () => {
  const { db, ctx, activeRunId } = harness.setup()
  const to = `run:${activeRunId}`
  const escalation = db.insertMessage({
    from: 'term_sender',
    to,
    subject: 'Escalation',
    type: 'escalation'
  })
  const decision = db.insertMessage({
    from: 'term_sender',
    to,
    subject: 'Decision gate',
    type: 'decision_gate'
  })
  await expect(
    harness.call(
      'orchestration.ackMessages',
      { terminal: 'term_coord', ackIds: [escalation.id, decision.id] },
      ctx
    )
  ).resolves.toMatchObject({ markedIds: [escalation.id, decision.id], alreadyReadIds: [] })
  expect(db.getMessageById(escalation.id)?.read).toBe(1)
  expect(db.getMessageById(decision.id)?.read).toBe(1)
})
