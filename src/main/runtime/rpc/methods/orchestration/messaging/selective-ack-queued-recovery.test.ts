import { expect, it, vi } from 'vitest'
import { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import {
  createBoundRun,
  createRuntime,
  driveToLiveIdle,
  pointerCount,
  PTY_ID
} from '../../../../orchestration/mailbox/terminal-pointer/orchestration-mailbox-notification-test-harness'
import { WRITE_ACCEPTED } from '../../../../../../shared/terminal/pty/lifecycle/pty-write-settlement'

it.each(['earlier', 'newer', 'cutoff'] as const)(
  'joins a queued pointer (%s)',
  assertQueuedRecovery
)

async function assertQueuedRecovery(preceding: 'earlier' | 'newer' | 'cutoff'): Promise<void> {
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
    const insertPreceding = () =>
      db.insertMessage({ from: 'term_sender', to: precedingMailbox, subject: 'Blocking mail' })
    if (preceding === 'earlier') {
      insertPreceding()
    }
    const notice = db.insertMessage({
      from: 'term_sender',
      to: noticeMailbox,
      subject: 'Recovery notice'
    })
    if (preceding !== 'earlier') {
      insertPreceding()
    }
    runtime.deliverPendingMessagesForHandle(precedingMailbox)
    await vi.advanceTimersByTimeAsync(10)
    expect(pointerCount(write)).toBe(1)
    const recovered = vi.fn()
    const recovery =
      preceding === 'cutoff'
        ? runtime.deliverPendingMessagesForHandle(noticeMailbox, undefined, notice.sequence)
        : runtime.notifyRestoredMessageArrived(noticeMailbox, notice.type, notice.id)
    if (!recovery) {
      throw new Error('Expected recovery completion for the queued mailbox')
    }
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
}

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
