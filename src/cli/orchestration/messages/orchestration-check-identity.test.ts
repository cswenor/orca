import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ORCHESTRATION_SELECTIVE_ACK_RUNTIME_CAPABILITY } from '../../../shared/orchestration/orchestration-runtime-capabilities'
import { RuntimeClientError } from '../../runtime/connection/types'

const callMock = vi.hoisted(() => vi.fn())
const getTerminalHandleMock = vi.hoisted(() => vi.fn())
const originalTerminalHandle = process.env.ORCA_TERMINAL_HANDLE
const originalPaneKey = process.env.ORCA_PANE_KEY

const printResultMock = vi.hoisted(() => vi.fn())
vi.mock('../../output/command-result-output', () => ({ printResult: printResultMock }))
vi.mock('../../terminals/terminal-selector', () => ({ getTerminalHandle: getTerminalHandleMock }))

import { ORCHESTRATION_HANDLERS } from '../orchestration-handlers'

describe('orchestration check identity', () => {
  beforeEach(() => {
    callMock.mockReset().mockResolvedValue({ result: { messages: [], count: 0 } })
    printResultMock.mockReset()
    getTerminalHandleMock.mockReset()
    delete process.env.ORCA_TERMINAL_HANDLE
    delete process.env.ORCA_PANE_KEY
  })

  afterEach(() => {
    if (originalTerminalHandle === undefined) {
      delete process.env.ORCA_TERMINAL_HANDLE
    } else {
      process.env.ORCA_TERMINAL_HANDLE = originalTerminalHandle
    }
    if (originalPaneKey === undefined) {
      delete process.env.ORCA_PANE_KEY
    } else {
      process.env.ORCA_PANE_KEY = originalPaneKey
    }
  })

  const invokeCheck = (flags: Map<string, string | boolean>, json = true) =>
    ORCHESTRATION_HANDLERS['orchestration check']({
      flags,
      client: { call: callMock },
      cwd: '/tmp/repo',
      json
    } as never)

  it('does not issue any check RPC when the execution host lacks selective ack', async () => {
    await expect(invokeCheck(new Map([['ack-ids', 'msg_selected']]))).rejects.toMatchObject({
      code: 'incompatible_runtime'
    })
    expect(callMock.mock.calls.map(([method]) => method)).toEqual(['status.get'])
  })

  it('rejects empty entries in --ack-ids before contacting the runtime', async () => {
    await expect(invokeCheck(new Map([['ack-ids', 'msg_selected,']]))).rejects.toMatchObject({
      code: 'invalid_argument'
    })
    expect(callMock).not.toHaveBeenCalled()
  })

  it.each([
    ['wait', true],
    ['ack', ''],
    ['types', ''],
    ['timeout-ms', '']
  ])(
    'rejects --ack-ids with explicitly supplied --%s before contacting the runtime',
    async (flag, value) => {
      await expect(
        invokeCheck(
          new Map<string, string | boolean>([
            ['ack-ids', 'msg_selected'],
            [flag, value]
          ])
        )
      ).rejects.toMatchObject({ code: 'invalid_argument' })
      expect(callMock).not.toHaveBeenCalled()
    }
  )

  it.each([true, false])(
    'never falls back to ordinary check (distinct method present: %s)',
    async (available) => {
      process.env.ORCA_TERMINAL_HANDLE = 'term_coord'
      callMock.mockResolvedValueOnce({
        result: { capabilities: [ORCHESTRATION_SELECTIVE_ACK_RUNTIME_CAPABILITY] }
      })
      if (available) {
        callMock.mockResolvedValueOnce({ result: { markedIds: ['msg_selected'] } })
      } else {
        callMock.mockRejectedValueOnce(
          new RuntimeClientError('method_not_found', 'Unknown RPC method')
        )
      }
      const outcome = invokeCheck(new Map([['ack-ids', 'msg_selected']]))
      if (available) {
        await outcome
        expect(printResultMock).toHaveBeenCalledWith(
          { result: { markedIds: ['msg_selected'] } },
          true,
          expect.any(Function)
        )
      } else {
        await expect(outcome).rejects.toMatchObject({ code: 'method_not_found' })
      }
      expect(callMock.mock.calls.map(([method]) => method)).toEqual([
        'status.get',
        'orchestration.ackMessages'
      ])
    }
  )

  it('rejects an empty --terminal instead of consuming the ambient mailbox', async () => {
    process.env.ORCA_TERMINAL_HANDLE = 'term_coord'

    await expect(
      invokeCheck(new Map<string, string | boolean>([['terminal', '']]))
    ).rejects.toMatchObject({ code: 'invalid_argument' })
    expect(callMock).not.toHaveBeenCalled()
  })
})
