import { describe, expect, it } from 'vitest'
import { OrchestrationRequestEvidence } from './orchestration-compatibility-envelope'

const SESSION = '4a1f6c2e-8b3d-4e7a-9c15-0d2b6e8f1a37'
const local = { remote: false, mutation: true, requestId: undefined }

describe('OrchestrationRequestEvidence', () => {
  it('uses paired execution targeting without forwarding source shell proof', () => {
    const evidence = new OrchestrationRequestEvidence({
      ORCA_TERMINAL_HANDLE: 'term_source',
      ORCA_PANE_KEY: 'tab_source:leaf_source'
    })
    const paired = { remote: true, mutation: true, requestId: 'req_target' }
    expect(evidence.envelopeFor('orchestration.ackMessages', paired)).toEqual({
      compatibilityInvocationId: 'req_target',
      orchestrationCompatibilityEvidence: undefined
    })
    expect(
      evidence.envelopeFor('orchestration.check', paired).orchestrationCompatibilityEvidence
    ).toEqual({ terminalHandle: 'term_source', paneKey: 'tab_source:leaf_source' })
  })

  it.each(['connector.act', 'connector.reply'])(
    'sends the calling chat’s session with %s, so the host can check it posts only for itself',
    (method) => {
      const evidence = new OrchestrationRequestEvidence({ ORCA_AGENT_SESSION_ID: SESSION })
      expect(evidence.envelopeFor(method, local)).toEqual({
        orchestrationCompatibilityEvidence: { agentSessionId: SESSION }
      })
    }
  )
})
