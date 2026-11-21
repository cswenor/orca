import type { TuiAgent } from '../../../../shared/agent-launch/catalog/tui-agent'
import type { SleepingAgentLaunchConfig } from '../../../../shared/agent-session/lifecycle/agent-session-resume'
import { sameAgentIdentity } from '../../../../shared/agent-status/title/agent-title-owner'
import type { PtyIncarnationId } from '../../../../shared/terminal/pty/lifecycle/pty-incarnation'
import type { DaemonLaunchNote } from '../../../daemon/protocol/daemon-launch-note-protocol'

/** Main's word on whether a PTY's process still runs an Orca launch. Always replaced whole, so its
 *  identity fences an async check; never persisted, so a main restart starts unknown. */
export type LaunchEvidence =
  | { readonly kind: 'unknown' }
  | {
      readonly kind: 'gone'
      readonly incarnationId: PtyIncarnationId | null
      /** A missing restart recipe does not by itself prove a tokenless caller. */
      readonly callerToken: string | null | undefined
    }
  | {
      readonly kind: 'held'
      readonly incarnationId: PtyIncarnationId | null
      readonly agent: TuiAgent
      readonly token: string
      readonly config: SleepingAgentLaunchConfig
    }

export type PtyLaunchFacts = {
  incarnationId: PtyIncarnationId | null
  launchEvidence?: LaunchEvidence
  launchConfig?: SleepingAgentLaunchConfig | null
}

type HeldLaunch = { agent: TuiAgent; token: string; config: SleepingAgentLaunchConfig }

/** The recipe of the Orca launch running in this PTY's process as `agent`; a typed agent gets none.
 *  A wrapper launch (Claude agent teams) reports its wrapped agent, which is still that launch. */
export function heldLaunchConfig(
  pty: PtyLaunchFacts | null | undefined,
  agent: string | null | undefined
): SleepingAgentLaunchConfig | undefined {
  const evidence = pty?.launchEvidence
  return evidence?.kind === 'held' &&
    // A launch recorded before its process id was known belongs to the process until a replacement.
    (evidence.incarnationId === null || evidence.incarnationId === pty?.incarnationId) &&
    agent &&
    sameAgentIdentity(evidence.agent, agent)
    ? evidence.config
    : undefined
}

/** Main's word on this PTY's launch as `agent`: its recipe, null when none runs, undefined when main
 *  can't tell, so readers keep their earlier rule. */
export function launchStatement(
  pty: PtyLaunchFacts | null | undefined,
  agent: string | null | undefined
): SleepingAgentLaunchConfig | null | undefined {
  const kind = pty?.launchEvidence?.kind ?? 'unknown'
  return kind === 'unknown' ? undefined : (heldLaunchConfig(pty, agent) ?? null)
}

export function holdLaunch(
  pty: PtyLaunchFacts,
  incarnationId: PtyIncarnationId | null,
  launch: HeldLaunch
): void {
  pty.launchEvidence = { kind: 'held', incarnationId, ...launch }
}

/** A fresh launch only has a recipe when all launch fields are known. */
export function recordFreshLaunch(
  pty: PtyLaunchFacts,
  launch: Partial<HeldLaunch> & { tokenInjected?: boolean } = {}
): void {
  const { agent, token, config, tokenInjected } = launch
  pty.launchEvidence =
    agent && token && config
      ? { kind: 'held', incarnationId: pty.incarnationId, agent, token, config }
      : {
          kind: 'gone',
          incarnationId: pty.incarnationId,
          callerToken:
            token && pty.incarnationId ? token : token || tokenInjected ? undefined : null
        }
}

/** A finished command or confirmed exit; sticky for this process until a fresh launch. */
export function retireLaunch(
  pty: PtyLaunchFacts,
  incarnationId: PtyIncarnationId | null = pty.incarnationId
): void {
  pty.launchEvidence = { kind: 'gone', incarnationId, callerToken: null }
  pty.launchConfig = null
}

/** Learning the first id of the same process binds its evidence; a new process can't still run its
 *  predecessor's launch, and the newcomer establishes its own. */
export function retireReplacedLaunch(pty: PtyLaunchFacts, next: PtyIncarnationId | null): void {
  const previous = pty.incarnationId
  const evidence = pty.launchEvidence
  if (!evidence || !next || previous === next) {
    return
  }
  if (!previous) {
    if (evidence.kind !== 'unknown' && evidence.incarnationId === null) {
      pty.launchEvidence = { ...evidence, incarnationId: next }
    }
    return
  }
  if (evidence.kind === 'unknown' || evidence.incarnationId !== next) {
    retireLaunch(pty, previous)
  }
}

/** A reattach can't override evidence main proved about the same process. An absent daemon note
 *  clears the launch recipe but cannot distinguish a retired launch from a note never armed. */
export function recordReattachedLaunch(
  pty: PtyLaunchFacts,
  incarnationId: PtyIncarnationId | undefined,
  note: DaemonLaunchNote | null | undefined
): LaunchEvidence | null {
  const current = pty.launchEvidence
  if (current && current.kind !== 'unknown' && current.incarnationId === (incarnationId ?? null)) {
    return null
  }
  if (note === null && incarnationId) {
    pty.launchEvidence = { kind: 'gone', incarnationId, callerToken: undefined }
    pty.launchConfig = null
    return null
  }
  const pending: LaunchEvidence = { kind: 'unknown' }
  pty.launchEvidence = pending
  return note && incarnationId ? pending : null
}
