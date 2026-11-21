import type {
  RuntimePtyWorktreeRecord,
  RuntimeLeafRecord
} from '../records/runtime-terminal-state-records'
export type PtyIncarnationHandleRecord = Readonly<{
  handle: string
  incarnationId: string | null
  leafKey: string
}>

export type TerminalHandlePty = {
  record: TerminalHandleRecord
  pty: RuntimePtyWorktreeRecord
}

export type TerminalHandleLeaf = {
  record: TerminalHandleRecord
  leaf: RuntimeLeafRecord
}

export type TerminalHandleRecord = Readonly<{
  handle: string
  runtimeId: string
  rendererGraphEpoch: number
  worktreeId: string
  tabId: string
  leafId: string
  ptyId: string | null
  ptyGeneration: number
  /** Issue-time stamp of the pane main bound a terminal handle for; read only by recoverTerminalPane. */
  issuedForPaneKey?: string
}>

export function syntheticPtyHandleRecord(
  handle: string,
  runtimeId: string,
  rendererGraphEpoch: number,
  pty: RuntimePtyWorktreeRecord
): TerminalHandleRecord {
  const id = `pty:${pty.ptyId}`
  return {
    handle,
    runtimeId,
    rendererGraphEpoch,
    worktreeId: pty.worktreeId,
    tabId: id,
    leafId: id,
    ptyId: pty.ptyId,
    ptyGeneration: 0
  }
}
