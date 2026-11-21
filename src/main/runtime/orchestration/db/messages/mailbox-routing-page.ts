import type { MessageType } from '../orchestration-records'

export const ORCHESTRATION_DELIVERY_BATCH_LIMIT = 50

export type MailboxRoutingPage = {
  routedCount: number
  hasMore: boolean
  nextSequence?: number
  types: MessageType[]
}

export type ForeignDirectMailboxRoutingPage = MailboxRoutingPage & {
  mailboxes: { mailboxHandle: string; types: MessageType[] }[]
}
