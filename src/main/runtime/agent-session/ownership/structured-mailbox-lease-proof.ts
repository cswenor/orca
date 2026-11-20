import { createHash, timingSafeEqual } from 'node:crypto'

/** Keep the process-ownership token itself out of descendant command environments. */
export function structuredMailboxLeaseProof(spawnToken: string): Buffer {
  return createHash('sha256').update('orca:structured-mailbox-lease\0').update(spawnToken).digest()
}

export function matchesStructuredMailboxLease(proof: unknown, spawnToken: string | undefined) {
  return (
    !!spawnToken &&
    typeof proof === 'string' &&
    /^[0-9a-f]{64}$/.test(proof) &&
    timingSafeEqual(Buffer.from(proof, 'hex'), structuredMailboxLeaseProof(spawnToken))
  )
}
