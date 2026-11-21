import { orchestrationMigrationData } from '../../../shared/orchestration/orchestration-rpc-contract'
import { ORCHESTRATION_CONTRACT_RUNTIME_CAPABILITY } from '../../../shared/orchestration/orchestration-runtime-capabilities'
import type { RuntimeStatus } from '../../../shared/remote-runtime/session-graph/runtime-session-contracts'
import { RuntimeClientError } from './types'

export function assertOrchestrationContractCompatible(status: RuntimeStatus): void {
  if (!status.capabilities?.includes(ORCHESTRATION_CONTRACT_RUNTIME_CAPABILITY)) {
    throw new RuntimeClientError(
      'orchestration_migration_required',
      'The connected Orca runtime does not support the current orchestration contract. No effects were applied.',
      orchestrationMigrationData('runtime_capability_missing')
    )
  }
}
