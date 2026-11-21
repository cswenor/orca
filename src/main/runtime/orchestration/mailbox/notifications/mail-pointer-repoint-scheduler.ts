const MAIL_POINTER_REPOINT_DELAY_MS = 2_000

export class MailPointerRepointScheduler {
  private readonly timersByHandle = new Map<string, ReturnType<typeof setTimeout>>()
  private readonly completions = new Map<string, PromiseWithResolvers<void>>()

  constructor(
    private readonly repoint: (handle: string, waitForRedrive: boolean) => void | Promise<void>
  ) {}

  schedule(handle: string): void {
    if (this.timersByHandle.has(handle)) {
      return
    }
    const timer = setTimeout(() => {
      if (this.timersByHandle.get(handle) !== timer) {
        return
      }
      this.timersByHandle.delete(handle)
      const completion = this.completions.get(handle)
      this.completions.delete(handle)
      const redrive = this.repoint(handle, completion !== undefined)
      if (completion) {
        if (redrive) {
          void redrive.then(completion.resolve, completion.resolve)
        } else {
          completion.resolve()
        }
      }
    }, MAIL_POINTER_REPOINT_DELAY_MS)
    timer.unref?.()
    this.timersByHandle.set(handle, timer)
  }

  completion(handle: string): Promise<void> {
    let pending = this.completions.get(handle)
    if (!pending) {
      pending = Promise.withResolvers<void>()
      this.completions.set(handle, pending)
    }
    return pending.promise
  }

  /** Handles still awaiting a repoint; a quiet scheduler holds none. */
  get pendingCount(): number {
    return this.timersByHandle.size
  }

  clear(): void {
    for (const timer of this.timersByHandle.values()) {
      clearTimeout(timer)
    }
    this.timersByHandle.clear()
    for (const pending of this.completions.values()) {
      pending.resolve()
    }
    this.completions.clear()
  }
}
