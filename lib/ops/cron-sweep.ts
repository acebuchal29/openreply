/**
 * One scheduled pass without an always-on worker.
 *
 * An external scheduler (any free cron service) calls /api/cron/sweep every few
 * minutes. Each call runs the comment sweep, then works the DM queue until it is
 * empty, then stops. This replaces `npm run worker` for self-hosters who have
 * no machine running 24 hours a day: everything happens inside a Vercel
 * function. Jobs that are delayed past this run (retries, follow-ups) are picked
 * up by a later run.
 */

import { getDMQueue, getRedisConnection } from "@/lib/queue/client";
import { createDMWorker } from "@/lib/queue/dm-worker";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";
import { reconcileComments } from "@/lib/polling/comment-reconciler";

const LOCK_KEY = "lock:cron-sweep";
// Health stays green for two missed runs of a 5-minute schedule.
const HEARTBEAT_TTL_SECONDS = Number(process.env.CRON_SWEEP_HEALTH_TTL_SECONDS ?? 660);
// Wait for a delayed job only if it comes due this soon.
const DELAYED_WAIT_MS = 45_000;

export interface CronSweepResult {
  skipped?: "already-running";
  processed: number;
  durationMs: number;
  stoppedBy?: "empty" | "budget";
}

export async function runCronSweep({
  budgetMs,
}: {
  budgetMs: number;
}): Promise<CronSweepResult> {
  const started = Date.now();
  const redis = getRedisConnection();

  // Two runs at once would sweep twice and compete for the same jobs. The lock
  // expires on its own if a run dies halfway.
  const lockSeconds = Math.ceil(budgetMs / 1000) + 30;
  const gotLock = await redis.set(LOCK_KEY, String(started), "EX", lockSeconds, "NX");
  if (!gotLock) {
    return { skipped: "already-running", processed: 0, durationMs: 0 };
  }

  let processed = 0;
  let stoppedBy: CronSweepResult["stoppedBy"];
  try {
    await recordWorkerHeartbeat({
      pid: process.pid,
      hostname: "vercel-cron",
      startedAt: new Date(started).toISOString(),
      ttlSeconds: HEARTBEAT_TTL_SECONDS,
    });

    await reconcileComments();

    const queue = getDMQueue();
    const worker = createDMWorker({ autorun: false });
    worker.on("completed", () => {
      processed += 1;
    });
    worker.on("failed", () => {
      processed += 1;
    });
    const running = worker.run();

    const deadline = started + budgetMs;
    let emptyChecks = 0;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 1500));
      const counts = await queue.getJobCounts("waiting", "active", "prioritized");
      const pending =
        (counts.waiting ?? 0) + (counts.active ?? 0) + (counts.prioritized ?? 0);
      if (pending > 0) {
        emptyChecks = 0;
        continue;
      }
      if (await delayedDueSoon(queue)) {
        emptyChecks = 0;
        continue;
      }
      emptyChecks += 1;
      if (emptyChecks >= 2) {
        stoppedBy = "empty";
        break;
      }
    }
    if (!stoppedBy) stoppedBy = "budget";

    // close() lets jobs already running finish before it resolves.
    await worker.close();
    await running.catch(() => {});

    await recordWorkerHeartbeat({
      pid: process.pid,
      hostname: "vercel-cron",
      startedAt: new Date(started).toISOString(),
      ttlSeconds: HEARTBEAT_TTL_SECONDS,
    });
  } finally {
    await redis.del(LOCK_KEY).catch(() => {});
  }

  return { processed, durationMs: Date.now() - started, stoppedBy };
}

async function delayedDueSoon(
  queue: ReturnType<typeof getDMQueue>
): Promise<boolean> {
  const delayed = await queue.getDelayed(0, 20);
  const now = Date.now();
  return delayed.some(
    (job) => job.timestamp + (job.delay ?? 0) - now <= DELAYED_WAIT_MS
  );
}
