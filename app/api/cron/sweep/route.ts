import { after, NextRequest, NextResponse } from "next/server";
import { runCronSweep } from "@/lib/ops/cron-sweep";

/**
 * Comment sweep + DM sending, for deployments without an always-on worker.
 *
 * Vercel's free crons only run once a day, so point any external scheduler at
 * this route every 5 minutes, with the header `Authorization: Bearer
 * <CRON_SECRET>`. It answers at once (many free schedulers give up after a few
 * seconds) and does the work afterwards, within this route's maxDuration.
 */

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

// Leave room under maxDuration for the jobs still running when the budget ends.
const BUDGET_MS = 200_000;

async function handle(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET || process.env.NEXTAUTH_SECRET;

  if (!cronSecret || authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      { success: false, error: "Unauthorized" },
      { status: 401 }
    );
  }

  after(async () => {
    try {
      const result = await runCronSweep({ budgetMs: BUDGET_MS });
      console.log("[Cron Sweep]", JSON.stringify(result));
    } catch (error) {
      console.error(
        "[Cron Sweep] Failed:",
        error instanceof Error ? error.message : error
      );
    }
  });

  return NextResponse.json({ success: true, started: true }, { status: 202 });
}

export const GET = handle;
export const POST = handle;
