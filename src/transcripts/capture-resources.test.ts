import path from "node:path";
import { afterEach, expect, it } from "vitest";
import { createTempDirTracker } from "../../test/helpers/temp-dir.js";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import {
  createPluginRegistryResourceOwner,
  drainPluginRegistryResourceDisposals,
  registerPluginRegistryResourceDisposer,
} from "../plugins/registry-resources.js";
import { withPluginRuntimeRegistryScope } from "../plugins/runtime/gateway-request-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import { stopTranscriptCapture } from "./capture-operations.js";
import { activeSessions, startTranscripts } from "./capture.js";
import { TranscriptsStore, transcriptSessionSelector } from "./store.js";

const tempDirs = createTempDirTracker();
afterEach(async () => {
  for (const entry of activeSessions.values()) {
    entry.resources.release();
  }
  activeSessions.clear();
  await drainPluginRegistryResourceDisposals();
  closeOpenClawStateDatabaseForTest();
  tempDirs.cleanup();
});

it("retains the original capture database across failed stop and actual retry completion", async () => {
  const stateDir = tempDirs.make("capture-resource-owner-");
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE events (value TEXT)");
  const entered = createDeferredCore();
  const finish = createDeferredCore();
  let failing = true;
  const registry = createEmptyPluginRegistry();
  registry.transcriptSourceProviders.push({
    pluginId: "capture-resource",
    source: "synthetic-fixture",
    provider: {
      id: "capture-resource",
      name: "Capture resource",
      sourceKinds: ["live-caption"],
      start: async ({ session, onUtterance }) => {
        await onUtterance({ text: "Preserved capture" });
        return { ok: true, session };
      },
      stop: async ({ sessionId }) => {
        db.prepare("INSERT INTO events VALUES (?)").run("stop");
        if (failing) {
          return { ok: false, error: "cleanup still owned" };
        }
        entered.resolve();
        await finish.promise;
        db.prepare("INSERT INTO events VALUES (?)").run("completed");
        return { ok: true, sessionId };
      },
    },
  });
  const owner = createPluginRegistryResourceOwner(registry, "scoped");
  registerPluginRegistryResourceDisposer(registry, "capture-resource", {
    id: "native-capture-database",
    dispose: () => db.close(),
  });
  const ctx = {
    stateDir,
    caller: { kind: "operator" as const, source: "local" as const },
    logger: { warn() {} },
  };
  const store = new TranscriptsStore(path.join(stateDir, "transcripts"), {
    env: { ...process.env, OPENCLAW_STATE_DIR: stateDir },
  });
  let stopping: Promise<unknown> | undefined;
  try {
    const started = await withPluginRuntimeRegistryScope(registry, () =>
      startTranscripts({
        ctx,
        store,
        rawParams: { providerId: "capture-resource", sessionId: "notes" },
      }),
    );
    const entry = activeSessions.get(started.session.sessionId)!;
    const selection = {
      session: entry.session,
      selector: transcriptSessionSelector(entry.session),
      activeCandidate: entry,
      selectedActive: entry,
      historicalRevision: undefined,
    };
    owner.release();
    await expect(stopTranscriptCapture({ ctx, store, selection })).rejects.toThrow(
      "cleanup still owned",
    );
    expect(entry.cleanupPending).toBe(true);
    expect(db.isOpen).toBe(true);
    failing = false;
    stopping = stopTranscriptCapture({ ctx, store, selection });
    await entered.promise;
    let drained = false;
    const drain = drainPluginRegistryResourceDisposals().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);
    finish.resolve();
    await stopping;
    await drain;
    expect(db.isOpen).toBe(false);
    expect((await store.readSummary(started.session)).summary?.transcript).toEqual([
      "Preserved capture",
    ]);
  } finally {
    finish.resolve();
    try {
      await stopping;
    } finally {
      owner.release();
    }
  }
});
