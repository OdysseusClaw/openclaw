import fs from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { clearPluginRegistryLoadCache } from "../../plugins/loader.js";
import { writePlugin } from "../../plugins/loader.test-fixtures.js";
import {
  drainPluginRegistryResourceDisposals,
  withPluginRegistryResourceOperation,
} from "../../plugins/registry-resources.js";
import { getActivePluginRegistry, resetPluginRuntimeStateForTest } from "../../plugins/runtime.js";
import { drainGlobalSingletonLifecycleState } from "../../shared/global-singleton.js";
import { createOpenClawTestState } from "../../test-utils/openclaw-test-state.js";

it("releases the actual scoped bootstrap cache when runtime module copies share a host", async () => {
  const state = await createOpenClawTestState({
    prefix: "openclaw-bootstrap-module-ownership-",
    env: { OPENCLAW_DISABLE_BUNDLED_PLUGINS: "1" },
  });
  const databasePath = state.path("bootstrap.sqlite");
  const disposedPath = state.path("disposed.txt");
  const modePath = state.path("registration-mode.txt");
  const plugin = writePlugin({
    id: "bootstrap-resource-proof",
    dir: state.path("plugin"),
    body: `module.exports = {
      id: "bootstrap-resource-proof",
      register(api) {
        const db = new (require("node:sqlite").DatabaseSync)(${JSON.stringify(databasePath)});
        db.exec("CREATE TABLE IF NOT EXISTS proof (value TEXT); INSERT INTO proof VALUES ('retained')");
        require("node:fs").writeFileSync(${JSON.stringify(modePath)}, api.registrationMode);
        api.lifecycle.registerRuntimeLifecycle({
          id: "sqlite",
          dispose() {
            db.close();
            require("node:fs").writeFileSync(${JSON.stringify(disposedPath)}, "closed");
          }
        });
        api.registerChannel({ plugin: {
          id: "bootstrap-resource-proof",
          meta: {
            id: "bootstrap-resource-proof", label: "Bootstrap resource proof",
            selectionLabel: "Bootstrap resource proof", docsPath: "/channels/bootstrap-resource-proof"
          },
          capabilities: { chatTypes: ["direct"] },
          config: { listAccountIds: () => [], resolveAccount: () => ({ accountId: "default" }) },
          outbound: { deliveryMode: "direct", sendText: async () => ({ channel: "bootstrap-resource-proof", messageId: "proof" }) }
        } });
      }
    };`,
  });
  fs.writeFileSync(
    state.path("plugin", "openclaw.plugin.json"),
    JSON.stringify({
      id: plugin.id,
      channels: [plugin.id],
      configSchema: { type: "object", properties: {} },
    }),
  );
  const config: OpenClawConfig = {
    agents: { ownership: "explicit", entries: { proof: { workspace: state.workspaceDir } } },
    plugins: {
      enabled: true,
      allow: [plugin.id],
      load: { paths: [plugin.file] },
      slots: { memory: "none" },
    },
  };
  const first = await import("./channel-bootstrap.runtime.js");
  vi.resetModules();
  const second = await import("./channel-bootstrap.runtime.js");
  try {
    expect(first.bootstrapOutboundChannelPlugin).not.toBe(second.bootstrapOutboundChannelPlugin);
    // The global Vitest setup publishes channel stubs; this fixture exercises standalone loading.
    resetPluginRuntimeStateForTest();
    expect(getActivePluginRegistry()).toBeNull();
    const invoke = (module: typeof first) =>
      withPluginRegistryResourceOperation(() =>
        module.bootstrapOutboundChannelPlugin({
          cfg: config,
          channel: plugin.id,
          agentId: "proof",
        }),
      );
    const firstRegistry = invoke(first);
    expect(firstRegistry?.channels[0]?.plugin.id).toBe(plugin.id);
    expect(invoke(second)).toBe(firstRegistry);
    expect(getActivePluginRegistry()).toBeNull();
    expect(fs.readFileSync(modePath, "utf8")).toBe("discovery");
    expect(fs.existsSync(disposedPath)).toBe(false);
    // End operation and loader-cache claims: only bootstrap cache retention remains.
    clearPluginRegistryLoadCache();
    await drainGlobalSingletonLifecycleState("restart");
    await drainPluginRegistryResourceDisposals();
    expect(fs.existsSync(disposedPath)).toBe(true);
  } finally {
    // This control also closes the stranded second cache on the defective implementation.
    first.resetOutboundChannelBootstrapStateForTests();
    second.resetOutboundChannelBootstrapStateForTests();
    clearPluginRegistryLoadCache();
    await drainPluginRegistryResourceDisposals();
    try {
      if (fs.existsSync(databasePath)) {
        const reopened = new DatabaseSync(databasePath, { readOnly: true });
        try {
          expect(reopened.prepare("SELECT value FROM proof").all()).toEqual([
            { value: "retained" },
          ]);
        } finally {
          reopened.close();
        }
      }
    } finally {
      await state.cleanup();
    }
  }
});
