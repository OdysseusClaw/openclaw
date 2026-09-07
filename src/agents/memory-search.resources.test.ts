import { expect, it } from "vitest";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import type { MemoryEmbeddingProviderAdapter } from "../plugins/memory-embedding-providers.js";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import {
  createPluginRegistryResourceOwner,
  drainPluginRegistryResourceDisposals,
  registerPluginRegistryResourceDisposer,
  requirePluginRegistryResourceScope,
} from "../plugins/registry-resources.js";
import { withPluginRuntimeRegistryScope } from "../plugins/runtime/gateway-request-scope.js";
import { resolveMemorySearchConfig } from "./memory-search.js";

it.each([false, true])(
  "owns native adapter defaults and multimodal validation until config resolution ends (throws=%s)",
  async (throws) => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    db.exec("CREATE TABLE models (name TEXT); INSERT INTO models VALUES ('native-model')");
    const registry = createEmptyPluginRegistry();
    const owner = createPluginRegistryResourceOwner(registry, "scoped");
    const failure = new Error("synthetic multimodal validation failure");
    let validations = 0;
    const adapter: MemoryEmbeddingProviderAdapter = {
      id: "native-config",
      transport: "local",
      get defaultModel() {
        requirePluginRegistryResourceScope();
        return String(db.prepare("SELECT name FROM models").get()?.name);
      },
      supportsMultimodalEmbeddings: ({ model }) => {
        requirePluginRegistryResourceScope();
        owner.release();
        expect(db.isOpen).toBe(true);
        expect(model).toBe("native-model");
        validations += 1;
        if (throws) {
          throw failure;
        }
        return true;
      },
      create: async () => ({ provider: null }),
    };
    registry.embeddingProviders.push({
      pluginId: "native-config",
      source: "synthetic-fixture",
      provider: adapter,
    });
    registerPluginRegistryResourceDisposer(registry, "native-config", {
      id: "native-embedding-database",
      dispose: () => db.close(),
    });
    try {
      const resolve = () =>
        withPluginRuntimeRegistryScope(registry, () =>
          resolveMemorySearchConfig(
            {
              memory: {
                search: {
                  provider: "native-config",
                  multimodal: { enabled: true, modalities: ["image"] },
                },
              },
            },
            "main",
          ),
        );
      if (throws) {
        expect(resolve).toThrow(failure);
      } else {
        expect(resolve()).toMatchObject({
          provider: "native-config",
          model: "native-model",
          remote: undefined,
        });
      }
      expect(validations).toBe(1);
      await drainPluginRegistryResourceDisposals();
      expect(db.isOpen).toBe(false);
    } finally {
      owner.release();
      await drainPluginRegistryResourceDisposals();
      if (db.isOpen) {
        db.close();
      }
    }
  },
);
