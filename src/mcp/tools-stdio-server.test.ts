import { DatabaseSync } from "node:sqlite";
import { setImmediate } from "node:timers/promises";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import {
  createPluginRegistryResourceOwner,
  drainPluginRegistryResourceDisposals,
  PluginRegistryResourceScope,
  registerPluginRegistryResourceDisposer,
} from "../plugins/registry-resources.js";
import { createDeferredCore } from "../shared/deferred.js";
import { createToolsMcpServer } from "./tools-stdio-server.js";

describe("MCP tool resource lifetime", () => {
  it.each(["server", "client"] as const)(
    "keeps SQLite open after %s close until an admitted tool actually settles",
    async (closingSide) => {
      const db = new DatabaseSync(":memory:");
      db.exec("CREATE TABLE proof (value INTEGER)");
      const registry = createEmptyPluginRegistry();
      const disposalStarted = createDeferredCore();
      const finishDisposal = createDeferredCore();
      const resources = new PluginRegistryResourceScope();
      resources.adopt({ registry, ...createPluginRegistryResourceOwner(registry, "scoped") });
      registerPluginRegistryResourceDisposer(registry, "fixture", {
        id: "database",
        async dispose() {
          disposalStarted.resolve();
          await finishDisposal.promise;
          db.close();
        },
      });
      const started = createDeferredCore();
      const cancelled = createDeferredCore();
      const finish = createDeferredCore();
      const server = createToolsMcpServer({
        name: "resource-lifetime",
        resources,
        tools: [
          {
            name: "write",
            label: "Write",
            description: "Write a synthetic row",
            parameters: { type: "object", properties: {} },
            async execute(_id, _args, signal) {
              signal?.addEventListener("abort", () => cancelled.resolve(), { once: true });
              started.resolve();
              await finish.promise;
              db.prepare("INSERT INTO proof VALUES (?)").run(1);
              return { content: [{ type: "text", text: "written" }], details: {} };
            },
          },
        ],
      });
      const client = new Client({ name: "resource-lifetime-client", version: "0.0.0" });
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
      const result = client.callTool({ name: "write", arguments: {} }).catch(() => undefined);
      await started.promise;
      if (closingSide === "client") {
        await client.close();
      }
      let closed = false;
      const close = server.close().then(() => {
        closed = true;
      });
      try {
        await cancelled.promise;
        expect(db.isOpen).toBe(true);
        expect(closed).toBe(false);
        finish.resolve();
        await disposalStarted.promise;
        await setImmediate();
        expect(db.isOpen).toBe(true);
        expect(closed).toBe(false);
      } finally {
        finish.resolve();
        finishDisposal.resolve();
        await close;
        await result;
        await client.close();
        await drainPluginRegistryResourceDisposals();
      }
      expect(db.isOpen).toBe(false);
    },
  );
});
