import { DatabaseSync } from "node:sqlite";
import { expect, it, vi } from "vitest";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import {
  createPluginRegistryResourceOwner,
  drainPluginRegistryResourceDisposals,
  registerPluginRegistryResourceDisposer,
} from "../plugins/registry-resources.js";
import { withPluginRuntimeRegistryScope } from "../plugins/runtime/gateway-request-scope.js";
import { createPluginRuntime } from "../plugins/runtime/index.js";
import { createDeferredCore } from "../shared/deferred.js";
import type {
  RealtimeVoiceBridge,
  RealtimeVoiceBridgeCreateRequest,
} from "../talk/provider-types.js";
import { startMeetingAgentRealtimeEngine } from "./realtime-agent-engine.js";
import type { MeetingRealtimeAudioTransport } from "./realtime-audio-transport.js";
import {
  startMeetingRealtimeEngine,
  type MeetingRealtimeAudioEngineHandle,
  type MeetingRealtimeToolCallParams,
} from "./realtime-engine.js";

function createFixture(
  kind: "voice" | "agent",
  connect: () => Promise<void> = async () => {},
  options: {
    createVoiceBridge?: (request: RealtimeVoiceBridgeCreateRequest) => RealtimeVoiceBridge;
    handleToolCall?: (params: MeetingRealtimeToolCallParams) => Promise<void>;
  } = {},
) {
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE native_state (value TEXT); INSERT INTO native_state VALUES ('retained')");
  const registry = createEmptyPluginRegistry();
  const close = vi.fn(() => {});
  const owner = createPluginRegistryResourceOwner(registry, "scoped");
  registerPluginRegistryResourceDisposer(registry, "meeting-native", {
    id: "meeting-native-db",
    dispose: () => db.close(),
  });
  registry.realtimeVoiceProviders.push({
    pluginId: "meeting-native",
    source: "synthetic-native-fixture",
    provider: {
      id: "meeting-native",
      label: "Meeting native",
      isConfigured: () => true,
      createBridge:
        options.createVoiceBridge ??
        (() => ({
          connect,
          close,
          sendAudio() {},
          setMediaTimestamp() {},
          acknowledgeMark() {},
          submitToolResult() {},
          isConnected: () => true,
        })),
    },
  });
  registry.realtimeTranscriptionProviders.push({
    pluginId: "meeting-native",
    source: "synthetic-native-fixture",
    provider: {
      id: "meeting-native",
      label: "Meeting native",
      isConfigured: () => true,
      createSession: () => ({ connect, close, sendAudio() {}, isConnected: () => true }),
    },
  });
  let onFatal: (() => void) | undefined;
  const stop = vi.fn(async () => {});
  const dispose = vi.fn(async () => {});
  const transport: MeetingRealtimeAudioTransport = {
    stop,
    dispose,
    onFatal(handler) {
      onFatal = handler;
    },
    startInput() {},
    async writeOutput() {},
    async clearOutput() {},
  };
  const params = {
    config: {
      chrome: { audioFormat: "pcm16-24khz" as const },
      realtime: {
        strategy: "bidi",
        provider: "meeting-native",
        providers: { "meeting-native": {} },
      },
    },
    fullConfig: {},
    runtime: createPluginRuntime(),
    platform: {
      displayName: "Meeting fixture",
      logScope: "meeting-fixture",
      sessionIdPrefix: "meeting-fixture",
    },
    meetingSessionId: "meeting-native",
    transport,
    logger: { info() {}, warn() {}, debug() {}, error() {} },
    consultAgent: async () => ({ text: "unused" }),
  };
  return {
    db,
    owner,
    close,
    stop,
    dispose,
    fatal() {
      onFatal?.();
    },
    start: () =>
      withPluginRuntimeRegistryScope(registry, () =>
        kind === "agent"
          ? startMeetingAgentRealtimeEngine(params)
          : startMeetingRealtimeEngine({
              ...params,
              tools: [],
              handleToolCall: options.handleToolCall ?? (async () => {}),
            }),
      ),
  };
}

it.each([
  { kind: "voice", failure: "close" },
  { kind: "voice", failure: "stop" },
  { kind: "voice", failure: "dispose" },
  { kind: "agent", failure: "stop" },
  { kind: "agent", failure: "dispose" },
  { kind: "agent", failure: "close" },
] as const)(
  "retains $kind native resources after rejected $failure until cleanup retry succeeds",
  async ({ kind, failure }) => {
    const fixture = createFixture(kind);
    let handle: MeetingRealtimeAudioEngineHandle | undefined;
    try {
      handle = await fixture.start();
      fixture.owner.release();
      if (failure === "close") {
        fixture.close.mockImplementationOnce(() => {
          throw new Error("cleanup failed");
        });
      } else {
        fixture[failure].mockRejectedValueOnce(new Error("cleanup failed"));
      }
      await expect(handle.stop()).rejects.toThrow("cleanup failed");
      expect(fixture.db.prepare("SELECT value FROM native_state").get()?.value).toBe("retained");
      await expect(handle.stop()).resolves.toBeUndefined();
      if (failure === "close") {
        expect(fixture.close).toHaveBeenCalledTimes(2);
      }
      await drainPluginRegistryResourceDisposals();
      expect(fixture.db.isOpen).toBe(false);
    } finally {
      await handle?.stop().catch(() => {});
      fixture.owner.release();
      await drainPluginRegistryResourceDisposals();
      if (fixture.db.isOpen) {
        fixture.db.close();
      }
    }
  },
);

it.each(["voice", "agent"] as const)(
  "keeps %s native resources through connect finishing after fatal cleanup",
  async (kind) => {
    const entered = createDeferredCore();
    const finish = createDeferredCore();
    const fixture = createFixture(kind, async () => {
      entered.resolve();
      await finish.promise;
      expect(fixture.db.prepare("SELECT value FROM native_state").get()?.value).toBe("retained");
    });

    const starting = fixture.start();
    const startResult = starting.catch((error: unknown) => error);
    try {
      await entered.promise;
      fixture.owner.release();
      fixture.fatal();
      await vi.waitFor(() => expect(fixture.dispose).toHaveBeenCalledOnce());
      expect(fixture.db.isOpen).toBe(true);
      finish.resolve();
      await expect(startResult).resolves.toMatchObject({
        message: expect.stringMatching(/stopped during/),
      });
      await drainPluginRegistryResourceDisposals();
      expect(fixture.db.isOpen).toBe(false);
    } finally {
      finish.resolve();
      await startResult;
      fixture.owner.release();
      await drainPluginRegistryResourceDisposals();
      if (fixture.db.isOpen) {
        fixture.db.close();
      }
    }
  },
);

it("retains an accepted provider submission after startup and transport cleanup reject", async () => {
  const entered = createDeferredCore();
  const finish = createDeferredCore();
  const submitted = createDeferredCore<unknown>();
  const submissionResult = submitted.promise.catch((error: unknown) => error);
  const fixture = createFixture("voice", undefined, {
    createVoiceBridge: (request) => ({
      connect: async () => {
        request.onToolCall?.({ itemId: "item", callId: "call", name: "synthetic", args: {} });
        await entered.promise;
        throw new Error("connect failed");
      },
      close() {},
      sendAudio() {},
      setMediaTimestamp() {},
      acknowledgeMark() {},
      isConnected: () => true,
      submitToolResult: async () => {
        entered.resolve();
        await finish.promise;
        try {
          submitted.resolve(fixture.db.prepare("SELECT value FROM native_state").get()?.value);
        } catch (error) {
          submitted.reject(error);
          throw error;
        }
      },
    }),
    handleToolCall: async ({ session, event }) => {
      await session.submitToolResult(event.callId, { text: "accepted before close" });
    },
  });
  fixture.stop.mockRejectedValue(new Error("transport stop failed"));
  fixture.dispose.mockRejectedValue(new Error("transport dispose failed"));
  const starting = fixture.start();
  try {
    await entered.promise;
    fixture.owner.release();
    await expect(starting).rejects.toThrow("connect failed");
    expect(fixture.db.isOpen).toBe(true);
    finish.resolve();
    await expect(submissionResult).resolves.toBe("retained");
    await drainPluginRegistryResourceDisposals();
    expect(fixture.db.isOpen).toBe(false);
  } finally {
    finish.resolve();
    await starting.catch(() => {});
    await submissionResult;
    fixture.owner.release();
    await drainPluginRegistryResourceDisposals();
    if (fixture.db.isOpen) {
      fixture.db.close();
    }
  }
});
