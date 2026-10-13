const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const playerHtml = readFileSync(
  path.join(__dirname, "..", "assets", "web-player.html"),
  "utf8",
);

function extract(source, pattern, description) {
  const match = source.match(pattern);
  assert.ok(match, `Could not find ${description} in web-player.html`);
  return match[1];
}

function createWorkletProcessor() {
  const source = extract(
    playerHtml,
    /const WORKLET_CODE = `([\s\S]*?)`;/,
    "AudioWorklet source",
  );
  let Processor;
  const sandbox = {
    sampleRate: 48_000,
    AudioWorkletProcessor: class {
      constructor() {
        this.port = { postMessage() {} };
      }
    },
    registerProcessor(_name, processor) {
      Processor = processor;
    },
  };

  vm.runInNewContext(source, sandbox);
  return Processor;
}

test("default multichannel playback routes a selected pair without summing channels", () => {
  const source = extract(
    playerHtml,
    /(function downmixToStereo\(interleaved\) \{[\s\S]*?\n  \})/,
    "stereo downmix function",
  );
  const context = vm.createContext({
    totalChannels: 4,
    activeChannels: new Set([0, 1, 2, 3]),
    mixMode: "first2",
  });

  const result = vm.runInContext(
    `${source}; downmixToStereo(new Float32Array([1, 10, 2, 20, 3, 30, 4, 40]))`,
    context,
  );

  assert.deepEqual(Array.from(result), [1, 10, 3, 30]);
});

test("AudioWorklet linearly resamples source PCM to the output device rate", () => {
  const Processor = createWorkletProcessor();
  const processor = new Processor({
    processorOptions: {
      capacitySamples: 48_000,
      prebufferSeconds: 0,
      sourceSampleRate: 24_000,
    },
  });
  processor.port.onmessage({
    data: {
      type: "data",
      l: new Float32Array([0, 1, 2, 3, 4, 5, 6, 7]),
      r: new Float32Array([0, 1, 2, 3, 4, 5, 6, 7]),
    },
  });

  const left = new Float32Array(4);
  const right = new Float32Array(4);
  processor.process([], [[left, right]]);

  assert.deepEqual(
    Array.from(left, (value) => Number(value.toFixed(3))),
    [0, 0.5, 1, 1.5],
  );
  assert.deepEqual(Array.from(right), Array.from(left));
});

test("HTTP fallback linearly resamples source PCM to the output device rate", async () => {
  const setupSource = extract(
    playerHtml,
    /(async function setupPlaybackEngine\(\) \{[\s\S]*?\n  \})\s*\n\s*\/\/ Original incoming PCM handling\./,
    "fallback playback engine",
  );
  const scriptProcessor = { connect() {}, onaudioprocess: null };
  const context = vm.createContext({
    window: { isSecureContext: false },
    audioCtx: {
      sampleRate: 48_000,
      audioWorklet: null,
      destination: {},
      createScriptProcessor() {
        return scriptProcessor;
      },
    },
    sampleRate: 24_000,
    ringCapacity: 0,
    ringBuffers: [],
    ringWrite: 0,
    ringRead: 0,
    ringAvailable: 0,
    fallbackUnderruns: 0,
    processorNode: null,
    usingWorklet: false,
    statsEl: { textContent: "" },
  });

  await vm.runInContext(
    `(${setupSource})()`,
    context,
  );
  vm.runInContext(
    "ringBuffers[0].set([0, 1, 2, 3, 4, 5, 6, 7]);" +
      "ringBuffers[1].set([0, 1, 2, 3, 4, 5, 6, 7]);" +
      "ringAvailable = 3_600; ringWrite = 3_600;",
    context,
  );

  const left = new Float32Array(4);
  const right = new Float32Array(4);
  scriptProcessor.onaudioprocess({
    outputBuffer: {
      getChannelData(channel) {
        return channel === 0 ? left : right;
      },
    },
  });

  assert.deepEqual(
    Array.from(left, (value) => Number(value.toFixed(3))),
    [0, 0.5, 1, 1.5],
  );
  assert.deepEqual(Array.from(right), Array.from(left));
});

test("AudioWorklet emits silence and records underruns instead of replaying stale samples", () => {
  const Processor = createWorkletProcessor();
  const processor = new Processor({
    processorOptions: {
      capacitySamples: 48_000,
      prebufferSeconds: 0,
      sourceSampleRate: 48_000,
    },
  });
  const left = new Float32Array(128).fill(1);
  const right = new Float32Array(128).fill(1);

  processor.process([], [[left, right]]);

  assert.ok(Array.from(left).every((value) => value === 0));
  assert.ok(Array.from(right).every((value) => value === 0));
  assert.equal(processor.underrunCount, 1);
});
