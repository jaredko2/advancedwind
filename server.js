const express = require('express');
const path = require('path');
const fs = require('fs');
const advancedWindPlugin = require('./index.js');

const app = express();
const PORT = 3000;
const HOST = '0.0.0.0';

// Config storage
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}
const configFile = path.join(dataDir, 'advancedwind-config.json');

// Registry of SignalK delta subscribers: Map<path, Set<{ deltaCb, excludeSelf }>>
const subscribers = new Map();

// Helper to push external simulator deltas to all subscribers
function deliverDelta(values, source = 'simulator') {
  const delta = {
    updates: [
      {
        $source: source,
        timestamp: new Date().toISOString(),
        values
      }
    ]
  };
  for (const entry of values) {
    const list = subscribers.get(entry.path);
    if (list) {
      for (const sub of [...list]) {
        try {
          sub.deltaCb(delta);
        } catch (err) {
          console.error(`Error in subscriber for ${entry.path}:`, err.message);
        }
      }
    }
  }
}

// Minimal BaconJS-style reactive-stream bus mock for signalkutilities
function createMockBus() {
  const bus = {};
  const chainMethods = [
    'onError', 'onEnd', 'skipDuplicates', 'map', 'filter', 'take', 'first',
    'toPromise', 'flatMap', 'flatMapLatest', 'merge', 'debounce',
    'debounceImmediate', 'throttle', 'delay', 'bufferWithTime', 'bufferWithCount',
    'combine', 'sampledBy', 'scan', 'fold', 'zip', 'awaiting', 'not', 'log',
    'doAction', 'doLog', 'doError', 'doEnd', 'withHandler', 'name',
    'withDescription', 'skip', 'slidingWindow', 'startWith', 'mapEnd',
    'skipWhile', 'takeWhile', 'takeUntil', 'errors', 'mapError', 'subscribe'
  ];
  for (const m of chainMethods) bus[m] = () => bus;
  bus.onValue = () => () => {};
  bus.push = () => {};
  bus.plug = () => () => {};
  bus.end = () => {};
  return bus;
}

// Build SignalK app shim
const skBase = {
  debug: (...args) => console.log('[SignalK:debug]', ...args),
  error: (...args) => console.error('[SignalK:error]', ...args),
  setPluginStatus: (status) => console.log('[Plugin Status]', status),
  setPluginError: (msg) => console.error('[Plugin Error]', msg),

  // Handle messages published by the plugin itself
  // Enforces excludeSelf: true so plugin outputs (e.g. back-calculated apparent wind)
  // never trigger feedback loops into the plugin's own inputs.
  handleMessage: (pluginId, delta) => {
    for (const update of delta?.updates ?? []) {
      for (const entry of update?.values ?? []) {
        const list = subscribers.get(entry.path);
        if (list) {
          for (const sub of [...list]) {
            // Only deliver to subscribers that explicitly opted out of excludeSelf (e.g. windShift)
            if (sub.excludeSelf === false) {
              try {
                sub.deltaCb(delta);
              } catch (e) {
                // ignore
              }
            }
          }
        }
      }
    }
  },

  getSelfPath: () => undefined,
  getPath: () => undefined,
  getMetadata: () => undefined,
  putSelfPath: (_p, _v, cb) => { if (cb) cb({ state: 'COMPLETED' }); },
  putPath: (_p, _v, cb) => { if (cb) cb({ state: 'COMPLETED' }); },

  readPluginOptions: () => {
    try {
      if (fs.existsSync(configFile)) {
        return JSON.parse(fs.readFileSync(configFile, 'utf-8'));
      }
    } catch (e) {
      console.warn('Could not read config file:', e.message);
    }
    return {};
  },

  savePluginOptions: (config, cb) => {
    try {
      fs.writeFileSync(configFile, JSON.stringify(config, null, 2));
    } catch (e) {
      console.error('Could not save config file:', e.message);
    }
    if (cb) cb();
  },

  getPluginOptions: () => ({}),
  getDataDirPath: () => dataDir,

  registerDeltaInputHandler: () => () => {},
  registerPutHandler: () => () => {},

  streambundle: {
    getSelfBus: () => createMockBus(),
    getBus: () => createMockBus(),
    getSelfStream: () => createMockBus(),
    getAvailablePaths: () => [],
  },

  subscriptionmanager: {
    subscribe: (msg, unsubscribes, _errorCb, deltaCb) => {
      // In SignalK 2.x, excludeSelf defaults to true unless explicitly overridden
      const excludeSelf = msg?.subscribeOptions?.excludeSelf !== false && msg?.excludeSelf !== false;
      for (const s of (msg.subscribe || [])) {
        if (!subscribers.has(s.path)) subscribers.set(s.path, new Set());
        const subRecord = { deltaCb, excludeSelf };
        subscribers.get(s.path).add(subRecord);
      }
      const unsub = () => {
        for (const s of (msg.subscribe || [])) {
          const list = subscribers.get(s.path);
          if (list) {
            for (const sub of [...list]) {
              if (sub.deltaCb === deltaCb) list.delete(sub);
            }
          }
        }
      };
      if (Array.isArray(unsubscribes)) unsubscribes.push(unsub);
    },
  },

  on: () => {},
  once: () => {},
  emit: () => {},
  removeListener: () => {},
  removeAllListeners: () => {},

  selfId: 'urn:mrn:signalk:uuid:00000000-0000-0000-0000-000000000000',
  selfType: 'vessels',
  selfContext: 'vessels.urn:mrn:signalk:uuid:00000000-0000-0000-0000-000000000000',
  config: {
    configPath: dataDir,
    appPath: __dirname,
    version: '2.28.0',
    name: 'signalk-server',
    basePath: '/signalk/v1',
    defaults: {},
  },
  reportOutputMessages: () => {},
  wrappedEmitter: {
    bindMethodsById: () => ({ on: () => {}, removeListener: () => {} }),
  },
};

const skApp = new Proxy(skBase, {
  get(target, prop) {
    if (prop in target) return target[prop];
    if (typeof prop === 'symbol') return undefined;
    return () => {};
  }
});

// Initialize plugin instance
const plugin = advancedWindPlugin(skApp);

// Express middleware
app.use(express.json());

// SignalK admin UI stylesheet mock endpoint (CoreUI 2.x)
app.get('/admin/.vite/manifest.json', (req, res) => {
  res.json({
    'assets/coreui.min.css': {
      file: 'assets/coreui.min.css',
      isEntry: true,
      css: ['assets/coreui.min.css']
    }
  });
});
app.use('/admin/assets', express.static(path.join(__dirname, 'node_modules/@coreui/coreui/dist/css')));

// Plugin API Router
const pluginRouter = express.Router();
plugin.registerWithRouter(pluginRouter);

// Mount plugin router on both paths for flexibility
app.use('/plugins/advancedwind', pluginRouter);
app.use('/plugins/advancedwind/public', express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

// Fallback for root
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start the plugin engine
plugin.start();

// Background Realistic Sailing Sensor Simulator
// Simulates a cruising yacht sailing on a close reach with realistic sea motion
let simStep = 0;
const simInterval = setInterval(() => {
  simStep++;
  const t = simStep * 0.5; // step in seconds

  // Heading ~ 210° (3.665 rad) with slight wave yawing (+/- 1.5°)
  const heading = 3.665 + Math.sin(t * 0.3) * 0.026;

  // Boat speed ~ 6.3 knots (3.24 m/s) with subtle surge
  const stw = 3.24 + Math.sin(t * 0.25) * 0.15;

  // SOG ~ 6.5 knots (3.34 m/s) with tidal drift
  const sog = 3.34 + Math.sin(t * 0.25) * 0.15;
  const cog = heading + 0.035; // ~2° tidal drift / leeway track

  // Apparent wind: ~14.5 knots (7.45 m/s) at ~42° (0.733 rad) with wave oscillation
  const aws = 7.45 + Math.sin(t * 0.2) * 0.6 + Math.cos(t * 0.07) * 0.3;
  const awa = 0.733 + Math.sin(t * 0.15) * 0.05 + Math.cos(t * 0.35) * 0.02;

  // Mast heel and pitch: boat heeled to port/starboard ~ 9° (0.157 rad)
  const roll = 0.157 + Math.sin(t * 0.8) * 0.025; // rolling with waves
  const pitch = 0.025 + Math.cos(t * 0.7) * 0.015; // pitching with waves

  // Leeway angle ~ 2.5° (0.043 rad)
  const leeway = 0.043 + Math.sin(t * 0.2) * 0.005;

  // Deliver deltas to subscribed handlers
  deliverDelta([
    { path: 'navigation.headingTrue', value: heading },
    { path: 'navigation.speedThroughWater', value: stw },
    { path: 'navigation.speedOverGround', value: sog },
    { path: 'navigation.courseOverGroundTrue', value: cog },
    { path: 'environment.wind.speedApparent', value: aws },
    { path: 'environment.wind.angleApparent', value: awa },
    { path: 'navigation.attitude', value: { roll, pitch } },
    { path: 'navigation.leewayAngle', value: leeway },
  ]);
}, 500);

// Graceful shutdown
process.on('SIGTERM', () => {
  clearInterval(simInterval);
  plugin.stop();
  process.exit(0);
});

process.on('SIGINT', () => {
  clearInterval(simInterval);
  plugin.stop();
  process.exit(0);
});

// Start listening on port 3000
const server = app.listen(PORT, HOST, () => {
  console.log(`Advanced Wind server running at http://${HOST}:${PORT}`);
});

module.exports = { app, server, plugin };
