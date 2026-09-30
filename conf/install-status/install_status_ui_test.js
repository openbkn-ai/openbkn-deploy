// Dependency-free check for token-carrying redirect and modal log viewer.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class Element {
  constructor(tag) { this.tag = tag; this.children = []; this.attributes = {}; this.listeners = {}; this._textContent = ''; this.value = ''; this.hidden = false; this.checked = false; this.open = false; this.showModalCalls = 0; }
  set innerHTML(value) { this.children = []; this._textContent = ''; this.html = value; }
  get innerHTML() { return this.html || ''; }
  set textContent(value) { this.children = []; this._textContent = String(value == null ? '' : value); }
  get textContent() { return this._textContent + this.children.map(child => child.textContent || '').join(''); }
  appendChild(child) { this.children.push(child); return child; }
  setAttribute(key, value) { this.attributes[key] = value; }
  getAttribute(key) { return this.attributes[key]; }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  showModal() { if (this.open) throw new Error('dialog is already open'); this.open = true; this.showModalCalls++; }
  close() { this.open = false; if (this.listeners.close) this.listeners.close({target: this, currentTarget: this}); }
  focus() {}
  querySelector(selector) { return this.children.find(child => (child.className || '').split(/\s+/).includes(selector.replace(/^\./, ''))) || null; }
}
const elements = new Map();
function element(key) { if (!elements.has(key)) elements.set(key, new Element('div')); return elements.get(key); }
function descendants(root, output = []) { output.push(root); root.children.forEach(child => { if (child instanceof Element) descendants(child, output); }); return output; }
const document = {
  createElement: tag => new Element(tag),
  createTextNode: text => ({textContent: text}),
  getElementById: element,
  querySelector: element,
  querySelectorAll: selector => {
    const all = Array.from(elements.values()).flatMap(root => descendants(root, []));
    if (selector === '.filterable') return all.filter(node => node.getAttribute('data-category'));
    if (selector === '#summary button[data-filter]') return element('summary').children.filter(node => node.getAttribute('data-filter'));
    if (selector === '#releases tbody tr') return element('#releases tbody').children;
    if (selector === '#health .hcard') return element('health').children;
    return [];
  },
};
const requests = [];
const fetch = async (url, options = {}) => {
  requests.push({url, options});
  if (String(url).includes('/logs?')) return {ok: true, status: 200, text: async () => 'safe test log line\n'};
  const data = url.includes('pods.json') ? {items: [{
    metadata: {name: 'api-abc'},
    spec: {containers: [{name: 'server', image: 'registry.example.test/api:1.2.3'}], initContainers: [{name: 'setup', image: 'registry.example.test/setup:1.2.3'}]},
    status: {phase: 'Running', startTime: '2026-09-27T08:01:00Z', conditions: [
      {type: 'PodScheduled', status: 'True', reason: ''},
      {type: 'Ready', status: 'False', reason: 'ContainersNotReady'}
    ], initContainerStatuses: [{name: 'setup', ready: true, restartCount: 1,
      state: 'terminated', reason: 'Completed', exitCode: 0}], containerStatuses: [{name: 'server', ready: false,
      restartCount: 2, state: 'waiting', reason: 'CrashLoopBackOff',
      lastTermination: {reason: 'Error', exitCode: 1}}]},
  }], refreshedAt: new Date().toISOString()} : url.includes('workloads.json') ? {items: [{
    kind: 'Deployment', metadata: {name: 'api'},
    spec: {desired: 1, containers: [{name: 'server', probes: {readiness: 'HTTP'}}]},
    status: {desired: 1, actual: 1, ready: 0, available: 0, updated: 1, conditions: [{type: 'Available', reason: 'MinimumReplicasUnavailable'}]},
  }, {
    kind: 'DaemonSet', metadata: {name: 'node-agent'},
    spec: {replicas: 3, containers: [{name: 'agent', probes: {liveness: 'HTTP'}}]},
    status: {desired: 3, actual: 3, ready: 2, available: 2, updated: 3, conditions: []},
  }, {
    kind: 'Deployment', metadata: {name: 'healthy-worker'},
    spec: {desired: 1, containers: []},
    status: {desired: 1, actual: 1, ready: 1, available: 1, updated: 1, conditions: []},
  }], refreshedAt: new Date().toISOString()} : url.includes('events.json') ? {
    items: [{lastSeen: '2026-09-26T10:00:00Z', objectKind: 'Pod', objectName: 'api-abc',
      reason: 'BackOff', summary: 'Container repeatedly failed', count: 2}],
    refreshedAt: new Date().toISOString(),
  } : {
    product: 'openbkn', version: '0.1.5', namespace: 'openbkn',
    accessAddress: {}, releases: [{name: 'api', chartVersion: '1.0.0', appVersion: '1.0.0', status: 'deployed', ready: '0/1'}],
    serviceHealth: [{name: 'api-svc', state: 'down', source: 'pod', ready: '0/1'}], depServices: [],
  };
  return {ok: true, status: 200, json: async () => data};
};
const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
assert.match(html, /<dialog id="logViewer"/, 'log viewer uses a native modal dialog');
assert.match(html, /lang=en/, 'URL parameter selects the English interface');
assert.match(html, /function t\(zh, en\)/, 'runtime strings use the language selector');
assert.match(html, /请选择容器以加载日志。/, 'Chinese is the default log-viewer placeholder');
assert.doesNotMatch(html, /<form id="authForm"|id="accessToken"/, 'the dashboard has no manual token form');
let replacedUrl = null;
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
vm.runInNewContext(script, {document, fetch, setInterval: () => {}, Date, console,
  window: {location: {protocol: 'https:', hostname: 'status.example.test', hash: '#token=test-studio-token', pathname: '/install-status', search: ''}, history: {replaceState: (_state, _title, url) => { replacedUrl = url; }}},
  btoa: value => Buffer.from(value, 'binary').toString('base64')});

setImmediate(async () => {
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(element('dashboardContent').hidden, false, 'dashboard opens directly on the administrator-only port');
  assert.ok(requests.length > 0, 'the dashboard loads status data immediately');
  assert.equal(replacedUrl, '/install-status', 'the redirect token is removed from the address bar immediately');
  const row = element('#livepods tbody').children[0];
  assert.ok(row, 'pod row renders');
  assert.match(row.children[4].children[0].textContent, /CrashLoopBackOff/);
  assert.match(row.children[4].children[0].textContent, /上次终止.*Error/);
  const diagnostics = row.children[4].children.find(child => child.tag === 'details');
  assert.ok(diagnostics, 'pod row includes an expandable sanitized describe summary');
  assert.match(diagnostics.textContent, /Pod Conditions：.*Ready=False.*ContainersNotReady/);
  assert.match(diagnostics.textContent, /Init Container\/setup.*重启 1.*镜像.*registry\.example\.test\/setup:1\.2\.3/);
  assert.match(diagnostics.textContent, /Container\/server.*反复崩溃.*镜像.*registry\.example\.test\/api:1\.2\.3/);
  assert.match(diagnostics.textContent, /Warning Event：BackOff/);
  const initItem = diagnostics.children[1].children.find(child => child.textContent.includes('Init Container/setup'));
  assert.ok(initItem.children.some(child => child.tag === 'button' && child.textContent.includes('查看日志')), 'init container can open its logs');
  const buttons = row.children[5].children;
  assert.equal(buttons.length, 1, 'each container has one log entry point');
  assert.equal(buttons[0].tag, 'button');
  assert.match(buttons[0].textContent, /查看日志/);
  assert.equal(buttons[0].attributes.target, undefined, 'logs do not open another page');
  assert.equal(element('#workloads tbody').children.length, 3);
  assert.equal(element('#warningEvents tbody').children.length, 1);
  assert.match(element('collectionFreshness').textContent, /Pods：.*Workloads：.*Events：/);
  assert.ok(element('health').children[0].children.some(child => child.tag === 'details'), 'service card includes associated troubleshooting evidence');
  assert.match(element('health').children[0].textContent, /Pod\/api-abc/);
  assert.match(element('health').children[0].textContent, /Warning · BackOff/);
  const detail = element('health').children[0].children.find(child => child.tag === 'details');
  const podDetail = detail.children[1].children.find(child => child.textContent.includes('Pod/api-abc'));
  assert.ok(podDetail.children.some(child => child.tag === 'button' && child.textContent.includes('查看日志')), 'service detail has one log action');
  const healthyRow = element('#workloads tbody').children[2];
  assert.equal(healthyRow.getAttribute('data-problem'), 'false');
  element('globalSearch').value = 'healthy-worker';
  element('globalSearch').listeners.input({target: element('globalSearch')});
  assert.equal(healthyRow.hidden, false, 'global search keeps matching resources visible');
  assert.equal(row.hidden, true, 'global search hides non-matching resources');
  element('globalSearch').value = '';
  element('globalSearch').listeners.input({target: element('globalSearch')});
  element('problemsOnly').checked = true;
  element('problemsOnly').listeners.change({target: element('problemsOnly')});
  assert.equal(healthyRow.hidden, true, 'problem-only filter hides healthy workloads');
  element('problemsOnly').checked = false;
  element('problemsOnly').listeners.change({target: element('problemsOnly')});
  const beforeRefresh = requests.length;
  element('refreshAll').listeners.click();
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.ok(requests.slice(beforeRefresh).some(request => String(request.url).includes('workloads.json')), 'manual refresh reloads live workload data');
  assert.equal(requests.some(request => String(request.url).includes('/logs')), false, 'logs are never fetched automatically');
  buttons[0].listeners.click({});
  assert.equal(element('logViewer').open, true, 'clicking a log action opens the modal');
  assert.equal(element('logViewer').showModalCalls, 1);
  await new Promise(resolve => setImmediate(resolve));
  const logRequest = requests.find(request => String(request.url).includes('/logs?'));
  assert.ok(logRequest, 'redirect token authorizes the log request');
  assert.match(logRequest.url, /pod=api-abc&container=server&previous=false&tail=100/);
  assert.equal(logRequest.options.headers.Authorization, 'Bearer test-studio-token');
  assert.match(element('logOutput').textContent, /safe test log line/);
  element('logSearch').value = 'test';
  element('logSearch').listeners.input();
  assert.match(element('logMatches').textContent, /匹配 1 \/ 1 行/);
  element('previousLog').listeners.click({});
  assert.equal(element('logViewer').showModalCalls, 1, 'switching recent/previous logs keeps the same modal open');
  await new Promise(resolve => setImmediate(resolve));
  const previousRequest = requests.filter(request => String(request.url).includes('/logs?')).at(-1);
  assert.match(previousRequest.url, /previous=true/);
  element('closeLogViewer').listeners.click();
  assert.equal(element('logViewer').open, false, 'close action dismisses the modal');
  assert.equal(element('logOutput').textContent, '', 'closing clears rendered log content');
  buttons[0].listeners.click({});
  await new Promise(resolve => setImmediate(resolve));
  let escapePrevented = false;
  element('logViewer').listeners.cancel({preventDefault() { escapePrevented = true; }});
  assert.equal(escapePrevented, true, 'Escape dismissal is handled as a modal cancel');
  assert.equal(element('logViewer').open, false, 'Escape closes the modal');
  assert.equal(element('logOutput').textContent, '', 'Escape also clears rendered log content');
  console.log('install-status modal log viewer checks passed');
});
