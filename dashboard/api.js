const STORE_KEY = 'bth4j.session';

function loadStored() {
  try {
    const raw = sessionStorage.getItem(STORE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (e) {
    return {};
  }
}

const stored = loadStored();
export const session = Vue.reactive({
  user: stored.user || null,
  role: stored.role || null,
  csrf: stored.csrf || null,
});

export function isAdmin() {
  return session.role === 'ADMIN';
}

export function setSession(s) {
  session.user = s.user;
  session.role = s.role;
  session.csrf = s.csrf || session.csrf;
  try {
    sessionStorage.setItem(STORE_KEY, JSON.stringify({ user: session.user, role: session.role, csrf: session.csrf }));
  } catch (e) {
    /* storage can be blocked; the in-memory copy still works for this page load */
  }
}

export function clearSession() {
  session.user = null;
  session.role = null;
  session.csrf = null;
  try {
    sessionStorage.removeItem(STORE_KEY);
  } catch (e) {
    /* nothing stored */
  }
}

export class ApiError extends Error {
  constructor(status, message, code = null) {
    super(message);
    this.status = status;
    this.code = code;
  }
  // the task changed since the page loaded it: reloading helps
  get stale() {
    return this.status === 409 && this.code === 'STALE';
  }
  // the task is in a state that does not allow the action: the message says which rule
  get wrongState() {
    return this.status === 409 && this.code === 'WRONG_STATE';
  }
  get gone() {
    return this.status === 404 && this.code === 'NOT_FOUND';
  }
}

function url(path, query) {
  const base = (window.BTH4J_API || '').replace(/\/+$/, '') + '/api/v1';
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && (v !== '' || k === 'cursor')) q.set(k, String(v));
  }
  const qs = q.toString();
  return base + path + (qs ? '?' + qs : '');
}

// The offset of the server's clock from the browser's, which decides "late" and "silent": the Date header has
// whole seconds, so one answer only bounds the offset, and each further answer narrows the bounds.
let offsetLow = 0;
let offsetHigh = 0;
let offsetKnown = false;

function noteServerClock(res, sent, received) {
  const date = Date.parse(res.headers.get('Date'));
  if (Number.isNaN(date)) return;
  const low = date - received;
  const high = date + 1000 - sent;
  if (offsetKnown && low <= offsetHigh && high >= offsetLow) {
    offsetLow = Math.max(offsetLow, low);
    offsetHigh = Math.min(offsetHigh, high);
  } else {
    offsetLow = low;
    offsetHigh = high;
    offsetKnown = true;
  }
}

export function serverNow() {
  return Date.now() + (offsetKnown ? (offsetLow + offsetHigh) / 2 : 0);
}

async function request(method, path, { query, body, noRedirect } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (method !== 'GET' && session.csrf) headers['X-CSRF-Token'] = session.csrf;
  let res;
  const sent = Date.now();
  try {
    res = await fetch(url(path, query), {
      method,
      headers,
      credentials: 'include',
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    throw new ApiError(0, 'Cannot reach the server');
  }
  noteServerClock(res, sent, Date.now());
  if (res.status === 401 && !noRedirect) {
    clearSession();
    location.hash = '#/login';
    throw new ApiError(401, 'Session expired, sign in again');
  }
  if (res.status === 204) return null;
  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (e) {
      data = null;
    }
  }
  if (!res.ok) {
    const code = (data && data.code) || null;
    if (res.status === 409 && code === 'STALE') {
      throw new ApiError(409, 'This item changed meanwhile. Reload it and try again.', code);
    }
    if (res.status === 404 && code === 'NOT_FOUND') {
      throw new ApiError(404, 'This ' + (path.startsWith('/schedules') ? 'schedule' : 'task') + ' no longer exists.', code);
    }
    const detail = data && (data.error || data.message);
    if (res.status === 409) throw new ApiError(409, detail || 'Request failed (HTTP 409)', code);
    if (res.status === 401) throw new ApiError(401, detail || 'Wrong user or password');
    if (res.status === 403) throw new ApiError(403, detail || 'Not allowed for your role');
    throw new ApiError(res.status, detail || 'Request failed (HTTP ' + res.status + ')');
  }
  return data;
}

const enc = encodeURIComponent;

// The newest /overview answer and when it came, whoever asked: the shell's header reads it, so the overview page
// and the shell do not both poll.
export const latestOverview = Vue.shallowRef(null);

async function overview() {
  const data = await request('GET', '/overview');
  latestOverview.value = { data, at: Date.now() };
  return data;
}

export const api = {
  login: (user, password) => request('POST', '/session', { body: { user, password }, noRedirect: true }),
  logout: () => request('DELETE', '/session', { noRedirect: true }),
  me: () => request('GET', '/me', { noRedirect: true }),
  health: () => request('GET', '/health'),
  overview,
  queues: () => request('GET', '/queues'),
  pauseQueue: (name) => request('POST', '/queues/' + enc(name) + '/pause', { body: {} }),
  resumeQueue: (name) => request('POST', '/queues/' + enc(name) + '/resume', { body: {} }),
  tasks: (query) => request('GET', '/tasks', { query }),
  task: (id) => request('GET', '/tasks/' + enc(id)),
  taskByKey: (key) => request('GET', '/tasks/by-key?key=' + enc(key)),
  payload: (id) => request('GET', '/tasks/' + enc(id) + '/payload'),
  retryTask: (id, revision) => request('POST', '/tasks/' + enc(id) + '/retry', { body: { revision } }),
  cancelTask: (id, revision, reason) => request('POST', '/tasks/' + enc(id) + '/cancel', { body: { revision, reason } }),
  deleteTask: (id, revision) => request('DELETE', '/tasks/' + enc(id), { query: { revision } }),
  schedules: (query) => request('GET', '/schedules', { query }),
  upcomingSchedules: (limit) => request('GET', '/schedules/upcoming', { query: { limit } }),
  pauseSchedule: (id, revision) => request('POST', '/schedules/' + enc(id) + '/pause', { body: { revision } }),
  resumeSchedule: (id, revision) => request('POST', '/schedules/' + enc(id) + '/resume', { body: { revision } }),
  triggerSchedule: (id, revision) => request('POST', '/schedules/' + enc(id) + '/trigger', { body: { revision } }),
  deleteSchedule: (id, revision) => request('DELETE', '/schedules/' + enc(id), { query: { revision } }),
  runners: () => request('GET', '/runners'),
  unserved: () => request('GET', '/unserved'),
  issues: () => request('GET', '/issues'),
};
