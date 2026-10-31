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
  constructor(status, message) {
    super(message);
    this.status = status;
  }
  get conflict() {
    return this.status === 409;
  }
}

function url(path, query) {
  const base = (window.BTH4J_API || '').replace(/\/+$/, '') + '/api/v1';
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== '') q.set(k, String(v));
  }
  const qs = q.toString();
  return base + path + (qs ? '?' + qs : '');
}

async function request(method, path, { query, body, noRedirect } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (method !== 'GET' && session.csrf) headers['X-CSRF-Token'] = session.csrf;
  let res;
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
    if (res.status === 409) {
      throw new ApiError(409, 'This item changed meanwhile. Reload it and try again.');
    }
    const detail = data && (data.error || data.message);
    if (res.status === 401) throw new ApiError(401, detail || 'Wrong user or password');
    if (res.status === 403) throw new ApiError(403, detail || 'Not allowed for your role');
    throw new ApiError(res.status, detail || 'Request failed (HTTP ' + res.status + ')');
  }
  return data;
}

const enc = encodeURIComponent;

export const api = {
  login: (user, password) => request('POST', '/session', { body: { user, password }, noRedirect: true }),
  logout: () => request('DELETE', '/session', { noRedirect: true }),
  me: () => request('GET', '/me', { noRedirect: true }),
  health: () => request('GET', '/health'),
  overview: () => request('GET', '/overview'),
  queues: () => request('GET', '/queues'),
  pauseQueue: (name) => request('POST', '/queues/' + enc(name) + '/pause', { body: {} }),
  resumeQueue: (name) => request('POST', '/queues/' + enc(name) + '/resume', { body: {} }),
  tasks: (query) => request('GET', '/tasks', { query }),
  task: (id) => request('GET', '/tasks/' + enc(id)),
  payload: (id) => request('GET', '/tasks/' + enc(id) + '/payload'),
  retryTask: (id, revision) => request('POST', '/tasks/' + enc(id) + '/retry', { body: { revision } }),
  cancelTask: (id, revision, reason) => request('POST', '/tasks/' + enc(id) + '/cancel', { body: { revision, reason } }),
  deleteTask: (id, revision) => request('DELETE', '/tasks/' + enc(id), { query: { revision } }),
  schedules: () => request('GET', '/schedules'),
  pauseSchedule: (id) => request('POST', '/schedules/' + enc(id) + '/pause', { body: {} }),
  resumeSchedule: (id) => request('POST', '/schedules/' + enc(id) + '/resume', { body: {} }),
  triggerSchedule: (id) => request('POST', '/schedules/' + enc(id) + '/trigger', { body: {} }),
  deleteSchedule: (id) => request('DELETE', '/schedules/' + enc(id)),
  workers: () => request('GET', '/workers'),
  issues: () => request('GET', '/issues'),
};
