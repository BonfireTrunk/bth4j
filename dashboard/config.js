// Set BTH4J_API when the dashboard is served from a different origin than the admin API, e.g. "https://host:8787".
// It lives in its own file because the server's Content-Security-Policy forbids inline scripts.
window.BTH4J_API = window.BTH4J_API || '';
