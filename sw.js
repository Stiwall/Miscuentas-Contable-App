const CACHE = 'miscuentas-v1';
const ASSETS = [
  '/',
  '/contabilidad.html',
  '/manifest.json',
  '/icon-192.png',
  '/icon-256.png',
  '/icon-512.png'
];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(k => Promise.all(k.filter(v => v !== CACHE).map(v => caches.delete(v)))).then(() => self.clients.claim()));
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  e.respondWith(
    fetch(e.request).then(r => { if (r.status === 200) { const c = caches.open(CACHE); c.then(x => x.put(e.request, r.clone())); } return r.clone(); }).catch(() => caches.match(e.request).then(x => x || fetch(e.request)))
  );
});
