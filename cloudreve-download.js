// 必填环境变量
// CLOUDREVE_BASE      Cloudreve 站点基础地址，如 https://data.cloudreve.org
// CLOUDREVE_EMAIL     登录邮箱账号
// CLOUDREVE_PASSWORD  登录密码
// CLOUDREVE_URI       需要下载的根目录 URI，如 cloudreve://my/download
// 可选环境变量
// DOWNLOAD_DIR        下载目录，默认 ./downloads
// PAGE_SIZE           每页文件数量，默认 200
// PREFER_DOWNLOAD     是否优先下载文件，默认 1
// ONLY_NEW            只下载新增文件，默认 1
// MAX_CONCURRENCY     最大并发下载数，默认 3
// DRY_RUN             dry run 模式，不实际下载文件，默认 0
// DEBUG               开启 debug 模式，默认 0

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

/**
 * 从环境变量读取配置值，若为空则使用默认值
 * @param {string} name 环境变量名
 * @param {string} def 默认值
 * @returns {string} 环境变量或默认值
 */
function env(name, def) {
  const v = process.env[name];
  return v === undefined || v === '' ? def : v;
}

const BASE = env('CLOUDREVE_BASE', '');
const EMAIL = env('CLOUDREVE_EMAIL', '');
const PASSWORD = env('CLOUDREVE_PASSWORD', '');
const ROOT_URI = env('CLOUDREVE_URI', '');
const PAGE_SIZE = parseInt(env('PAGE_SIZE', '200'), 10);
const DOWNLOAD_DIR = env('DOWNLOAD_DIR', path.resolve(process.cwd(), 'downloads'));
const PREFER_DOWNLOAD = env('PREFER_DOWNLOAD', '1') === '1';
const ONLY_NEW = env('ONLY_NEW', '1') === '1';
const MAX_CONCURRENCY = parseInt(env('MAX_CONCURRENCY', '3'), 10);
const DRY_RUN = env('DRY_RUN', '0') === '1';
const STATE_FILE = path.resolve(process.cwd(), '.cloudreve.state.json');
const DEBUG = env('DEBUG', '0') === '1';

function assertConfig() {
  if (!BASE || !EMAIL || !PASSWORD || !ROOT_URI) {
    throw new Error('Missing env: CLOUDREVE_BASE, CLOUDREVE_EMAIL, CLOUDREVE_PASSWORD, CLOUDREVE_URI');
  }
}

// 根据 URL 协议选择 http 或 https 客户端
function pickAgent(url) {
  return url.protocol === 'http:' ? http : https;
}

/**
 * 发起 JSON 请求并解析响应
 * 支持 3xx 跳转；非 200 状态码抛错；响应体解析失败抛错
 * @param {URL} url 请求地址
 * @param {string} method HTTP 方法
 * @param {Object} headers 请求头
 * @param {Object} body JSON 请求体
 * @returns {Promise<any>} 解析后的 JSON 对象
 */
function reqJson(url, method, headers, body) {
  return new Promise((resolve, reject) => {
    const data = body ? Buffer.from(JSON.stringify(body)) : null;
    const h = Object.assign({}, headers || {});
    if (data) {
      h['Content-Type'] = 'application/json';
      h['Content-Length'] = String(data.length);
    }
    const agent = pickAgent(url);
    const req = agent.request({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''),
      method,
      headers: h,
    }, res => {
      const code = res.statusCode || 0;
      if (code >= 300 && code < 400 && res.headers.location) {
        const nextUrl = new URL(res.headers.location, url);
        resolve(reqJson(nextUrl, method, headers, body));
        return;
      }
      const bufs = [];
      res.on('data', c => bufs.push(Buffer.from(c)));
      res.on('end', () => {
        const txt = Buffer.concat(bufs).toString('utf8');
        if (code !== 200) {
          reject(new Error('HTTP ' + code + ' ' + txt));
          return;
        }
        try {
          const obj = JSON.parse(txt);
          resolve(obj);
        } catch (e) {
          reject(new Error('JSON parse failed ' + e.message + ' ' + txt));
        }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

/**
 * 以流的方式下载文件到指定路径（使用临时文件 .part，成功后重命名）
 * 支持 3xx 跳转；遇到 JSON 响应或非 200 状态码时解析并抛错
 * @param {string} urlStr 直链地址
 * @param {string} outPath 输出文件路径
 * @param {Object} headers 额外请求头
 * @returns {Promise<string>} 最终文件路径
 */
function downloadToFile(urlStr, outPath, headers) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const agent = pickAgent(url);
    const dir = path.dirname(outPath);
    fs.mkdirSync(dir, { recursive: true });
    const tmp = outPath + '.part';
    const file = fs.createWriteStream(tmp);
    const req = agent.request({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''),
      method: 'GET',
      headers: headers || {},
    }, res => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const nextUrl = new URL(res.headers.location, url).href;
        file.close(() => fs.unlink(tmp, () => resolve(downloadToFile(nextUrl, outPath, headers))));
        return;
      }
      const ct = (res.headers['content-type'] || '').toLowerCase();
      if ((res.statusCode || 0) !== 200 || ct.includes('application/json')) {
        const bufs = [];
        res.on('data', c => bufs.push(Buffer.from(c)));
        res.on('end', () => {
          const txt = Buffer.concat(bufs).toString('utf8');
          file.close(() => fs.unlink(tmp, () => {
            try {
              const obj = JSON.parse(txt);
              if (typeof obj === 'object' && obj && obj.code && obj.code !== 0) {
                reject(new Error('API ' + obj.code + ' ' + (obj.msg || '')));
                return;
              }
            } catch (_) {}
            reject(new Error('HTTP ' + res.statusCode + ' ' + txt));
          }));
        });
        return;
      }
      res.pipe(file);
      file.on('finish', () => {
        file.close(() => {
          fs.rename(tmp, outPath, err => {
            if (err) reject(err); else resolve(outPath);
          });
        });
      });
    });
    req.on('error', err => {
      file.close(() => fs.unlink(tmp, () => reject(err)));
    });
    req.end();
  });
}

async function ping(base) {
  const url = new URL('/api/v4/site/ping', base);
  const r = await reqJson(url, 'GET');
  return r;
}

/**
 * 登录获取访问/刷新令牌
 * @param {string} base Cloudreve 基础地址
 * @param {string} email 账号邮箱
 * @param {string} password 密码
 * @returns {{access_token:string,refresh_token:string,access_expires:string,refresh_expires:string}}
 */
async function login(base, email, password) {
  const url = new URL('/api/v4/session/token', base);
  const r = await reqJson(url, 'POST', {}, { email, password });
  if (!r || r.code !== 0 || !r.data || !r.data.token) throw new Error('Login failed');
  const t = r.data.token;
  return {
    access_token: t.access_token,
    refresh_token: t.refresh_token,
    access_expires: t.access_expires,
    refresh_expires: t.refresh_expires,
  };
}

function parseJwtExp(t) {
  try {
    const parts = String(t).split('.');
    if (parts.length < 2) return 0;
    const payload = JSON.parse(Buffer.from(parts[1].replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8'));
    const e = Number(payload && payload.exp ? payload.exp : 0);
    return isNaN(e) ? 0 : e * 1000;
  } catch (_) {
    return 0;
  }
}

/**
 * 判断访问令牌是否仍有效（优先使用返回的过期时间，回退解析 JWT exp）
 * @param {{access_token:string,access_expires?:string}} auth
 * @returns {boolean}
 */
function isAccessValid(auth) {
  if (!auth || !auth.access_token) return false;
  if (auth.access_expires) {
    const e = Date.parse(auth.access_expires);
    if (!isNaN(e)) return Date.now() + 60000 < e;
  }
  const e2 = parseJwtExp(auth.access_token);
  if (e2) return Date.now() + 60000 < e2;
  return true;
}

async function ensureAuth(state) {
  let auth = state.auth || null;
  if (!isAccessValid(auth)) {
    auth = await login(BASE, EMAIL, PASSWORD);
    state.auth = auth;
    saveState(state);
  }
  return auth;
}

/**
 * 列出目录内容
 * @param {string} base 基础地址
 * @param {string} token 访问令牌
 * @param {string} uri 目录 URI（cloudreve://...）
 * @param {number} pageSize 每页数量
 * @returns {Promise<{files:any[],context_hint?:string}>}
 */
async function listDir(base, token, uri, pageSize) {
  const url = new URL('/api/v4/file', base);
  url.searchParams.set('uri', uri);
  url.searchParams.set('page_size', String(pageSize));
  const r = await reqJson(url, 'GET', { Authorization: 'Bearer ' + token });
  if (!r || r.code !== 0 || !r.data) throw new Error('List failed');
  return r.data;
}

function isFolder(item) {
  return item && item.type === 1;
}

function isFile(item) {
  return item && item.type !== 1;
}

// 将 cloudreve 路径映射为相对文件系统路径（用于输出目录结构）
function relPathFrom(rootUri, filePath) {
  const a = rootUri.replace(/\\/g, '/');
  const b = filePath.replace(/\\/g, '/');
  if (!b.startsWith(a)) return filePath;
  const seg = b.slice(a.length);
  const p = seg.replace(/^\//, '');
  const comps = p.split('/').filter(Boolean);
  return comps.join(path.sep);
}

/**
 * 深度遍历收集根目录下所有文件（文件夹入栈继续遍历）
 * @param {string} base 基础地址
 * @param {string} token 访问令牌
 * @param {string} rootUri 根目录 URI
 * @returns {Promise<Array<{item:any,contextHint:string|null}>>}
 */
async function collectFiles(base, token, rootUri) {
  const stack = [{ uri: rootUri, contextHint: null }];
  const files = [];
  while (stack.length) {
    const cur = stack.pop();
    const data = await listDir(base, token, cur.uri, PAGE_SIZE);
    const ctx = data.context_hint || null;
    for (const item of data.files || []) {
      if (isFolder(item)) stack.push({ uri: item.path, contextHint: ctx }); else files.push({ item, contextHint: ctx });
    }
  }
  return files;
}

/**
 * 调用直链接口，输入 body 兼容多种字段形式
 * @param {string} base 基础地址
 * @param {string} token 访问令牌
 * @param {Object} body 请求体（兼容 uri/uris/files/items 等）
 */
async function tryFileUrl(base, token, body) {
  const url = new URL('/api/v4/file/url', base);
  const r = await reqJson(url, 'POST', { Authorization: 'Bearer ' + token }, body);
  return r;
}

function pickUrlFromData(data) {
  if (!data) return null;
  if (typeof data === 'string') return data;
  if (Array.isArray(data)) {
    const x = data[0];
    if (!x) return null;
    if (typeof x === 'string') return x;
    const u = pickUrlFromObject(x);
    if (u) return u;
    return null;
  }
  if (typeof data === 'object') {
    const u = pickUrlFromObject(data);
    if (u) return u;
    const any = findHttpUrl(data);
    if (any) return any;
  }
  return null;
}

function decodeTimeFlowStringTime(s, timeNowMs) {
  if (!s) return '';
  const timeNowS = Math.floor(timeNowMs / 1000);
  const timeNowBackup = timeNowS;
  const timeDigits = [];
  if (timeNowS === 0) {
    timeDigits.push(0);
  } else {
    let tempTime = timeNowS;
    while (tempTime > 0) {
      timeDigits.push(tempTime % 10);
      tempTime = Math.floor(tempTime / 10);
    }
  }
  const res = new Array(s.length);
  let secret = Array.from(s);
  let add = secret.length % 2 === 0;
  let timeDigitIndex = (secret.length - 1) % timeDigits.length;
  const originalLen = secret.length;
  for (let pos = 0; pos < originalLen; pos++) {
    const targetIndex = originalLen - 1 - pos;
    let newIndex = targetIndex;
    if (add) {
      newIndex += timeDigits[timeDigitIndex] * timeDigitIndex;
    } else {
      newIndex = 2 * timeDigitIndex * timeDigits[timeDigitIndex] - newIndex;
    }
    if (newIndex < 0) newIndex = -newIndex;
    newIndex %= secret.length;
    res[targetIndex] = secret[newIndex];
    const lastIndex = secret.length - 1;
    if (newIndex !== lastIndex) {
      const a = secret[newIndex];
      secret[newIndex] = secret[lastIndex];
      secret[lastIndex] = a;
    }
    secret.pop();
    add = !add;
    timeDigitIndex -= 1;
    if (timeDigitIndex < 0) timeDigitIndex = timeDigits.length - 1;
  }
  const resStr = res.join('');
  const parts = resStr.split('|', 2);
  if (parts.length < 2 || parts[0] !== String(timeNowBackup)) {
    throw new Error('timestamp mismatch');
  }
  return parts[1];
}

function tryDecodeObfuscated(s) {
  const now = Date.now();
  for (let offset = -5; offset <= 5; offset++) {
    const t = now + offset * 1000;
    try {
      const raw = decodeTimeFlowStringTime(s, t);
      const dec = decodeURIComponent(raw);
      if (dec) return dec;
    } catch (_) {}
  }
  return null;
}

function pickUrlFromObject(obj) {
  if (!obj || typeof obj !== 'object') return null;
  if (obj.url) return sanitizeUrl(obj.url);
  if (obj.direct_url) return sanitizeUrl(obj.direct_url);
  if (obj.download_url) return sanitizeUrl(obj.download_url);
  if (obj.link) return sanitizeUrl(obj.link);
  if (obj.obfuscated) {
    const d = tryDecodeObfuscated(obj.obfuscated);
    if (d) return sanitizeUrl(d);
  }
  if (Array.isArray(obj.urls) && obj.urls.length) {
    const u = pickUrlFromObject(obj.urls[0]);
    if (u) return u;
  }
  const any = findHttpUrl(obj);
  if (any) return sanitizeUrl(any);
  return null;
}

function findHttpUrl(val) {
  if (!val) return null;
  if (typeof val === 'string') {
    const s = String(val).trim();
    if (/^https?:\/\//i.test(s)) return s;
    const m = s.match(/https?:\/\/[^\s"']+/i);
    return m ? m[0] : null;
  }
  if (Array.isArray(val)) {
    for (const x of val) {
      const r = findHttpUrl(x);
      if (r) return r;
    }
    return null;
  }
  if (typeof val === 'object') {
    const keys = Object.keys(val);
    for (const k of keys) {
      const r = findHttpUrl(val[k]);
      if (r) return r;
    }
  }
  return null;
}

// 规范化 URL 字符串：去空白与包裹引号，提取首个 http(s) 链接
function sanitizeUrl(u) {
  if (!u) return null;
  let s = String(u);
  s = s.trim();
  s = s.replace(/^`+|`+$/g, '');
  s = s.replace(/^"+|"+$/g, '');
  s = s.replace(/^'+|'+$/g, '');
  if (!/^https?:\/\//i.test(s)) {
    const found = findHttpUrl(s);
    if (found) s = found;
  }
  return s;
}

/**
 * 当直链主机为本地地址（localhost/127.0.0.1/::1/0.0.0.0）时，
 * 重写为 CLOUDREVE_BASE 对应的主机与协议，保证外网可访问
 * @param {string|null} u 原始直链
 * @param {string} base 站点基础地址
 * @returns {string|null} 重写后的直链
 */
function rewriteUrlHost(u, base) {
  if (!u || !base) return u;
  try {
    const src = new URL(sanitizeUrl(u));
    const b = new URL(base);
    const h = src.hostname.toLowerCase();
    if (h !== 'localhost' && h !== '127.0.0.1' && h !== '::1' && h !== '0.0.0.0') return src.href;
    const proto = b.protocol || src.protocol;
    const host = b.hostname;
    const port = b.port ? ':' + b.port : '';
    return proto + '//' + host + port + src.pathname + (src.search || '');
  } catch (_) {
    return sanitizeUrl(u);
  }
}

/**
 * 获取文件直链：尝试多种 body 结构，以兼容不同后端版本
 * 若 DEBUG 开启，打印请求与响应用于排查
 * @param {string} base 基础地址
 * @param {string} token 访问令牌
 * @param {any} fileItem 文件项
 * @param {string|null} contextHint 上下文提示（由列表接口返回）
 * @returns {Promise<string|null>} 直链
 */
async function getDirectUrl(base, token, fileItem, contextHint) {
  const uri = fileItem.path;
  const id = fileItem.id;
  const entity = fileItem.primary_entity;
  const capability = fileItem.capability;
  const bodies = [
    { uris: [uri], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { files: [{ uri, id, entity, capability }], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { uri: [uri], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { uri, prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { items: [{ uri, id, entity, capability }], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { files: [{ uri }], download: PREFER_DOWNLOAD, enforce_download: PREFER_DOWNLOAD, prefer_preview: !PREFER_DOWNLOAD, context_hint: contextHint },
    { uri, download: PREFER_DOWNLOAD, enforce_download: PREFER_DOWNLOAD, prefer_preview: !PREFER_DOWNLOAD, context_hint: contextHint },
    { uris: [uri], download: PREFER_DOWNLOAD, enforce_download: PREFER_DOWNLOAD, prefer_preview: !PREFER_DOWNLOAD, context_hint: contextHint },
    { files: [{ id }], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { items: [{ id }], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { id: [id], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
    { files: [{ entity }], prefer_download: PREFER_DOWNLOAD, context_hint: contextHint },
  ];
  for (const b of bodies) {
    try {
      if (DEBUG) {
        try { console.log('file/url body', JSON.stringify(b)); } catch (_) {}
      }
      const resp = await tryFileUrl(base, token, b);
      let u = pickUrlFromData(resp && resp.data);
      u = rewriteUrlHost(u, base);
      if (DEBUG && resp) {
        try { console.log('file/url returned', JSON.stringify(resp)); } catch (_) {}
      }
      if (u) return u;
    } catch (e) {
      if (DEBUG) {
        try { console.log('file/url error', String(e && e.message ? e.message : e)); } catch (_) {}
      }
    }
  }
  return null;
}

function loadState() {
  try {
    const t = fs.readFileSync(STATE_FILE, 'utf8');
    return JSON.parse(t);
  } catch (_) {
    return { downloaded: {} };
  }
}

function saveState(st) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(st));
}

/**
 * 主流程：
 * 1) 配置校验与目录准备
 * 2) 基础地址 http->https 兼容性测试
 * 3) 登录并遍历收集文件列表
 * 4) 并发下载文件，处理直链过期与主机重写
 */
async function run() {
  assertConfig();
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
  let useBase = BASE;
  if (/^http:/i.test(BASE)) {
    try {
      const httpsBase = 'https' + BASE.slice(4);
      const test = await ping(httpsBase);
      if (test && test.code === 0) useBase = httpsBase;
    } catch (_) {}
  }
  const pingInfo = await ping(useBase);
  const state = loadState();
  const auth = await ensureAuth(state);
  let token = auth.access_token;
  const files = await collectFiles(useBase, token, ROOT_URI);
  if (DEBUG) {
    try { console.log('ping', JSON.stringify(pingInfo)); } catch (_) {}
    try { console.log('file count', files.length); } catch (_) {}
  }
  const stateRef = state;
  let active = 0;
  let idx = 0;
  const errors = [];
  const next = () => {
    if (idx >= files.length && active === 0) return Promise.resolve();
    while (active < MAX_CONCURRENCY && idx < files.length) {
      const f = files[idx++];
      active++;
      const rel = relPathFrom(ROOT_URI, f.item.path);
      const out = path.resolve(DOWNLOAD_DIR, rel);
      const key = f.item.id + ':' + (f.item.updated_at || '');
      const exists = fs.existsSync(out);
      const same = exists && ONLY_NEW ? (fs.statSync(out).size === (f.item.size || 0)) : false;
      const skip = ONLY_NEW && exists && same && state.downloaded[key];
      const p = (async () => {
        if (skip) return;
        let url = await getDirectUrl(useBase, token, f.item, f.contextHint);
        url = sanitizeUrl(url);
        url = rewriteUrlHost(url, useBase);
        if (!url) throw new Error('No URL for ' + f.item.path);
        if (DRY_RUN) return;
        let headers = {};
        try {
          const b = new URL(useBase);
          const u = new URL(url);
          if (b.hostname === u.hostname) headers = { Authorization: 'Bearer ' + token };
        } catch (_) {}
        if (DEBUG) {
          try { console.log('downloading', f.item.path, '->', out, 'url', url); } catch (_) {}
        }
        try {
          await downloadToFile(url, out, headers);
        } catch (e) {
          const msg = String(e && e.message ? e.message : e);
          const expired = msg.includes('signature expired') || msg.includes('API 40005');
          if (expired) {
            let fresh = await getDirectUrl(useBase, token, f.item, f.contextHint);
            fresh = sanitizeUrl(fresh);
            fresh = rewriteUrlHost(fresh, useBase);
            if (!fresh) throw e;
            await downloadToFile(fresh, out, headers);
          } else {
            throw e;
          }
        }
        stateRef.downloaded[key] = true;
      })();
      p.then(() => { active--; next(); }).catch(e => { errors.push(String(e)); active--; next(); });
    }
    return new Promise(resolve => {
      const tid = setInterval(() => {
        if (idx >= files.length && active === 0) {
          clearInterval(tid);
          resolve();
        }
      }, 100);
    });
  };
  await next();
  saveState(stateRef);
  if (errors.length) {
    if (DEBUG) {
      try { console.error('errors', JSON.stringify(errors)); } catch (_) {}
    }
    throw new Error(errors.join('\n'));
  }
}

run().catch(e => {
  console.error(e && e.stack ? e.stack : String(e));
  process.exit(1);
});
