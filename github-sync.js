// 必填环境变量
// GITHUB_REPO          GitHub 仓库，如 owner/repo 或 `https://github.com/owner/repo`
// SYNC_MODE            同步模式：`local`（本地/Raw下载到目录）或 `CNB`（推送到 CNB 远程）
// 当 SYNC_MODE=`CNB` 时：
// CNB_REMOTE_URL       CNB 仓库地址（HTTPS），如 `https://cnb.cool/org/repo.git`
// 认证二选一：
// CNB_REMOTE_TOKEN     CNB 令牌（用户名固定为 `cnb`，密码为该 token）
// 或 CNB_REMOTE_USERNAME 远程用户名
// CNB_REMOTE_PASSWORD     远程密码
// 可选环境变量
// GITHUB_BRANCH        分支，默认 `main`
// GITHUB_TOKEN         GitHub 令牌（私仓或提高速率）
// LOCAL_SOURCE_DIR     本地源目录，仅在 `local` 模式生效
// DEST_DIR             目标目录，默认 `./scripts`，CNB模式生效
// SUBDIR               仅同步仓库/本地的指定子目录
// MAX_CONCURRENCY      最大并发数，默认 `4`
// DELETE_EXTRANEOUS    删除多余文件，默认 `0`
// DRY_RUN              干跑模式，不执行写入/推送，默认 `0`
// DEBUG                调试日志，默认 `0`
// TEMP_DIR             CNB 推送的临时镜像目录，默认 `./.cnb-sync-tmp`
// 运行时会在日志中以注释风格输出这些环境变量的当前值（敏感信息已脱敏）

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const cp = require('child_process');
const crypto = require('crypto');

function env(name, def) {
  const v = process.env[name];
  return v === undefined || v === '' ? def : v;
}

function pickAgent(u) {
  const proto = typeof u === 'string' ? (u.startsWith('https:') ? 'https:' : (u.startsWith('http:') ? 'http:' : '')) : u.protocol;
  return proto === 'https:' ? https : http;
}

const REPO = env('GITHUB_REPO', '');
const BRANCH = env('GITHUB_BRANCH', 'main');
const TOKEN = env('GITHUB_TOKEN', '');
const DEST_DIR = env('DEST_DIR', path.resolve(process.cwd(), 'scripts'));
const SUBDIR = env('SUBDIR', '');
const MAX_CONCURRENCY = parseInt(env('MAX_CONCURRENCY', '4'), 10);
const DRY_RUN = env('DRY_RUN', '0') === '1';
const DELETE_EXTRANEOUS = env('DELETE_EXTRANEOUS', '0') === '1';
const STATE_FILE = path.resolve(process.cwd(), '.github-sync.state.json');
const DEBUG = env('DEBUG', '0') === '1';
const SYNC_MODE = env('SYNC_MODE', 'local').toLowerCase();
const CNB_REMOTE_URL = env('CNB_REMOTE_URL', '');
const CNB_REMOTE_BRANCH = env('CNB_REMOTE_BRANCH', BRANCH);
const CNB_REMOTE_USERNAME = env('CNB_REMOTE_USERNAME', '');
const CNB_REMOTE_PASSWORD = env('CNB_REMOTE_PASSWORD', '');
const CNB_REMOTE_TOKEN = env('CNB_REMOTE_TOKEN', '');
const TEMP_DIR = env('TEMP_DIR', path.resolve(process.cwd(), '.cnb-sync-tmp'));
const LOCAL_SOURCE_DIR = env('LOCAL_SOURCE_DIR', '');

function parseRepo(s) {
  let t = String(s || '').trim();
  if (!t) return { owner: '', repo: '' };
  if (/^https?:\/\//i.test(t)) {
    try {
      const u = new URL(t);
      const comps = u.pathname.replace(/^\/+/, '').split('/');
      return { owner: comps[0] || '', repo: (comps[1] || '').replace(/\.git$/i, '') };
    } catch (_) {
      return { owner: '', repo: '' };
    }
  }
  const parts = t.split('/');
  return { owner: parts[0] || '', repo: (parts[1] || '').replace(/\.git$/i, '') };
}

function reqJson(url, method, headers) {
  return new Promise((resolve, reject) => {
    const agent = pickAgent(url);
    const h = Object.assign({}, headers || {});
    const req = agent.request({
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''),
      method,
      headers: h,
    }, res => {
      const status = res.statusCode || 0;
      const bufs = [];
      res.on('data', c => bufs.push(Buffer.from(c)));
      res.on('end', () => {
        const txt = Buffer.concat(bufs).toString('utf8');
        if (status >= 300 && status < 400 && res.headers.location) {
          const nextUrl = new URL(res.headers.location, url);
          reqJson(nextUrl, method, headers).then(resolve).catch(reject);
          return;
        }
        if (status !== 200) {
          reject(new Error('HTTP ' + status + ' ' + txt));
          return;
        }
        try {
          const obj = JSON.parse(txt);
          resolve(obj);
        } catch (e) {
          reject(new Error('JSON parse failed ' + e.message));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

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
        file.close(() => fs.unlink(tmp, () => downloadToFile(nextUrl, outPath, headers).then(resolve).catch(reject)));
        return;
      }
      if ((res.statusCode || 0) !== 200) {
        const bufs = [];
        res.on('data', c => bufs.push(Buffer.from(c)));
        res.on('end', () => {
          const txt = Buffer.concat(bufs).toString('utf8');
          file.close(() => fs.unlink(tmp, () => reject(new Error('HTTP ' + res.statusCode + ' ' + txt))));
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

function loadState() {
  try {
    const t = fs.readFileSync(STATE_FILE, 'utf8');
    return JSON.parse(t);
  } catch (_) {
    return { files: {} };
  }
}

function saveState(st) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(st));
}

function pickHeaders() {
  const h = { 'User-Agent': 'qinglong-github-sync' };
  if (TOKEN) h['Authorization'] = 'token ' + TOKEN;
  return h;
}

function withAuth(urlStr, username, password, token) {
  try {
    const u = new URL(urlStr);
    if (token) {
      u.username = token;
      u.password = '';
    } else {
      u.username = username || '';
      u.password = password || '';
    }
    return u.href;
  } catch (_) {
    return urlStr;
  }
}

function withAuthCnb(urlStr, username, password, token) {
  try {
    const u = new URL(urlStr);
    if (token) {
      u.username = 'cnb';
      u.password = token;
    } else {
      u.username = username || '';
      u.password = password || '';
    }
    return u.href;
  } catch (_) {
    return urlStr;
  }
}

function maskUrlAuth(urlStr) {
  try {
    const u = new URL(urlStr);
    if (u.password) u.password = '***';
    if (u.username) u.username = '***';
    return u.href;
  } catch (_) {
    return urlStr;
  }
}

function mask(val) {
  if (!val) return '';
  const s = String(val);
  if (s.length <= 4) return '***';
  return s.slice(0, 2) + '***' + s.slice(-2);
}

function logEnvSummary() {
  try {
    console.log('// ENV GITHUB_REPO =', REPO);
    console.log('// ENV GITHUB_BRANCH =', BRANCH);
    console.log('// ENV SYNC_MODE =', SYNC_MODE);
    console.log('// ENV DEST_DIR =', DEST_DIR);
    if (SUBDIR) console.log('// ENV SUBDIR =', SUBDIR);
    if (LOCAL_SOURCE_DIR) console.log('// ENV LOCAL_SOURCE_DIR =', LOCAL_SOURCE_DIR);
    if (CNB_REMOTE_URL) console.log('// ENV CNB_REMOTE_URL =', maskUrlAuth(CNB_REMOTE_URL));
    if (CNB_REMOTE_TOKEN) console.log('// ENV CNB_REMOTE_TOKEN =', mask(CNB_REMOTE_TOKEN));
    if (CNB_REMOTE_USERNAME) console.log('// ENV CNB_REMOTE_USERNAME =', CNB_REMOTE_USERNAME);
    if (CNB_REMOTE_PASSWORD) console.log('// ENV CNB_REMOTE_PASSWORD =', mask(CNB_REMOTE_PASSWORD));
    if (TOKEN) console.log('// ENV GITHUB_TOKEN =', mask(TOKEN));
    console.log('// ENV MAX_CONCURRENCY =', MAX_CONCURRENCY);
    console.log('// ENV DELETE_EXTRANEOUS =', DELETE_EXTRANEOUS ? '1' : '0');
    console.log('// ENV DRY_RUN =', DRY_RUN ? '1' : '0');
    console.log('// ENV DEBUG =', DEBUG ? '1' : '0');
  } catch (_) {}
}

function runCmd(cmd, args, cwd, extraEnv) {
  return new Promise((resolve, reject) => {
    const p = cp.spawn(cmd, args, { cwd, env: Object.assign({}, process.env, extraEnv || {}), stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    p.stdout.on('data', d => { out += d.toString(); });
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('close', code => {
      if (code === 0) resolve(out); else reject(new Error(cmd + ' ' + args.join(' ') + ' exit ' + code + ' ' + err));
    });
    p.on('error', reject);
  });
}

function rimraf(target) {
  try {
    if (!fs.existsSync(target)) return;
    const stat = fs.statSync(target);
    if (stat.isDirectory()) {
      const ents = fs.readdirSync(target);
      for (const e of ents) {
        rimraf(path.resolve(target, e));
      }
      fs.rmdirSync(target);
    } else {
      fs.unlinkSync(target);
    }
  } catch (_) {}
}

async function syncToCnbGit(owner, repo) {
  if (DRY_RUN) {
    try {
      console.log('mode', 'cnb');
      const gh = TOKEN ? withAuth('https://github.com/' + owner + '/' + repo + '.git', TOKEN, '', TOKEN) : ('https://github.com/' + owner + '/' + repo + '.git');
      const remote = withAuthCnb(CNB_REMOTE_URL, CNB_REMOTE_USERNAME, CNB_REMOTE_PASSWORD, CNB_REMOTE_TOKEN);
      console.log('plan', 'git clone --mirror', maskUrlAuth(gh));
      console.log('plan', 'git remote add cnb', maskUrlAuth(remote));
      console.log('plan', 'git push --mirror cnb');
    } catch (_) {}
    return;
  }
  if (!CNB_REMOTE_URL) throw new Error('CNB_REMOTE_URL required');
  rimraf(TEMP_DIR);
  fs.mkdirSync(TEMP_DIR, { recursive: true });
  let gh = 'https://github.com/' + owner + '/' + repo + '.git';
  if (TOKEN) gh = withAuth(gh, TOKEN, '', TOKEN);
  try { console.log('mode', 'cnb'); } catch (_) {}
  try { console.log('exec', 'git clone --mirror', maskUrlAuth(gh), '->', TEMP_DIR); } catch (_) {}
  await runCmd('git', ['clone', '--mirror', gh, TEMP_DIR], process.cwd());
  const remote = withAuthCnb(CNB_REMOTE_URL, CNB_REMOTE_USERNAME, CNB_REMOTE_PASSWORD, CNB_REMOTE_TOKEN);
  try { console.log('exec', 'git remote add cnb', maskUrlAuth(remote)); } catch (_) {}
  await runCmd('git', ['remote', 'add', 'cnb', remote], TEMP_DIR);
  try { console.log('exec', 'git ls-remote cnb'); } catch (_) {}
  try {
    const refs = await runCmd('git', ['ls-remote', 'cnb'], TEMP_DIR);
    const lines = String(refs || '').trim().split(/\r?\n/).filter(Boolean);
    console.log('remote refs', lines.length);
  } catch (e) {
    try { console.log('remote refs error', String(e && e.message ? e.message : e)); } catch (_) {}
  }
  try { console.log('exec', 'git push --mirror cnb'); } catch (_) {}
  await runCmd('git', ['push', 'cnb', '--all'], TEMP_DIR);  // 推送所有分支
  await runCmd('git', ['push', 'cnb', '--tags'], TEMP_DIR); // 推送所有标签
  try { console.log('done', 'push mirror'); } catch (_) {}
  rimraf(TEMP_DIR);
}

async function listTree(owner, repo, ref) {
  const url = new URL('/repos/' + owner + '/' + repo + '/git/trees/' + encodeURIComponent(ref) + '?recursive=1', 'https://api.github.com');
  const data = await reqJson(url, 'GET', pickHeaders());
  if (!data || !data.tree || !Array.isArray(data.tree)) throw new Error('GitHub tree failed');
  return data.tree.filter(x => x && x.type === 'blob').map(x => ({ path: x.path, sha: x.sha, size: x.size || 0 }));
}

function relFromSubdir(p, subdir) {
  if (!subdir) return p;
  const a = subdir.replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
  const b = p.replace(/\\/g, '/');
  if (!a) return p;
  if (!b.startsWith(a + '/') && b !== a) return null;
  const r = b.slice(a.length).replace(/^\//, '');
  return r;
}

function shouldSkipByState(st, rel, sha) {
  const prev = st.files[rel];
  return prev && prev === sha;
}

function sha1File(filePath) {
  return new Promise((resolve, reject) => {
    try {
      const h = crypto.createHash('sha1');
      const s = fs.createReadStream(filePath);
      s.on('data', d => h.update(d));
      s.on('end', () => resolve(h.digest('hex')));
      s.on('error', reject);
    } catch (e) {
      reject(e);
    }
  });
}

function collectLocalBlobs(srcDir) {
  const res = [];
  const base = path.resolve(srcDir);
  const stack = [base];
  while (stack.length) {
    const cur = stack.pop();
    const ents = fs.readdirSync(cur, { withFileTypes: true });
    for (const ent of ents) {
      const full = path.resolve(cur, ent.name);
      if (ent.isDirectory()) {
        stack.push(full);
        continue;
      }
      const rel = full.slice(base.length + 1).replace(/\\/g, '/');
      res.push({ full, rel, size: fs.statSync(full).size });
    }
  }
  return res;
}

async function run() {
  const { owner, repo } = parseRepo(REPO);
  if (!owner || !repo) throw new Error('GITHUB_REPO required');
  logEnvSummary();
  if (SYNC_MODE === 'cnb') {
    await syncToCnbGit(owner, repo);
    return;
  }
  if (LOCAL_SOURCE_DIR) {
    fs.mkdirSync(DEST_DIR, { recursive: true });
    const state = loadState();
    const blobs = collectLocalBlobs(LOCAL_SOURCE_DIR);
    const targets = blobs.map(b => ({ rel: relFromSubdir(b.rel, SUBDIR) ?? b.rel, src: b.full, out: path.resolve(DEST_DIR, relFromSubdir(b.rel, SUBDIR) ?? b.rel), size: b.size }));
    let active = 0;
    let idx = 0;
    const errors = [];
    const next = () => {
      if (idx >= targets.length && active === 0) return Promise.resolve();
      while (active < MAX_CONCURRENCY && idx < targets.length) {
        const t = targets[idx++];
        if (t.rel === null) continue;
        active++;
        const p = (async () => {
          const sha = await sha1File(t.src);
          if (shouldSkipByState(state, t.rel, sha)) return;
          if (!DRY_RUN) {
            const dir = path.dirname(t.out);
            fs.mkdirSync(dir, { recursive: true });
            fs.copyFileSync(t.src, t.out);
          }
          state.files[t.rel] = sha;
        })();
        p.then(() => { active--; next(); }).catch(e => { errors.push(String(e)); active--; next(); });
      }
      return new Promise(resolve => {
        const tid = setInterval(() => {
          if (idx >= targets.length && active === 0) {
            clearInterval(tid);
            resolve();
          }
        }, 100);
      });
    };
    await next();
    if (DELETE_EXTRANEOUS && !DRY_RUN) {
      const keep = new Set(targets.map(t => t.rel));
      const baseLen = DEST_DIR.length;
      const stack = [DEST_DIR];
      while (stack.length) {
        const cur = stack.pop();
        const ents = fs.readdirSync(cur, { withFileTypes: true });
        for (const ent of ents) {
          const full = path.resolve(cur, ent.name);
          const rel = full.slice(baseLen + 1).replace(/\\/g, '/');
          if (ent.isDirectory()) {
            stack.push(full);
            continue;
          }
          if (!keep.has(rel)) {
            try { fs.unlinkSync(full); } catch (_) {}
          }
        }
      }
    }
    saveState(state);
    if (DEBUG) {
      try { console.log('synced', targets.length); } catch (_) {}
    }
    if (DRY_RUN) {
      try { console.log('dry-run copy from', LOCAL_SOURCE_DIR, 'to', DEST_DIR); } catch (_) {}
    }
    return;
  }
  fs.mkdirSync(DEST_DIR, { recursive: true });
  const state = loadState();
  const blobs = await listTree(owner, repo, BRANCH);
  const targets = [];
  for (const b of blobs) {
    const rel = relFromSubdir(b.path, SUBDIR);
    if (rel === null) continue;
    const out = path.resolve(DEST_DIR, rel);
    targets.push({ rel, out, sha: b.sha, size: b.size });
  }
  let active = 0;
  let idx = 0;
  const errors = [];
  const next = () => {
    if (idx >= targets.length && active === 0) return Promise.resolve();
    while (active < MAX_CONCURRENCY && idx < targets.length) {
      const t = targets[idx++];
      active++;
      const p = (async () => {
        if (shouldSkipByState(state, t.rel, t.sha)) return;
        if (DRY_RUN) return;
        const raw = new URL('/' + owner + '/' + repo + '/' + encodeURIComponent(BRANCH) + '/' + t.rel.replace(/\\/g, '/'), 'https://raw.githubusercontent.com');
        const headers = pickHeaders();
        await downloadToFile(raw.href, t.out, headers);
        state.files[t.rel] = t.sha;
      })();
      p.then(() => { active--; next(); }).catch(e => { errors.push(String(e)); active--; next(); });
    }
    return new Promise(resolve => {
      const tid = setInterval(() => {
        if (idx >= targets.length && active === 0) {
          clearInterval(tid);
          resolve();
        }
      }, 100);
    });
  };
  await next();
  if (DELETE_EXTRANEOUS && !DRY_RUN) {
    const keep = new Set(targets.map(t => t.rel));
    const baseLen = DEST_DIR.length;
    const stack = [DEST_DIR];
    while (stack.length) {
      const cur = stack.pop();
      const ents = fs.readdirSync(cur, { withFileTypes: true });
      for (const ent of ents) {
        const full = path.resolve(cur, ent.name);
        const rel = full.slice(baseLen + 1).replace(/\\/g, '/');
        if (ent.isDirectory()) {
          stack.push(full);
          continue;
        }
        if (!keep.has(rel)) {
          try { fs.unlinkSync(full); } catch (_) {}
        }
      }
    }
  }
  saveState(state);
  if (errors.length) throw new Error(errors.join('\n'));
  if (DEBUG) {
    try { console.log('synced', targets.length); } catch (_) {}
  }
}

run().catch(e => {
  console.error(e && e.stack ? e.stack : String(e));
  process.exit(1);
});
