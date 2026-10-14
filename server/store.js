import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { execSync } = require("child_process");

const DEFAULT_BASE = "https://ilinkai.weixin.qq.com";
const MAX_LOCAL_TOKENS = 10;
const LOCK_STALE_MS = 30_000;
const LOCK_WAIT_MS = 60_000;

export function homeDir() {
  const raw = process.env.GROK_WECHAT_HOME?.trim();
  if (raw) return path.resolve(raw);
  return path.join(os.homedir(), ".grok-wechat");
}

export function paths() {
  const home = homeDir();
  return {
    home,
    state: path.join(home, "account.json"),
    inbox: path.join(home, "inbox.jsonl"),
    inboxPending: path.join(home, "inbox-pending.jsonl"),
    inboxSeen: path.join(home, "inbox-seen.json"),
    wakeQueue: path.join(home, "wake-queue.jsonl"),
    wakeMeta: path.join(home, "wake-meta.json"),
    pid: path.join(home, "monitor.pid"),
    media: path.join(home, "media"),
    log: path.join(home, "monitor.log"),
    lockDir: path.join(home, ".locks"),
  };
}

export function emptyAccount() {
  return {
    token: "",
    baseUrl: DEFAULT_BASE,
    ilinkBotId: "",
    ilinkUserId: "",
    getUpdatesBuf: "",
    contextTokens: {},
    typingTickets: {},
    wake: null,
    dedicatedAssistant: null,
    sessionExpired: false,
    sessionExpiredAt: 0,
  };
}

export function emptyState() {
  return {
    accounts: [],
    pendingQrs: [],
    allowFrom: [],
    seenPeers: [],
  };
}

function sleepSync(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* busy wait for short lock contention */
  }
}

function tryChmod(p, mode) {
  try {
    if (fs.existsSync(p)) fs.chmodSync(p, mode);
  } catch {
    // ignore
  }
}

export function ensureHome() {
  const { home, media, lockDir } = paths();
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  tryChmod(home, 0o700);
  fs.mkdirSync(media, { recursive: true, mode: 0o700 });
  tryChmod(media, 0o700);
  fs.mkdirSync(lockDir, { recursive: true, mode: 0o700 });
}

function migratePermissions() {
  const p = paths();
  tryChmod(p.home, 0o700);
  tryChmod(p.state, 0o600);
  tryChmod(fallbackWakePath(), 0o600);
  tryChmod(p.inbox, 0o600);
  tryChmod(p.inboxPending, 0o600);
  tryChmod(p.wakeQueue, 0o600);
  tryChmod(p.wakeMeta, 0o600);
  tryChmod(p.inboxSeen, 0o600);
  tryChmod(p.log, 0o600);
  tryChmod(p.pid, 0o600);
  tryChmod(p.media, 0o700);
}

export function isPidAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function lockPath(name) {
  return path.join(paths().lockDir, `${name}.lock`);
}

function acquireLock(name, timeoutMs = LOCK_WAIT_MS) {
  ensureHome();
  const lp = lockPath(name);
  const start = Date.now();
  while (true) {
    try {
      const fd = fs.openSync(lp, "wx");
      try {
        fs.writeFileSync(fd, `${process.pid}\n${Date.now()}\n`);
      } finally {
        fs.closeSync(fd);
      }
      tryChmod(lp, 0o600);
      return lp;
    } catch (err) {
      if (err?.code !== "EEXIST") throw err;
      try {
        const raw = fs.readFileSync(lp, "utf8");
        const [pidLine, tsLine] = raw.split("\n");
        const oldPid = Number(pidLine);
        const ts = Number(tsLine) || 0;
        const stale = (ts && Date.now() - ts > LOCK_STALE_MS) || (oldPid && !isPidAlive(oldPid));
        if (stale) {
          try {
            fs.unlinkSync(lp);
            continue;
          } catch {
            // ignore
          }
        }
      } catch {
        try {
          fs.unlinkSync(lp);
          continue;
        } catch {
          // ignore
        }
      }
      if (Date.now() - start > timeoutMs) throw new Error(`lock timeout: ${name}`);
      sleepSync(25);
    }
  }
}

function releaseLock(lp) {
  try {
    if (lp && fs.existsSync(lp)) fs.unlinkSync(lp);
  } catch {
    // ignore
  }
}

export function withLock(name, fn) {
  const lp = acquireLock(name);
  try {
    return fn();
  } finally {
    releaseLock(lp);
  }
}

export async function withLockAsync(name, fn) {
  const lp = acquireLock(name);
  try {
    return await fn();
  } finally {
    releaseLock(lp);
  }
}

function migrateLegacyState(raw) {
  if (Array.isArray(raw.accounts)) return raw;
  const state = emptyState();
  if (raw.token) {
    state.accounts.push({
      ...emptyAccount(),
      token: raw.token,
      baseUrl: raw.baseUrl || DEFAULT_BASE,
      ilinkBotId: raw.ilinkBotId || "",
      ilinkUserId: raw.ilinkUserId || "",
      getUpdatesBuf: raw.getUpdatesBuf || "",
      contextTokens: raw.contextTokens || {},
      typingTickets: raw.typingTickets || {},
    });
  }
  if (raw.pendingQr?.qrcode) {
    state.pendingQrs.push({ qrcode: raw.pendingQr.qrcode, at: raw.pendingQr.at || Date.now() });
  }
  state.allowFrom = raw.allowFrom || [];
  state.seenPeers = raw.seenPeers || [];
  return state;
}

function stripAccountWakeKeys(state) {
  for (const a of state.accounts || []) {
    if (a?.wake) a.wake = null;
  }
  return state;
}

export function loadState() {
  ensureHome();
  migratePermissions();
  const { state } = paths();
  if (!fs.existsSync(state)) return emptyState();
  try {
    const raw = JSON.parse(fs.readFileSync(state, "utf8"));
    const merged = { ...emptyState(), ...migrateLegacyState(raw) };
    return stripAccountWakeKeys(merged);
  } catch {
    return emptyState();
  }
}

export function saveState(next) {
  ensureHome();
  const cleaned = stripAccountWakeKeys(JSON.parse(JSON.stringify(next)));
  const { state } = paths();
  const tmp = `${state}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(cleaned, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, state);
  tryChmod(state, 0o600);
}

export function saveStateSecure(next) {
  saveState(next);
}

export function updateState(mutator) {
  return withLock("state", () => {
    const current = loadState();
    const next = mutator(current) ?? current;
    saveState(next);
    return next;
  });
}

export function listAccounts(state = loadState()) {
  return state.accounts.filter((a) => a.token);
}

export function findAccount(state, { ilink_bot_id, ilink_user_id, token } = {}) {
  if (ilink_bot_id) {
    const hit = state.accounts.find((a) => a.ilinkBotId === ilink_bot_id);
    if (hit) return hit;
  }
  if (ilink_user_id) {
    const hit = state.accounts.find((a) => a.ilinkUserId === ilink_user_id);
    if (hit) return hit;
  }
  if (token) {
    const hit = state.accounts.find((a) => a.token === token);
    if (hit) return hit;
  }
  return null;
}

export function requireAccount(state, { ilink_bot_id, ilink_user_id } = {}) {
  if (ilink_bot_id || ilink_user_id) {
    const account = findAccount(state, { ilink_bot_id, ilink_user_id });
    if (!account?.token) {
      throw new Error(`未找到账号 ilink_bot_id=${ilink_bot_id || ""} ilink_user_id=${ilink_user_id || ""}`);
    }
    return account;
  }
  const accounts = listAccounts(state);
  if (!accounts.length) throw new Error("未登录。先 wechat_login_start 出码，立即 wechat_login_wait 至 logged_in=true");
  if (accounts.length === 1) return accounts[0];
  throw new Error("已绑定多个微信账号，请指定 ilink_bot_id");
}

export function localTokenList(state = loadState()) {
  return listAccounts(state)
    .map((a) => a.token)
    .slice(0, MAX_LOCAL_TOKENS);
}

export function upsertAccount(state, account) {
  const idx = state.accounts.findIndex((a) =>
    (account.ilinkBotId && a.ilinkBotId === account.ilinkBotId)
    || (account.token && a.token === account.token));
  if (idx >= 0) {
    state.accounts[idx] = { ...state.accounts[idx], ...account };
    const [updated] = state.accounts.splice(idx, 1);
    state.accounts.unshift(updated);
  } else {
    state.accounts.unshift(account);
  }
  return state;
}

export function removeAccount(state, { ilink_bot_id, ilink_user_id } = {}) {
  state.accounts = state.accounts.filter((a) => {
    if (ilink_bot_id && a.ilinkBotId === ilink_bot_id) return false;
    if (ilink_user_id && a.ilinkUserId === ilink_user_id) return false;
    return true;
  });
  return state;
}

export function rememberContext(state, account, userId, contextToken) {
  if (!account || !userId || !contextToken) return state;
  account.contextTokens[userId] = { token: contextToken, at: Date.now() };
  if (!state.seenPeers) state.seenPeers = [];
  if (!state.seenPeers.includes(userId)) state.seenPeers.push(userId);
  return state;
}

export function contextFor(account, userId) {
  const entry = account?.contextTokens?.[userId];
  return entry?.token || "";
}

export function accountsWithContext(state, userId) {
  return listAccounts(state).filter((a) => contextFor(a, userId));
}

export function resolveAccountForPeer(state, { to_user_id, ilink_bot_id } = {}) {
  if (ilink_bot_id) return requireAccount(state, { ilink_bot_id });
  if (!to_user_id) throw new Error("没有目标用户。传入 to_user_id，或等对方先发一条微信");
  const matches = accountsWithContext(state, to_user_id);
  if (!matches.length) {
    throw new Error("还没有该用户的 context_token。对方需要先从微信给你发一条消息，会话通道才会建立");
  }
  if (matches.length > 1) {
    throw new Error(`多个账号都能回复 ${to_user_id}，请指定 ilink_bot_id`);
  }
  return matches[0];
}

export function isAllowed(state, userId) {
  if (!userId) return false;
  if (!state.allowFrom?.length) return true;
  return state.allowFrom.includes(userId);
}

export function allowlistOpen(state = loadState()) {
  return !state.allowFrom?.length;
}

function readJsonl(file) {
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function writeJsonl(file, rows) {
  ensureHome();
  const tmp = `${file}.tmp`;
  const body = rows.length ? `${rows.map((r) => JSON.stringify(r)).join("\n")}\n` : "";
  fs.writeFileSync(tmp, body, { mode: 0o600 });
  fs.renameSync(tmp, file);
  tryChmod(file, 0o600);
}

function loadSeenIds() {
  const { inboxSeen } = paths();
  if (!fs.existsSync(inboxSeen)) return new Set();
  try {
    const arr = JSON.parse(fs.readFileSync(inboxSeen, "utf8"));
    return new Set(Array.isArray(arr) ? arr : []);
  } catch {
    return new Set();
  }
}

function saveSeenIds(set) {
  const { inboxSeen } = paths();
  const arr = [...set].slice(-5000);
  const tmp = `${inboxSeen}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(arr), { mode: 0o600 });
  fs.renameSync(tmp, inboxSeen);
  tryChmod(inboxSeen, 0o600);
}

function messageKey(m) {
  const id = m?.message_id;
  if (id !== undefined && id !== null && String(id) !== "") {
    return `${m.ilink_bot_id || ""}::${String(id)}`;
  }
  return "";
}

export function appendInbox(messages) {
  if (!messages.length) return { appended: 0, skipped_dup: 0 };
  return withLock("inbox", () => {
    ensureHome();
    const { inbox } = paths();
    const seen = loadSeenIds();
    const fresh = [];
    let skipped = 0;
    for (const m of messages) {
      const key = messageKey(m);
      if (key) {
        if (seen.has(key)) {
          skipped += 1;
          continue;
        }
        seen.add(key);
      }
      fresh.push(m);
    }
    if (fresh.length) {
      const lines = `${fresh.map((m) => JSON.stringify(m)).join("\n")}\n`;
      fs.appendFileSync(inbox, lines);
      try {
        const fd = fs.openSync(inbox, "r+");
        try {
          fs.fsyncSync(fd);
        } finally {
          fs.closeSync(fd);
        }
      } catch {
        // ignore
      }
      tryChmod(inbox, 0o600);
    }
    saveSeenIds(seen);
    return { appended: fresh.length, skipped_dup: skipped };
  });
}

/** Move inbox -> pending and return messages. Unacked can be re-read. */
export function claimInbox() {
  return withLock("inbox", () => {
    const { inbox, inboxPending } = paths();
    const incoming = readJsonl(inbox);
    const pending = readJsonl(inboxPending);
    const byKey = new Map();
    let anon = 0;
    for (const m of [...pending, ...incoming]) {
      let key = messageKey(m);
      if (!key) {
        key = `anon:${anon}:${JSON.stringify(m).slice(0, 160)}`;
        anon += 1;
      }
      if (!byKey.has(key)) byKey.set(key, m);
    }
    const all = [...byKey.values()];
    writeJsonl(inbox, []);
    writeJsonl(inboxPending, all);
    return all;
  });
}

/** @deprecated alias — now claims to pending instead of discarding */
export function drainInbox() {
  return claimInbox();
}

export function ackInbox(messageIds) {
  return withLock("inbox", () => {
    const { inboxPending } = paths();
    const pending = readJsonl(inboxPending);
    if (!messageIds || !messageIds.length) {
      writeJsonl(inboxPending, []);
      return { acked: pending.length, remaining: 0 };
    }
    const idSet = new Set(messageIds.map(String));
    const remaining = pending.filter((m) => !idSet.has(String(m.message_id ?? "")));
    const acked = pending.length - remaining.length;
    writeJsonl(inboxPending, remaining);
    return { acked, remaining: remaining.length };
  });
}

export function peekInboxCount() {
  return readJsonl(paths().inbox).length;
}

export function peekPendingCount() {
  return readJsonl(paths().inboxPending).length;
}

export function clearInboxFiles() {
  return withLock("inbox", () => {
    writeJsonl(paths().inbox, []);
    writeJsonl(paths().inboxPending, []);
    return { cleared: true };
  });
}

export function filterInboxByBotId(ilinkBotId) {
  if (!ilinkBotId) return clearInboxFiles();
  return withLock("inbox", () => {
    const { inbox, inboxPending } = paths();
    const keepInbox = readJsonl(inbox).filter((m) => m.ilink_bot_id !== ilinkBotId);
    const keepPending = readJsonl(inboxPending).filter((m) => m.ilink_bot_id !== ilinkBotId);
    writeJsonl(inbox, keepInbox);
    writeJsonl(inboxPending, keepPending);
    return {
      remaining_inbox: keepInbox.length,
      remaining_pending: keepPending.length,
    };
  });
}

export function writePid(pid) {
  ensureHome();
  fs.writeFileSync(paths().pid, String(pid), { mode: 0o600 });
  tryChmod(paths().pid, 0o600);
}

export function readPid() {
  const { pid } = paths();
  if (!fs.existsSync(pid)) return 0;
  const n = Number(fs.readFileSync(pid, "utf8").trim());
  return Number.isFinite(n) ? n : 0;
}

export function clearPid() {
  const { pid } = paths();
  if (fs.existsSync(pid)) fs.unlinkSync(pid);
}

export function readProcessCmdline(pid) {
  try {
    const raw = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8");
    return raw.replace(/\0/g, " ").trim();
  } catch {
    try {
      return execSync(`ps -p ${pid} -o args=`, { encoding: "utf8" }).trim();
    } catch {
      return "";
    }
  }
}

export function isMonitorPid(pid) {
  if (!pid || !isPidAlive(pid)) return false;
  const args = readProcessCmdline(pid);
  return args.includes("--monitor") && args.includes("grok-wechat-plugin/server/index.js");
}

export function findMonitorPids() {
  const found = new Set();
  try {
    const out = execSync('pgrep -f "grok-wechat-plugin/server/index.js --monitor" 2>/dev/null || true', {
      encoding: "utf8",
    });
    for (const line of out.split("\n")) {
      const n = Number(line.trim());
      if (isMonitorPid(n)) found.add(n);
    }
  } catch {
    // ignore
  }
  return [...found];
}

export function monitorRunning() {
  const pid = readPid();
  if (isMonitorPid(pid)) return pid;
  if (pid) clearPid();
  const orphans = findMonitorPids();
  if (orphans.length) {
    writePid(orphans[0]);
    return orphans[0];
  }
  return 0;
}

export function fallbackWakePath() {
  return path.join(homeDir(), "wake.json");
}

export function loadFallbackWake() {
  const wakeFile = fallbackWakePath();
  if (!fs.existsSync(wakeFile)) return null;
  try {
    const cfg = JSON.parse(fs.readFileSync(wakeFile, "utf8"));
    if (cfg.url && cfg.key) return { url: cfg.url, key: cfg.key };
  } catch {
    // ignore
  }
  return null;
}

export function saveGlobalWake(url, key) {
  ensureHome();
  const wakeFile = fallbackWakePath();
  const tmp = `${wakeFile}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ url, key }, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, wakeFile);
  tryChmod(wakeFile, 0o600);
}

export function hasGlobalWake() {
  return Boolean(loadFallbackWake());
}

export function resolveWakeForAccount(account) {
  // Account-level wake keys are intentionally unused; global wake.json only.
  void account;
  return loadFallbackWake();
}

export function hasAccountWake(account) {
  void account;
  return hasGlobalWake();
}

// --- wake retry queue ---

function loadWakeMeta() {
  const { wakeMeta } = paths();
  if (!fs.existsSync(wakeMeta)) {
    return { last_status: null, last_at: 0, last_error: "", last_bot_id: "", coalesce: {} };
  }
  try {
    return {
      last_status: null,
      last_at: 0,
      last_error: "",
      last_bot_id: "",
      coalesce: {},
      ...JSON.parse(fs.readFileSync(wakeMeta, "utf8")),
    };
  } catch {
    return { last_status: null, last_at: 0, last_error: "", last_bot_id: "", coalesce: {} };
  }
}

function saveWakeMeta(meta) {
  ensureHome();
  const { wakeMeta } = paths();
  const tmp = `${wakeMeta}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(meta, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, wakeMeta);
  tryChmod(wakeMeta, 0o600);
}

export function getWakeMeta() {
  return withLock("wake", () => loadWakeMeta());
}

export function enqueueWake(entry) {
  return withLock("wake", () => {
    ensureHome();
    const { wakeQueue } = paths();
    const row = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      attempts: 0,
      next_at: Date.now(),
      created_at: Date.now(),
      ...entry,
    };
    fs.appendFileSync(wakeQueue, `${JSON.stringify(row)}\n`);
    tryChmod(wakeQueue, 0o600);
    return row;
  });
}

/** Merge coalesce wakes for the same bot into one delayed job (first wake already fired). */
export function enqueueOrMergeCoalesceWake(entry, delayUntil) {
  return withLock("wake", () => {
    ensureHome();
    const rows = readJsonl(paths().wakeQueue);
    const botId = entry.ilink_bot_id || "";
    const idx = rows.findIndex(
      (r) => r.reason === "coalesce" && (r.ilink_bot_id || "") === botId && (r.attempts || 0) === 0,
    );
    const uniq = (arr) => [...new Set((arr || []).filter((x) => x !== undefined && x !== ""))];
    if (idx >= 0) {
      const cur = rows[idx];
      const merged = {
        ...cur,
        count: (cur.count || 0) + (entry.count || 0),
        from_user_ids: uniq([...(cur.from_user_ids || []), ...(entry.from_user_ids || [])]),
        message_ids: uniq([...(cur.message_ids || []), ...(entry.message_ids || [])]),
        next_at: Math.max(cur.next_at || 0, delayUntil || Date.now()),
        dedicated_assistant_id: entry.dedicated_assistant_id || cur.dedicated_assistant_id || "",
        dedicated_assistant_name: entry.dedicated_assistant_name || cur.dedicated_assistant_name || "",
        ilink_user_id: entry.ilink_user_id || cur.ilink_user_id || "",
      };
      rows[idx] = merged;
      writeJsonl(paths().wakeQueue, rows);
      return merged;
    }
    const row = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      attempts: 0,
      created_at: Date.now(),
      next_at: delayUntil || Date.now(),
      reason: "coalesce",
      ...entry,
    };
    // ensure next_at from delayUntil wins over entry
    row.next_at = delayUntil || Date.now();
    rows.push(row);
    writeJsonl(paths().wakeQueue, rows);
    return row;
  });
}

export function wakeQueueDepth() {
  return readJsonl(paths().wakeQueue).length;
}

export function listDueWakeJobs(now = Date.now()) {
  return withLock("wake", () => {
    const rows = readJsonl(paths().wakeQueue);
    return rows.filter((r) => (r.next_at || 0) <= now);
  });
}

export function updateWakeJobs(mutator) {
  return withLock("wake", () => {
    const rows = readJsonl(paths().wakeQueue);
    const next = mutator(rows) ?? rows;
    writeJsonl(paths().wakeQueue, next);
    return next;
  });
}

export function recordWakeResult({ botId, ok, status, error }) {
  return withLock("wake", () => {
    const meta = loadWakeMeta();
    meta.last_status = ok ? "ok" : "error";
    meta.last_at = Date.now();
    meta.last_error = error || "";
    meta.last_http_status = status ?? null;
    meta.last_bot_id = botId || "";
    if (!meta.coalesce) meta.coalesce = {};
    if (botId) meta.coalesce[botId] = Date.now();
    saveWakeMeta(meta);
    return meta;
  });
}

export function shouldCoalesceWake(botId, windowMs = 2000) {
  return withLock("wake", () => {
    const meta = loadWakeMeta();
    const last = meta.coalesce?.[botId] || 0;
    return Date.now() - last < windowMs;
  });
}

export function markCoalesceWake(botId) {
  return withLock("wake", () => {
    const meta = loadWakeMeta();
    if (!meta.coalesce) meta.coalesce = {};
    meta.coalesce[botId] = Date.now();
    saveWakeMeta(meta);
  });
}

export function sanitizeMediaLabel(label) {
  const base = path.basename(String(label || "file.bin")).replace(/[^\w.\-()+@]+/g, "_");
  const cleaned = base.replace(/^\.+/, "") || "file.bin";
  if (cleaned.includes("..") || cleaned.includes("/") || cleaned.includes("\\")) {
    throw new Error("非法媒体文件名");
  }
  return cleaned.slice(0, 180);
}

export function safeMediaPath(label) {
  ensureHome();
  const mediaRoot = path.resolve(paths().media);
  const name = `${Date.now()}-${sanitizeMediaLabel(label)}`;
  const full = path.resolve(mediaRoot, name);
  if (!full.startsWith(`${mediaRoot}${path.sep}`) && full !== mediaRoot) {
    throw new Error("媒体路径越界");
  }
  return full;
}

export function assertSendMediaPathAllowed(filePath) {
  const resolved = path.resolve(filePath);
  const allowedRoots = [
    path.resolve("/home/box"),
    path.resolve("/workspace"),
    path.resolve(paths().media),
  ];
  const ok = allowedRoots.some((root) => resolved === root || resolved.startsWith(`${root}${path.sep}`));
  if (!ok) throw new Error(`不允许读取该路径（仅 /home/box、/workspace 与 media 目录）: ${resolved}`);
  if (!fs.existsSync(resolved)) throw new Error(`文件不存在: ${resolved}`);
  return resolved;
}

export function rotateLogIfHuge(maxBytes = 10 * 1024 * 1024) {
  const { log } = paths();
  try {
    if (!fs.existsSync(log)) return false;
    const st = fs.statSync(log);
    if (st.size <= maxBytes) return false;
    const bak = `${log}.1`;
    try {
      if (fs.existsSync(bak)) fs.unlinkSync(bak);
    } catch {
      // ignore
    }
    fs.renameSync(log, bak);
    fs.writeFileSync(log, "", { mode: 0o600 });
    tryChmod(log, 0o600);
    return true;
  } catch {
    return false;
  }
}

export function pruneMediaOlderThan(maxAgeMs = 7 * 24 * 3600 * 1000) {
  const { media } = paths();
  let removed = 0;
  try {
    const now = Date.now();
    for (const name of fs.readdirSync(media)) {
      const full = path.join(media, name);
      try {
        const st = fs.statSync(full);
        if (st.isFile() && now - st.mtimeMs > maxAgeMs) {
          fs.unlinkSync(full);
          removed += 1;
        }
      } catch {
        // ignore
      }
    }
  } catch {
    // ignore
  }
  return removed;
}

export function advanceGetUpdatesBuf(token, buf) {
  if (typeof buf !== "string") return;
  updateState((s) => {
    const hit = findAccount(s, { token });
    if (hit) hit.getUpdatesBuf = buf;
    return s;
  });
}
