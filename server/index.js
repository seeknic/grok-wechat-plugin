#!/usr/bin/env node
import { spawn } from "node:child_process";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import {
  ackInbox,
  advanceGetUpdatesBuf,
  appendInbox,
  claimInbox,
  clearInboxFiles,
  clearPid,
  enqueueWake,
  enqueueOrMergeCoalesceWake,
  filterInboxByBotId,
  findAccount,
  findMonitorPids,
  getWakeMeta,
  isAllowed,
  isMonitorPid,
  listAccounts,
  loadState,
  markCoalesceWake,
  monitorRunning,
  paths,
  peekInboxCount,
  peekPendingCount,
  pruneMediaOlderThan,
  recordWakeResult,
  removeAccount,
  resolveWakeForAccount,
  rotateLogIfHuge,
  shouldCoalesceWake,
  updateState,
  updateWakeJobs,
  wakeQueueDepth,
  withLock,
  writePid,
} from "./store.js";
import {
  collectInbound,
  getUpdates,
  loginStart,
  loginWait,
  markSessionExpired,
  notifyStart,
  notifyStop,
  sendMedia,
  sendText,
  setAccountWake,
  setDedicatedAssistant,
  setTyping,
  statusPayload,
} from "./ilink.js";
import { registerShutdownCleanup, touchConnectorActive, connectorAbandoned, runUninstall, uninstallWechat } from "./uninstall.js";

const self = fileURLToPath(import.meta.url);
const WAKE_BACKOFF_MS = [5_000, 15_000, 60_000, 180_000, 600_000];
const COALESCE_MS = 2_000;

try {
  fs.mkdirSync("/workspace/.grok-wechat", { recursive: true });
  fs.appendFileSync(
    "/workspace/.grok-wechat/mcp-host.log",
    `${new Date().toISOString()} spawn pid=${process.pid} argv=${JSON.stringify(process.argv)} cwd=${process.cwd()}\n`,
  );
} catch { /* ignore */ }
try {
  if (process.stdout._handle?.setBlocking) process.stdout._handle.setBlocking(true);
  if (process.stdin._handle?.setBlocking) process.stdin._handle.setBlocking(true);
} catch { /* ignore */ }

function ok(data) {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function fail(message) {
  return { content: [{ type: "text", text: JSON.stringify({ error: message }, null, 2) }], isError: true };
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

function reapOrphanMonitors(keepPid = 0) {
  for (const pid of findMonitorPids()) {
    if (keepPid && pid === keepPid) continue;
    if (pid === process.pid) continue;
    try {
      process.kill(pid, "SIGTERM");
    } catch {
      // ignore
    }
  }
}

function ensureMonitor() {
  return withLock("monitor", () => {
    const existing = monitorRunning();
    if (existing && isMonitorPid(existing)) {
      reapOrphanMonitors(existing);
      return { already_running: true, pid: existing };
    }
    const accounts = listAccounts();
    if (!accounts.length) return { started: false, reason: "not_logged_in" };
    reapOrphanMonitors(0);
    const child = spawn(process.execPath, [self, "--monitor"], {
      detached: true,
      stdio: "ignore",
      env: process.env,
    });
    child.unref();
    writePid(child.pid);
    return { started: true, pid: child.pid, account_count: accounts.length };
  });
}

const accountRefSchema = {
  ilink_bot_id: { type: "string", description: "指定要操作的已绑定微信账号 bot id" },
  ilink_user_id: { type: "string", description: "指定要操作的已绑定微信 user id" },
};

const tools = {
  wechat_login_start: {
    description: "获取并向用户展示微信 iLink 登录二维码。出码后下一步立即调用 wechat_login_wait，轮询至 logged_in=true。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok(await loginStart()),
  },
  wechat_login_wait: {
    description: "轮询等待扫码结果。wechat_login_start 出码后立即调用；logged_in=false 且未过期时重复调用；expired=true 时重新 wechat_login_start 后再 wait。",
    inputSchema: {
      type: "object",
      properties: {
        timeout_ms: { type: "number", description: "本次等待毫秒，默认 120000" },
        qrcode: { type: "string", description: "可选，指定等待的二维码 id；默认等待最近一次 wechat_login_start 返回的码" },
      },
    },
    handle: async ({ timeout_ms, qrcode }) => {
      const r = await loginWait(timeout_ms || 120_000, qrcode);
      if (r && r.logged_in) ensureMonitor();
      return ok(r);
    },
  },
  wechat_status: {
    description: "查看账号、global wake、专属助手、allowlist（含 allowlist_open）、inbox/pending、wake 队列、monitor 等。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => {
      const payload = statusPayload();
      payload.monitor_pid = monitorRunning();
      payload.monitor_ok = Boolean(payload.monitor_pid);
      return ok(payload);
    },
  },
  wechat_logout: {
    description: "登出微信账号。默认登出全部已绑定账号；可指定 ilink_bot_id 只登出一个。",
    inputSchema: {
      type: "object",
      properties: {
        ...accountRefSchema,
        all: { type: "boolean", description: "true 时登出全部账号（默认行为）" },
      },
    },
    handle: async ({ ilink_bot_id, ilink_user_id, all }) => {
      const pid = monitorRunning();
      const state = loadState();
      const logoutAll = all !== false && !ilink_bot_id && !ilink_user_id;
      if (logoutAll) {
        if (pid) {
          try { process.kill(pid, "SIGTERM"); } catch { /* already gone */ }
          clearPid();
        }
        updateState((s) => {
          s.accounts = [];
          s.pendingQrs = [];
          return s;
        });
        clearInboxFiles();
        return ok({ logged_out: true, scope: "all", inbox_cleared: true });
      }
      const before = listAccounts(state).length;
      const target = findAccount(state, { ilink_bot_id, ilink_user_id });
      const botId = target?.ilinkBotId || ilink_bot_id || "";
      updateState((s) => removeAccount(s, { ilink_bot_id, ilink_user_id }));
      const after = listAccounts(loadState()).length;
      if (after === before) {
        throw new Error("未找到要登出的账号");
      }
      if (botId) filterInboxByBotId(botId);
      if (!after && pid) {
        try { process.kill(pid, "SIGTERM"); } catch { /* already gone */ }
        clearPid();
      } else if (after && !monitorRunning()) {
        ensureMonitor();
      }
      return ok({
        logged_out: true,
        scope: "one",
        ilink_bot_id: botId || undefined,
        remaining_accounts: after,
      });
    },
  },
  wechat_inbox: {
    description: "取出入站消息并移入 pending（不永久删除）。处理成功后必须调用 wechat_inbox_ack 确认；未 ack 的消息可再次被 wechat_inbox 读到。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => {
      const messages = claimInbox();
      return ok({
        messages,
        from_monitor: true,
        pending: true,
        hint: "处理完后调用 wechat_inbox_ack，传入 message_ids；省略则清空全部 pending。",
      });
    },
  },
  wechat_inbox_ack: {
    description: "确认已处理的入站消息：按 message_ids 从 pending 删除；省略 message_ids 则清空全部 pending。",
    inputSchema: {
      type: "object",
      properties: {
        message_ids: {
          type: "array",
          items: { type: ["string", "number"] },
          description: "要确认删除的 message_id 列表；省略则清空全部 pending",
        },
      },
    },
    handle: async ({ message_ids }) => ok(ackInbox(message_ids)),
  },
  wechat_send: {
    description: "向微信用户发文本。必须带该用户已缓存的 context_token（对方先发过消息）。Markdown 会转成纯文本。",
    inputSchema: {
      type: "object",
      properties: {
        text: { type: "string" },
        to_user_id: { type: "string", description: "微信对端 user id" },
        ilink_bot_id: accountRefSchema.ilink_bot_id,
      },
      required: ["text", "to_user_id"],
    },
    handle: async ({ text, to_user_id, ilink_bot_id }) => ok(await sendText(to_user_id, text, ilink_bot_id)),
  },
  wechat_send_media: {
    description: "发送图片/视频/文件。路径须在 /home/box、/workspace 或 media 目录下。",
    inputSchema: {
      type: "object",
      properties: {
        path: { type: "string" },
        kind: { type: "string", enum: ["image", "video", "file"] },
        to_user_id: { type: "string" },
        ilink_bot_id: accountRefSchema.ilink_bot_id,
        caption: { type: "string" },
      },
      required: ["path", "kind", "to_user_id"],
    },
    handle: async ({ path, kind, to_user_id, ilink_bot_id, caption }) =>
      ok(await sendMedia(to_user_id, path, kind, caption, ilink_bot_id)),
  },
  wechat_typing: {
    description: "发送或取消「正在输入」。回复前先打开，发完后关闭。",
    inputSchema: {
      type: "object",
      properties: {
        on: { type: "boolean" },
        to_user_id: { type: "string" },
        ilink_bot_id: accountRefSchema.ilink_bot_id,
      },
      required: ["on", "to_user_id"],
    },
    handle: async ({ on, to_user_id, ilink_bot_id }) => ok(await setTyping(to_user_id, on, ilink_bot_id)),
  },
  wechat_start_monitor: {
    description: "在本机后台启动长轮询，为每个已绑定账号拉取入站消息并按账号 POST 对应 webhook。已在跑则直接返回。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok(ensureMonitor()),
  },
  wechat_set_wake: {
    description: "探测成功（HTTP 2xx）后保存全局入站 webhook（wake.json，权限 600）。仅写入全局配置，不把 key 写入 account.json。",
    inputSchema: {
      type: "object",
      properties: {
        url: { type: "string", description: "安装助手 Routine「微信入站唤醒」的 webhook 地址" },
        key: { type: "string", description: "webhook 密钥" },
        ilink_bot_id: { type: "string", description: "可选，仅用于返回信息；密钥不会写入账号" },
      },
      required: ["url", "key"],
    },
    handle: async ({ ilink_bot_id, url, key }) => ok(await setAccountWake(ilink_bot_id, url, key)),
  },
  wechat_set_dedicated_assistant: {
    description: "为已绑定微信账号登记专属 Grok Bot 助手 id/名称。每次扫码绑定成功后创建专属助手并调用，供入站按 ilink_bot_id 转交与卸载侧栏列表。",
    inputSchema: {
      type: "object",
      properties: {
        ilink_bot_id: { type: "string", description: "已绑定账号 bot id" },
        assistant_id: { type: "string", description: "专属助手 id" },
        assistant_name: { type: "string", description: "专属助手侧栏名称（如「微信1」）" },
      },
      required: ["ilink_bot_id", "assistant_name"],
    },
    handle: async ({ ilink_bot_id, assistant_id, assistant_name }) =>
      ok(setDedicatedAssistant(ilink_bot_id, assistant_id, assistant_name)),
  },
  wechat_stop_monitor: {
    description: "停止后台长轮询。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => {
      const pid = monitorRunning();
      if (!pid) return ok({ stopped: true, was_running: false });
      try { process.kill(pid, "SIGTERM"); } catch { /* ignore */ }
      clearPid();
      return ok({ stopped: true, pid });
    },
  },
  wechat_approve: {
    description: "把用户加入 allowlist。allowlist 为空表示接受所有私信（allowlist_open=true）；非空则只处理列表内用户。",
    inputSchema: {
      type: "object",
      properties: {
        user_id: { type: "string" },
        clear: { type: "boolean", description: "true 时清空 allowlist，恢复接收全部私信" },
      },
    },
    handle: async ({ user_id, clear }) => {
      const state = updateState((s) => {
        if (clear) s.allowFrom = [];
        else if (user_id && !s.allowFrom.includes(user_id)) s.allowFrom.push(user_id);
        return s;
      });
      return ok({ allow_from: state.allowFrom, allowlist_open: !state.allowFrom.length });
    },
  },
  wechat_uninstall: {
    description: "手动完整卸载微信渠道（唯一会删除状态/插件的路径）：停止 monitor、删除状态与插件目录、清理自启项。不会在 MCP 退出或重启时自动触发。",
    inputSchema: { type: "object", properties: {} },
    handle: async () => ok(uninstallWechat()),
  },
};

const TOOL_LIST = Object.entries(tools).map(([name, t]) => ({
  name,
  description: t.description,
  inputSchema: t.inputSchema,
}));

async function dispatch(name, args) {
  const tool = tools[name];
  if (!tool) return fail(`未知工具: ${name}`);
  try {
    return await tool.handle(args || {});
  } catch (err) {
    return fail(err instanceof Error ? err.message : String(err));
  }
}

function writeMessage(msg) {
  const json = JSON.stringify(msg);
  fs.writeSync(1, json + "\n");
  try {
    fs.appendFileSync("/workspace/.grok-wechat/mcp-host.log", `${new Date().toISOString()} out ${json.slice(0, 200)}\n`);
  } catch { /* ignore */ }
}

async function onRpc(msg) {
  try {
    fs.appendFileSync("/workspace/.grok-wechat/mcp-host.log", `${new Date().toISOString()} in ${JSON.stringify({id: msg.id, method: msg.method}).slice(0,300)}\n`);
  } catch { /* ignore */ }
  const { id, method, params } = msg;
  if (method === "initialize") {
    touchConnectorActive();
    ensureMonitor();
    return {
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: params?.protocolVersion || "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "grok-wechat", version: "1.1.0" },
      },
    };
  }
  if (method === "notifications/initialized" || method === "initialized") return null;
  if (method === "ping") return { jsonrpc: "2.0", id, result: {} };
  if (method === "tools/list") {
    touchConnectorActive();
    return { jsonrpc: "2.0", id, result: { tools: TOOL_LIST } };
  }
  if (method === "tools/call") {
    touchConnectorActive();
    const result = await dispatch(params?.name, params?.arguments);
    return { jsonrpc: "2.0", id, result };
  }
  if (id === undefined) return null;
  return { jsonrpc: "2.0", id, error: { code: -32601, message: `Method not found: ${method}` } };
}

function startMcp() {
  // Auto-uninstall on exit is disabled; registerShutdownCleanup is a no-op.
  registerShutdownCleanup();
  ensureMonitor();
  let buf = Buffer.alloc(0);
  process.stdin.on("data", async (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      const headerEnd = buf.indexOf("\r\n\r\n");
      if (headerEnd === -1) {
        const nl = buf.indexOf("\n");
        if (nl !== -1) {
          const line = buf.slice(0, nl).toString("utf8").trim();
          buf = buf.slice(nl + 1);
          if (line.startsWith("{")) {
            try {
              const reply = await onRpc(JSON.parse(line));
              if (reply) writeMessage(reply);
            } catch (err) {
              writeMessage({ jsonrpc: "2.0", id: null, error: { code: -32700, message: String(err) } });
            }
            continue;
          }
        }
        break;
      }
      const header = buf.slice(0, headerEnd).toString("utf8");
      const match = header.match(/Content-Length:\s*(\d+)/i);
      if (!match) {
        buf = buf.slice(headerEnd + 4);
        continue;
      }
      const len = Number(match[1]);
      const start = headerEnd + 4;
      if (buf.length < start + len) break;
      const body = buf.slice(start, start + len).toString("utf8");
      buf = buf.slice(start + len);
      try {
        const reply = await onRpc(JSON.parse(body));
        if (reply) writeMessage(reply);
      } catch (err) {
        writeMessage({ jsonrpc: "2.0", id: null, error: { code: -32700, message: String(err) } });
      }
    }
  });
}

function startSafetyKeeper() {
  const log = (line) => {
    try {
      fs.appendFileSync(paths().log, `${new Date().toISOString()} safety-keeper ${line}\n`);
    } catch { /* ignore */ }
  };
  log(`start pid=${process.pid}`);
  setInterval(() => {
    touchConnectorActive();
  }, 60_000);
  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));
  // Stay alive so deferred cleanup (legacy hosts) sees an MCP_MATCH process.
  setInterval(() => {}, 60_000);
}

async function postWake(cfg, payload, log) {
  const res = await fetch(cfg.url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${cfg.key}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  log(`wake bot=${payload.ilink_bot_id || ""} ${res.status} ${text.slice(0, 180)}`);
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}: ${text.slice(0, 180)}`);
    err.status = res.status;
    throw err;
  }
  return res.status;
}

async function wakeAgent(messages, log) {
  if (!messages.length) return;
  const state = loadState();
  const byBot = new Map();
  for (const m of messages) {
    const botId = m.ilink_bot_id || "";
    if (!byBot.has(botId)) byBot.set(botId, []);
    byBot.get(botId).push(m);
  }
  for (const [botId, batch] of byBot) {
    const account = findAccount(state, { ilink_bot_id: botId });
    const cfg = resolveWakeForAccount(account);
    if (!cfg) {
      log(`wake skipped bot=${botId}: no wake config`);
      continue;
    }
    if (shouldCoalesceWake(botId, COALESCE_MS)) {
      // First wake already notified the agent; delay+merge follow-ups so we don't
      // fire a second webhook immediately (duplicate handoffs). Agent still gets
      // a single catch-up wake after the window for messages that arrived mid-turn.
      const meta = getWakeMeta();
      const last = meta.coalesce?.[botId] || Date.now();
      const delayUntil = last + COALESCE_MS;
      log(`wake coalesce bot=${botId} (defer until ${delayUntil}, window ${COALESCE_MS}ms)`);
      enqueueOrMergeCoalesceWake({
        ilink_bot_id: botId,
        ilink_user_id: account?.ilinkUserId || batch[0]?.ilink_user_id || "",
        count: batch.length,
        from_user_ids: batch.map((m) => m.from_user_id),
        message_ids: batch.map((m) => m.message_id).filter((x) => x !== undefined && x !== ""),
        dedicated_assistant_id: account?.dedicatedAssistant?.id || "",
        dedicated_assistant_name: account?.dedicatedAssistant?.name || "",
        reason: "coalesce",
      }, delayUntil);
      continue;
    }
    const payload = {
      source: "grok-wechat",
      count: batch.length,
      from_user_ids: batch.map((m) => m.from_user_id),
      message_ids: batch.map((m) => m.message_id).filter((x) => x !== undefined && x !== ""),
      ilink_bot_id: botId,
      ilink_user_id: account?.ilinkUserId || batch[0]?.ilink_user_id || "",
      dedicated_assistant_id: account?.dedicatedAssistant?.id || "",
      dedicated_assistant_name: account?.dedicatedAssistant?.name || "",
    };
    try {
      markCoalesceWake(botId);
      const status = await postWake(cfg, payload, log);
      recordWakeResult({ botId, ok: true, status });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`wake error bot=${botId} ${msg}`);
      recordWakeResult({ botId, ok: false, status: err?.status, error: msg });
      enqueueWake({
        ilink_bot_id: botId,
        ilink_user_id: payload.ilink_user_id,
        count: batch.length,
        from_user_ids: payload.from_user_ids,
        message_ids: payload.message_ids,
        dedicated_assistant_id: payload.dedicated_assistant_id,
        dedicated_assistant_name: payload.dedicated_assistant_name,
        last_error: msg,
      });
    }
  }
}

async function flushWakeQueue(log) {
  const state = loadState();
  const due = [];
  updateWakeJobs((rows) => {
    const now = Date.now();
    for (const row of rows) {
      if ((row.next_at || 0) <= now) due.push(row);
    }
    return rows;
  });
  if (!due.length) return;

  const keep = [];
  const all = [];
  updateWakeJobs((rows) => {
    all.push(...rows);
    return rows;
  });

  const dueIds = new Set(due.map((d) => d.id));
  for (const row of all) {
    if (!dueIds.has(row.id)) {
      keep.push(row);
      continue;
    }
    const account = findAccount(state, { ilink_bot_id: row.ilink_bot_id });
    const cfg = resolveWakeForAccount(account);
    if (!cfg) {
      log(`wake-queue drop bot=${row.ilink_bot_id}: no wake config`);
      continue;
    }
    const payload = {
      source: "grok-wechat",
      count: row.count || (row.from_user_ids || []).length || 1,
      from_user_ids: row.from_user_ids || [],
      message_ids: row.message_ids || [],
      ilink_bot_id: row.ilink_bot_id || "",
      ilink_user_id: row.ilink_user_id || "",
      dedicated_assistant_id: row.dedicated_assistant_id || account?.dedicatedAssistant?.id || "",
      dedicated_assistant_name: row.dedicated_assistant_name || account?.dedicatedAssistant?.name || "",
      retry: true,
      attempts: (row.attempts || 0) + 1,
    };
    try {
      const status = await postWake(cfg, payload, log);
      recordWakeResult({ botId: row.ilink_bot_id, ok: true, status });
      log(`wake-queue ok bot=${row.ilink_bot_id} attempt=${payload.attempts}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const attempts = (row.attempts || 0) + 1;
      const backoff = WAKE_BACKOFF_MS[Math.min(attempts - 1, WAKE_BACKOFF_MS.length - 1)];
      recordWakeResult({ botId: row.ilink_bot_id, ok: false, status: err?.status, error: msg });
      if (attempts >= 12) {
        log(`wake-queue give up bot=${row.ilink_bot_id} after ${attempts}: ${msg}`);
      } else {
        keep.push({
          ...row,
          attempts,
          next_at: Date.now() + backoff,
          last_error: msg,
        });
        log(`wake-queue retry bot=${row.ilink_bot_id} attempt=${attempts} in ${backoff}ms`);
      }
    }
  }
  updateWakeJobs(() => keep);
}

async function pollAccount(account, log) {
  if (account.sessionExpired) {
    const age = Date.now() - (account.sessionExpiredAt || 0);
    if (age < 5 * 60_000) {
      return { messages: [], expired: true, skipped: true };
    }
  }
  const raw = await getUpdates(account, { deferCursor: true });
  if (raw.session_expired) {
    log(`session expired bot=${account.ilinkBotId}`);
    markSessionExpired(account.ilinkBotId, true);
    const cfg = resolveWakeForAccount(account);
    if (cfg) {
      try {
        await postWake(cfg, {
          source: "grok-wechat",
          error: "session_expired",
          ilink_bot_id: account.ilinkBotId,
          ilink_user_id: account.ilinkUserId,
          count: 0,
        }, log);
      } catch (err) {
        log(`session_expired wake failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    return { messages: [], expired: true };
  }
  const collected = await collectInbound(raw.msgs || [], account);
  const state = loadState();
  const allowed = collected.messages.filter((m) => isAllowed(state, m.from_user_id));
  // Append (+fsync) BEFORE advancing cursor
  if (allowed.length) {
    appendInbox(allowed);
  }
  if (typeof raw.get_updates_buf === "string") {
    advanceGetUpdatesBuf(account.token, raw.get_updates_buf);
    account.getUpdatesBuf = raw.get_updates_buf;
  }
  if (account.sessionExpired) {
    markSessionExpired(account.ilinkBotId, false);
  }
  return { messages: allowed, expired: false };
}

async function runMonitor() {
  rotateLogIfHuge();
  const logStream = fs.createWriteStream(paths().log, { flags: "a" });
  const log = (line) => {
    logStream.write(`${new Date().toISOString()} ${line}\n`);
  };
  log("monitor start (reliability build)");
  writePid(process.pid);
  const accounts = listAccounts();
  await Promise.all(accounts.map((a) => notifyStart(a)));
  const onExit = async () => {
    const current = listAccounts();
    await Promise.all(current.map((a) => notifyStop(a)));
    clearPid();
    process.exit(0);
  };
  process.on("SIGTERM", onExit);
  process.on("SIGINT", onExit);
  let failures = 0;
  let loops = 0;
  while (true) {
    try {
      if (connectorAbandoned()) {
        // Do NOT uninstall. Stop monitor only; state and plugin remain.
        log("connector abandoned (>30s), stopping monitor without uninstall");
        clearPid();
        process.exit(0);
      }
      const accountsNow = listAccounts();
      if (!accountsNow.length) {
        log("no accounts, stopping");
        clearPid();
        process.exit(0);
      }
      const results = await Promise.all(accountsNow.map((account) => pollAccount(account, log)));
      failures = 0;
      const batch = results.flatMap((r) => r.messages);
      if (results.some((r) => r.expired)) {
        await sleep(60_000);
      }
      if (batch.length) {
        log(`inbox +${batch.length} (pending=${peekPendingCount()} ready=${peekInboxCount()})`);
        await wakeAgent(batch, log);
      }
      await flushWakeQueue(log);
      loops += 1;
      if (loops % 20 === 0) {
        rotateLogIfHuge();
        pruneMediaOlderThan();
      }
    } catch (err) {
      failures += 1;
      log(`getupdates error ${err instanceof Error ? err.message : String(err)}`);
      await sleep(failures >= 3 ? 30_000 : 2_000);
    }
  }
}

const argv = process.argv;
if (argv.includes("--safety-keeper")) {
  // Intentionally NOT monitor mode. Keeps a process matching MCP_MATCH alive.
  startSafetyKeeper();
} else if (argv.includes("--monitor")) {
  runMonitor();
} else if (argv.includes("--ensure-monitor")) {
  process.stdout.write(JSON.stringify(ensureMonitor()) + "\n");
} else if (argv.includes("--uninstall")) {
  process.stdout.write(`${JSON.stringify(runUninstall({ deferPluginRemoval: false }), null, 2)}\n`);
} else {
  startMcp();
}
