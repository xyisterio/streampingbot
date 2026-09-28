"use strict";

/**
 * StreamPingBot — один Node-процесс (для Suga и любого другого always-on хостинга).
 *
 * Делает всё сам, без Cloudflare Worker и без внешних чекеров:
 *  - принимает сообщения Telegram через long polling (getUpdates) — публичный URL и вебхук не нужны;
 *  - раз в CHECK_INTERVAL_SEC секунд проверяет статусы отслеживаемых каналов;
 *  - при переходе offline -> online шлёт уведомление подписчикам;
 *  - хранит подписки и статусы в Redis (REDIS_URL). Без REDIS_URL — в памяти (только для локальных тестов).
 *
 * Переменные окружения:
 *  BOT_TOKEN           обязательно — токен от @BotFather
 *  REDIS_URL           адрес Redis, например redis://default:pass@host:6379
 *  CHECK_INTERVAL_SEC  необязательно, по умолчанию 120
 *  DEBUG_KEY           необязательно — включает GET /debug?key=...&user=... (диагностика запроса к Chaturbate)
 *  PORT                необязательно — порт для health-check, по умолчанию 3000
 */

const http = require("http");

const BOT_TOKEN = process.env.BOT_TOKEN;
const REDIS_URL = process.env.REDIS_URL || "";
const CHECK_INTERVAL_SEC = Number(process.env.CHECK_INTERVAL_SEC || 120);
const PORT = Number(process.env.PORT || 3000);
const DEBUG_KEY = process.env.DEBUG_KEY || "";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// STORAGE — Redis (или память для локальных тестов)
// ---------------------------------------------------------------------------

let store;

function memoryStore() {
  const sets = new Map();
  const strings = new Map();
  return {
    async sadd(k, v) { (sets.get(k) || sets.set(k, new Set()).get(k)).add(v); },
    async srem(k, v) { const s = sets.get(k); if (s) { s.delete(v); if (!s.size) sets.delete(k); } },
    async smembers(k) { return [...(sets.get(k) || [])]; },
    async get(k) { return strings.has(k) ? strings.get(k) : null; },
    async set(k, v, ttlSec) {
      strings.set(k, v);
      if (ttlSec) setTimeout(() => strings.delete(k), ttlSec * 1000).unref();
    },
    async del(k) { strings.delete(k); },
  };
}

async function createStore() {
  if (!REDIS_URL) {
    console.warn("REDIS_URL не задан — данные хранятся в памяти и пропадут при перезапуске!");
    return memoryStore();
  }
  const { createClient } = require("redis");
  const client = createClient({ url: REDIS_URL });
  client.on("error", (e) => console.error("redis error:", e.message));
  await client.connect();
  console.log("Redis подключён");
  return {
    sadd: (k, v) => client.sAdd(k, v),
    srem: (k, v) => client.sRem(k, v),
    smembers: (k) => client.sMembers(k),
    get: (k) => client.get(k),
    set: (k, v, ttlSec) => (ttlSec ? client.set(k, v, { EX: ttlSec }) : client.set(k, v)),
    del: (k) => client.del(k),
  };
}

const K = {
  watchers: (p, c) => `watchers:${p}:${c}`, // set chatId — кто следит за каналом
  subs: (chat) => `subs:${chat}`,           // set "provider:channel" — подписки чата
  channels: (p) => `channels:${p}`,         // set channel — все отслеживаемые каналы провайдера
  status: (p, c) => `status:${p}:${c}`,     // "online" | "offline"
  state: (chat) => `state:${chat}`,         // "awaiting_watch"
};

async function addSubscription(chatId, provider, channel) {
  await store.sadd(K.watchers(provider, channel), String(chatId));
  await store.sadd(K.subs(chatId), `${provider}:${channel}`);
  await store.sadd(K.channels(provider), channel);
}

async function removeSubscription(chatId, provider, channel) {
  await store.srem(K.watchers(provider, channel), String(chatId));
  await store.srem(K.subs(chatId), `${provider}:${channel}`);
  const left = await store.smembers(K.watchers(provider, channel));
  if (left.length === 0) {
    await store.srem(K.channels(provider), channel);
    await store.del(K.status(provider, channel));
  }
}

async function listSubscriptions(chatId) {
  const items = await store.smembers(K.subs(chatId));
  return items
    .map((s) => {
      const i = s.indexOf(":");
      return { provider: s.slice(0, i), channel: s.slice(i + 1) };
    })
    .sort((a, b) => a.channel.localeCompare(b.channel));
}

// Чат заблокировал бота — убираем все его подписки, чтобы не долбиться впустую
async function dropChat(chatId) {
  for (const s of await listSubscriptions(chatId)) {
    await removeSubscription(chatId, s.provider, s.channel);
  }
}

// ---------------------------------------------------------------------------
// PROVIDERS — логика конкретных сайтов. Новый сайт = новый объект с
// parseInput(), roomUrl(), checkStatus().
// ---------------------------------------------------------------------------

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

function cbAjaxRequest(username) {
  return fetch("https://chaturbate.com/get_edge_hls_url_ajax/", {
    method: "POST",
    headers: {
      "X-Requested-With": "XMLHttpRequest",
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json, text/javascript, */*; q=0.01",
      Origin: "https://chaturbate.com",
      Referer: `https://chaturbate.com/${username}/`,
      "User-Agent": BROWSER_UA,
    },
    body: `room_slug=${encodeURIComponent(username)}`,
    signal: AbortSignal.timeout(10000),
  });
}

function cbPageRequest(username) {
  return fetch(`https://chaturbate.com/${username}/`, {
    headers: { "User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9" },
    signal: AbortSignal.timeout(10000),
  });
}

const PROVIDERS = {
  chaturbate: {
    name: "Chaturbate",

    parseInput(raw) {
      const text = String(raw).trim();
      const m = text.match(/chaturbate\.com\/([a-zA-Z0-9_\-.]+)/i);
      return (m ? m[1] : text).replace(/^\/+|\/+$/g, "").toLowerCase();
    },

    roomUrl(username) {
      return `https://chaturbate.com/${username}/`;
    },

    // "online" | "offline" | null (не удалось определить)
    async checkStatus(username) {
      // Метод 1: AJAX-эндпоинт плеера — компактный JSON с room_status
      try {
        const res = await cbAjaxRequest(username);
        if (res.ok) {
          const data = await res.json();
          if (data.success === false) return "offline";
          const st = String(data.room_status || "").toLowerCase();
          if (st) return st === "offline" ? "offline" : "online";
        }
      } catch {
        // упадём на метод 2
      }
      // Метод 2: разбор HTML страницы комнаты
      try {
        const res = await cbPageRequest(username);
        if (res.status === 404) return "offline";
        if (res.ok) {
          const html = await res.text();
          const m = html.match(/"room_status"\s*:\s*"([a-z_]+)"/i);
          if (m) return m[1].toLowerCase() === "offline" ? "offline" : "online";
          if (/room is currently offline/i.test(html)) return "offline";
          if (/isn.t online/i.test(html)) return "offline";
        }
      } catch {
        // ничего не поделать
      }
      return null;
    },
  },

  // stripchat: { name: "Stripchat", parseInput(raw) {...}, roomUrl(c) {...}, async checkStatus(c) {...} },
};

const DEFAULT_PROVIDER = "chaturbate";

const BOT_COMMANDS = [
  { command: "start", description: "Открыть меню бота" },
  { command: "watch", description: "Следить за каналом: /watch <ссылка>" },
  { command: "unwatch", description: "Перестать следить: /unwatch <username>" },
  { command: "list", description: "Список каналов и их статусы" },
];

// ---------------------------------------------------------------------------
// TELEGRAM
// ---------------------------------------------------------------------------

async function tg(method, payload) {
  const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

async function sendMessage(chatId, text, extra = {}) {
  let r;
  try {
    r = await tg("sendMessage", { chat_id: chatId, text, ...extra });
  } catch (e) {
    console.error("sendMessage failed:", e.message);
    return { ok: false };
  }
  if (!r.ok && r.error_code === 403) await dropChat(chatId).catch(() => {});
  return r;
}

const answerCallback = (id, text) =>
  tg("answerCallbackQuery", { callback_query_id: id, text, show_alert: false }).catch(() => {});

function mainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "➕ Добавить канал", callback_data: "menu:add" }],
      [{ text: "📋 Мои подписки", callback_data: "menu:list" }],
    ],
  };
}

// ---------------------------------------------------------------------------
// ACTIONS
// ---------------------------------------------------------------------------

const statusEmoji = (s) => (s === "online" ? "🟢" : s === "offline" ? "⚪" : "❔");

async function doWatch(chatId, argText) {
  const provider = DEFAULT_PROVIDER;
  const p = PROVIDERS[provider];
  const channel = p.parseInput(argText);
  if (!channel) return sendMessage(chatId, "Не смог распознать username.");

  await addSubscription(chatId, provider, channel);
  const status = await p.checkStatus(channel);
  if (status) await store.set(K.status(provider, channel), status);

  const statusText =
    status === "online" ? "🟢 в эфире" : status === "offline" ? "⚪ офлайн" : "не удалось определить (проверю по расписанию)";
  return sendMessage(
    chatId,
    `Слежу за ${p.name}: ${channel}\nТекущий статус: ${statusText}\nПришлю сообщение, когда начнётся трансляция.`,
    { reply_markup: mainMenuKeyboard() }
  );
}

async function sendList(chatId, { refresh = false } = {}) {
  const subs = await listSubscriptions(chatId);
  if (subs.length === 0) {
    return sendMessage(chatId, "Пока нет отслеживаемых каналов.", { reply_markup: mainMenuKeyboard() });
  }

  const rows = [];
  for (const s of subs) {
    const provider = PROVIDERS[s.provider];
    if (!provider) continue;
    let status;
    if (refresh) {
      status = await provider.checkStatus(s.channel).catch(() => null);
      if (status) await store.set(K.status(s.provider, s.channel), status);
    } else {
      status = await store.get(K.status(s.provider, s.channel));
    }
    rows.push([
      { text: `${statusEmoji(status)} [${provider.name}] ${s.channel}`, callback_data: `unwatch:${s.provider}:${s.channel}` },
    ]);
  }
  rows.push([
    { text: "🔄 Обновить статусы", callback_data: "menu:listrefresh" },
    { text: "➕ Добавить", callback_data: "menu:add" },
  ]);

  return sendMessage(
    chatId,
    "Твои подписки (🟢 в эфире / ⚪ офлайн / ❔ ещё не проверено). Нажми на канал, чтобы отписаться:",
    { reply_markup: { inline_keyboard: rows } }
  );
}

function sendStartMenu(chatId) {
  return sendMessage(
    chatId,
    "Привет! Я слежу за трансляциями и пришлю уведомление, когда стрим начнётся.\n\n" +
      `Проверка идёт раз в ${Math.round(CHECK_INTERVAL_SEC / 60 * 10) / 10} мин. Сейчас поддерживается только Chaturbate.`,
    { reply_markup: mainMenuKeyboard() }
  );
}

// ---------------------------------------------------------------------------
// ОБРАБОТКА ОБНОВЛЕНИЙ
// ---------------------------------------------------------------------------

async function handleCallbackQuery(cq) {
  const chatId = cq.message.chat.id;
  const data = cq.data || "";

  if (data === "menu:add") {
    await store.set(K.state(chatId), "awaiting_watch", 600);
    await answerCallback(cq.id);
    return sendMessage(chatId, "Пришли ссылку или username Chaturbate-канала.");
  }
  if (data === "menu:list") {
    await answerCallback(cq.id);
    return sendList(chatId);
  }
  if (data === "menu:listrefresh") {
    await answerCallback(cq.id, "Проверяю...");
    return sendList(chatId, { refresh: true });
  }
  if (data.startsWith("unwatch:")) {
    const [, provider, channel] = data.split(":");
    await removeSubscription(chatId, provider, channel);
    await answerCallback(cq.id, `Отписан от ${channel}`);
    return sendList(chatId);
  }
  return answerCallback(cq.id);
}

async function handleText(chatId, text) {
  const [cmdRaw, ...rest] = text.split(/\s+/);
  const cmd = cmdRaw.startsWith("/") ? cmdRaw.split("@")[0] : null;
  const arg = rest.join(" ");

  if (cmd) {
    await store.del(K.state(chatId));
    switch (cmd) {
      case "/start":
        return sendStartMenu(chatId);
      case "/watch":
        return arg ? doWatch(chatId, arg) : sendMessage(chatId, "Использование: /watch <ссылка или username>");
      case "/unwatch": {
        if (!arg) return sendMessage(chatId, "Использование: /unwatch <username>");
        const channel = PROVIDERS[DEFAULT_PROVIDER].parseInput(arg);
        await removeSubscription(chatId, DEFAULT_PROVIDER, channel);
        return sendMessage(chatId, `Больше не слежу за ${channel}.`);
      }
      case "/list":
        return sendList(chatId);
      default:
        return sendMessage(chatId, "Не знаю такую команду.", { reply_markup: mainMenuKeyboard() });
    }
  }

  if ((await store.get(K.state(chatId))) === "awaiting_watch") {
    await store.del(K.state(chatId));
    return doWatch(chatId, text);
  }
  return sendMessage(chatId, "Используй кнопки ниже или команду /start.", { reply_markup: mainMenuKeyboard() });
}

async function handleUpdate(update) {
  if (update.callback_query) return handleCallbackQuery(update.callback_query);
  const msg = update.message;
  if (!msg || !msg.text) return;
  return handleText(msg.chat.id, msg.text.trim());
}

// ---------------------------------------------------------------------------
// LONG POLLING
// ---------------------------------------------------------------------------

async function pollUpdates() {
  let offset = 0;
  for (;;) {
    try {
      const r = await tg("getUpdates", {
        offset,
        timeout: 50,
        allowed_updates: ["message", "callback_query"],
      });
      if (!r.ok) {
        // 409 — параллельно работает другой инстанс (например, при редеплое); просто ждём
        console.error("getUpdates error:", r.error_code, r.description);
        await sleep(r.error_code === 409 ? 3000 : 5000);
        continue;
      }
      for (const u of r.result) {
        offset = u.update_id + 1;
        handleUpdate(u).catch((e) => console.error("handleUpdate error:", e));
      }
    } catch (e) {
      console.error("getUpdates failed:", e.message);
      await sleep(5000);
    }
  }
}

// ---------------------------------------------------------------------------
// ПЕРИОДИЧЕСКАЯ ПРОВЕРКА СТАТУСОВ
// ---------------------------------------------------------------------------

let pollingNow = false;

async function checkOneChannel(providerKey, channel) {
  const provider = PROVIDERS[providerKey];
  const newStatus = await provider.checkStatus(channel).catch(() => null);
  if (!newStatus) return; // не смогли определить — пропускаем цикл

  const key = K.status(providerKey, channel);
  const oldStatus = await store.get(key);
  if (newStatus === oldStatus) return;

  await store.set(key, newStatus);
  if (newStatus === "online" && oldStatus === "offline") {
    const watchers = await store.smembers(K.watchers(providerKey, channel));
    const url = provider.roomUrl(channel);
    await Promise.all(watchers.map((id) => sendMessage(id, `🔴 ${channel} в эфире!\n${url}`)));
  }
}

async function pollAll() {
  if (pollingNow) return; // предыдущий обход ещё не закончился
  pollingNow = true;
  try {
    for (const providerKey of Object.keys(PROVIDERS)) {
      const channels = await store.smembers(K.channels(providerKey));
      for (const channel of channels) {
        await checkOneChannel(providerKey, channel);
        await sleep(700); // не долбим сайт пачкой запросов
      }
    }
  } finally {
    pollingNow = false;
  }
}

// ---------------------------------------------------------------------------
// HTTP: health-check + диагностика
// ---------------------------------------------------------------------------

async function debugChaturbate(user) {
  const out = {};
  try {
    const r = await cbAjaxRequest(user);
    out.ajax_status = r.status;
    out.ajax_body = (await r.text()).slice(0, 300);
  } catch (e) {
    out.ajax_error = String(e);
  }
  try {
    const r = await cbPageRequest(user);
    out.html_status = r.status;
    const t = await r.text();
    out.html_has_room_status = /"room_status"/.test(t);
    out.html_snippet = t.slice(0, 200);
  } catch (e) {
    out.html_error = String(e);
  }
  out.resolved = await PROVIDERS.chaturbate.checkStatus(user);
  return out;
}

function startHttpServer() {
  http
    .createServer(async (req, res) => {
      const url = new URL(req.url, "http://localhost");
      if (url.pathname === "/debug" && DEBUG_KEY && url.searchParams.get("key") === DEBUG_KEY) {
        const user = url.searchParams.get("user");
        if (!user) {
          res.writeHead(400).end("need ?user=");
          return;
        }
        const result = await debugChaturbate(user);
        res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify(result, null, 2));
        return;
      }
      res.writeHead(200).end("StreamPingBot is running");
    })
    .listen(PORT, () => console.log(`HTTP на порту ${PORT}`));
}

// ---------------------------------------------------------------------------
// ЗАПУСК
// ---------------------------------------------------------------------------

async function main() {
  if (!BOT_TOKEN) {
    console.error("BOT_TOKEN не задан");
    process.exit(1);
  }
  store = await createStore();
  startHttpServer();

  // Старый вебхук (от воркера) мешает getUpdates — снимаем
  console.log("deleteWebhook:", JSON.stringify(await tg("deleteWebhook", { drop_pending_updates: false })));
  console.log("setMyCommands:", JSON.stringify(await tg("setMyCommands", { commands: BOT_COMMANDS })));

  setInterval(() => pollAll().catch((e) => console.error("pollAll error:", e)), CHECK_INTERVAL_SEC * 1000);
  setTimeout(() => pollAll().catch((e) => console.error("pollAll error:", e)), 5000);

  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));

  console.log("StreamPingBot запущен");
  pollUpdates();
}

if (require.main === module) {
  main().catch((e) => {
    console.error("fatal:", e);
    process.exit(1);
  });
} else {
  // для локальных тестов
  module.exports = {
    handleUpdate,
    pollAll,
    PROVIDERS,
    init: async () => {
      store = await createStore();
    },
  };
}
