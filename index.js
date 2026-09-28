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
 *  DEBUG_KEY           необязательно — включает GET /debug?key=...&site=...&user=... (диагностика запроса к сайту)
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
    async keys() { return [...sets.keys(), ...strings.keys()]; },
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
  console.log("Redis подключён:", REDIS_URL.replace(/\/\/[^@]*@/, "//***@"));
  return {
    sadd: (k, v) => client.sAdd(k, v),
    srem: (k, v) => client.sRem(k, v),
    smembers: (k) => client.sMembers(k),
    get: (k) => client.get(k),
    set: (k, v, ttlSec) => (ttlSec ? client.set(k, v, { EX: ttlSec }) : client.set(k, v)),
    del: (k) => client.del(k),
    keys: async () => { const out = []; for await (const k of client.scanIterator({ MATCH: "*", COUNT: 200 })) out.push(k); return out; },
  };
}

const K = {
  watchers: (p, c) => `watchers:${p}:${c}`, // set chatId — кто следит за каналом
  subs: (chat) => `subs:${chat}`,           // set "provider:channel" — подписки чата
  channels: (p) => `channels:${p}`,         // set channel — все отслеживаемые каналы провайдера
  status: (p, c) => `status:${p}:${c}`,     // "online" | "offline"
  state: (chat) => `state:${chat}`,         // "awaiting_watch"
  pending: (chat) => `pending:${chat}`,     // username, для которого ждём выбор сайта
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
// PROVIDERS — логика конкретных сайтов.
//
// Чтобы добавить сайт, допиши объект в PROVIDERS. Поля:
//   name            — название для сообщений
//   aliases         — как сайт можно назвать в /watch (`/watch stripchat nick`)
//   hosts           — домены, по которым ссылка узнаётся как ссылка этого сайта
//   parseUrl(url)   — достаёт username из объекта URL (или null)
//   roomUrl(nick)   — ссылка на комнату
//   checkStatus(nick) — "online" | "offline" | null (не удалось определить)
//   caseSensitive   — true, если регистр ника важен (иначе ник приводится к lower-case)
//   beta            — true, если проверка статуса ещё не проверена на реальном сайте
// Всё остальное (подписки, список, уведомления) общее.
// ---------------------------------------------------------------------------

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

const USERNAME_RE = /^[a-zA-Z0-9_\-.]{2,50}$/;

function httpGet(url, headers = {}) {
  return fetch(url, {
    headers: { "User-Agent": BROWSER_UA, Accept: "application/json, text/plain, */*", ...headers },
    signal: AbortSignal.timeout(10000),
  });
}

// Первый сегмент пути как username, кроме служебных разделов сайта
function firstPathSegment(url, reserved) {
  const seg = url.pathname.split("/").filter(Boolean)[0];
  if (!seg) return null;
  let s;
  try { s = decodeURIComponent(seg); } catch { return null; }
  if (reserved.has(s.toLowerCase())) return null;
  return USERNAME_RE.test(s) ? s : null;
}

// --- Chaturbate ---------------------------------------------------------------

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
  return httpGet(`https://chaturbate.com/${username}/`, { "Accept-Language": "en-US,en;q=0.9" });
}

const CB_RESERVED = new Set([
  "in", "b", "tag", "tags", "female-cams", "male-cams", "couple-cams", "trans-cams", "followed-cams",
  "auth", "accounts", "supporter", "tipping", "api", "affiliates", "contest", "security", "terms",
]);

// --- Провайдеры ---------------------------------------------------------------

const PROVIDERS = {
  chaturbate: {
    name: "Chaturbate",
    aliases: ["cb", "chaturbate"],
    hosts: ["chaturbate.com", "chaturbate.global", "chaturbate.eu"],

    parseUrl(url) {
      // партнёрские ссылки вида /in/?tour=...&room=username
      const room = url.searchParams.get("room");
      if (room && USERNAME_RE.test(room)) return room;
      return firstPathSegment(url, CB_RESERVED);
    },

    roomUrl: (u) => `https://chaturbate.com/${encodeURIComponent(u)}/`,

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

    async debug(user) {
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
      return out;
    },
  },

  stripchat: {
    name: "Stripchat",
    aliases: ["sc", "stripchat", "xhamsterlive"],
    hosts: ["stripchat.com", "stripchat.global", "stripchat.xxx", "xhamsterlive.com"],
    caseSensitive: true,

    parseUrl: (url) =>
      firstPathSegment(
        url,
        new Set(["girls", "couples", "men", "trans", "favorites", "login", "signup", "api", "tags", "cams", "girl", "guys", "models", "search", "blog", "en", "ru", "de", "es", "fr", "it"])
      ),

    roomUrl: (u) => `https://stripchat.com/${encodeURIComponent(u)}`,

    request: (u) =>
      httpGet(`https://stripchat.com/api/front/v2/models/username/${encodeURIComponent(u)}/cam`, {
        Referer: `https://stripchat.com/${encodeURIComponent(u)}`,
        "Accept-Language": "en-US,en;q=0.9",
      }),

    async checkStatus(u) {
      let res;
      try {
        res = await this.request(u);
      } catch (e) {
        console.warn(`[stripchat] ${u}: запрос не удался: ${e.message}`);
        return null;
      }
      if (res.status === 404) return "offline";
      if (!res.ok) {
        console.warn(`[stripchat] ${u}: HTTP ${res.status} (403/429 или HTML — вероятно, блок по IP)`);
        return null;
      }
      let data;
      try {
        data = await res.json();
      } catch {
        console.warn(`[stripchat] ${u}: ответ не JSON (скорее всего страница-заглушка антибота)`);
        return null;
      }
      const user = data?.user?.user ?? data?.user ?? {};
      const live = user.isLive ?? data?.user?.isLive ?? data?.cam?.isLive;
      if (typeof live === "boolean") return live ? "online" : "offline";
      const st = String(user.status || "").toLowerCase();
      if (st) return ["off", "offline", "idle"].includes(st) ? "offline" : "online";
      console.warn(`[stripchat] ${u}: неизвестная структура ответа, ключи: ${Object.keys(data || {}).join(",")}`);
      return null;
    },
  },

  cam4: {
    name: "Cam4",
    aliases: ["cam4"],
    hosts: ["cam4.com", "cam4.co.uk", "cam4.de", "cam4.es", "cam4.fr", "cam4.it", "cam4.eu"],
    beta: true,

    parseUrl: (url) =>
      firstPathSegment(url, new Set(["female", "male", "couple", "trans", "featured", "login", "signup", "tags", "search", "cams", "rest"])),

    roomUrl: (u) => `https://www.cam4.com/${encodeURIComponent(u)}`,

    request: (u) => httpGet(`https://www.cam4.com/rest/v1.0/profile/${encodeURIComponent(u)}/streamInfo`),

    // Консервативно: online только при явных признаках стрима, offline — при 204/404
    async checkStatus(u) {
      try {
        const res = await this.request(u);
        if (res.status === 204 || res.status === 404) return "offline";
        if (!res.ok) return null;
        const data = await res.json().catch(() => null);
        if (data && (data.cdnURL || data.edgeURL || data.hlsPreviewUrl || data.canUseCDN)) return "online";
      } catch {
        // не смогли определить
      }
      return null;
    },
  },

  camsoda: {
    name: "CamSoda",
    aliases: ["cs", "camsoda"],
    hosts: ["camsoda.com"],
    beta: true,

    parseUrl: (url) =>
      firstPathSegment(url, new Set(["browse", "login", "signup", "tags", "categories", "search", "api", "tips"])),

    roomUrl: (u) => `https://www.camsoda.com/${encodeURIComponent(u)}`,

    request: (u) => httpGet(`https://www.camsoda.com/api/v1/chat/react/${encodeURIComponent(u)}`),

    // Консервативно: online только если в ответе есть данные потока
    async checkStatus(u) {
      try {
        const res = await this.request(u);
        if (res.status === 404) return "offline";
        if (!res.ok) return null;
        const data = await res.json();
        const s = data?.stream;
        if (s && (s.token || s.stream_name || (Array.isArray(s.edge_servers) && s.edge_servers.length))) return "online";
        if (data?.user) return "offline";
      } catch {
        // не смогли определить
      }
      return null;
    },
  },
};

// Общий debug для провайдеров без своего debug()
async function debugProvider(key, user) {
  const p = PROVIDERS[key];
  const out = { site: key, user };
  if (p.debug) Object.assign(out, await p.debug(user));
  else if (p.request) {
    try {
      const r = await p.request(user);
      out.status = r.status;
      out.body = (await r.text()).slice(0, 400);
    } catch (e) {
      out.error = String(e);
    }
  }
  out.resolved = await p.checkStatus(user);
  return out;
}

// ---------------------------------------------------------------------------
// РАЗБОР ВВОДА: ссылка / "сайт ник" / просто ник
// ---------------------------------------------------------------------------

const providerKeys = () => Object.keys(PROVIDERS);

function findProviderByAlias(word) {
  const w = String(word).toLowerCase();
  return providerKeys().find((k) => k === w || PROVIDERS[k].aliases.includes(w)) || null;
}

function findProviderByHost(host) {
  const h = host.toLowerCase().replace(/^www\./, "");
  return providerKeys().find((k) => PROVIDERS[k].hosts.some((d) => h === d || h.endsWith("." + d))) || null;
}

function normalizeChannel(key, nick) {
  return PROVIDERS[key].caseSensitive ? nick : nick.toLowerCase();
}

function tryParseUrl(token) {
  const hasScheme = /^https?:\/\//i.test(token);
  const looksLikeHost = /^([a-z0-9-]+\.)+[a-z]{2,}(\/|\?|#|$)/i.test(token);
  if (!hasScheme && !looksLikeHost) return null;
  try {
    return new URL(hasScheme ? token : "https://" + token);
  } catch {
    return null;
  }
}

/**
 * Результат:
 *  { type: "ok", provider, channel }   — сайт и ник определены
 *  { type: "pick", username }          — это просто ник, нужно спросить сайт
 *  { type: "unsupported", host }       — ссылка на неизвестный сайт
 *  { type: "invalid" }                 — не похоже ни на что
 */
function resolveInput(raw) {
  const tokens = String(raw).trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return { type: "invalid" };

  let forced = null;
  if (tokens.length === 2) {
    forced = findProviderByAlias(tokens[0]);
    if (!forced) return { type: "invalid" };
    tokens.shift();
  } else if (tokens.length > 2) {
    return { type: "invalid" };
  }

  const token = tokens[0].replace(/^@/, "");
  const url = tryParseUrl(token);

  if (url) {
    const key = findProviderByHost(url.hostname);
    if (key) {
      if (forced && forced !== key) return { type: "invalid" };
      const nick = PROVIDERS[key].parseUrl(url);
      return nick ? { type: "ok", provider: key, channel: normalizeChannel(key, nick) } : { type: "invalid" };
    }
    if (/^https?:\/\//i.test(token) || token.includes("/")) {
      return { type: "unsupported", host: url.hostname.replace(/^www\./, "") };
    }
    // иначе это просто ник с точкой (например, john.doe) — идём дальше
  }

  if (!USERNAME_RE.test(token)) return { type: "invalid" };
  if (forced) return { type: "ok", provider: forced, channel: normalizeChannel(forced, token) };
  return { type: "pick", username: token };
}

const supportedSitesText = () =>
  providerKeys()
    .map((k) => `• ${PROVIDERS[k].name}${PROVIDERS[k].beta ? " (β)" : ""}`)
    .join("\n");

const BOT_COMMANDS = [
  { command: "start", description: "Открыть меню бота" },
  { command: "watch", description: "Следить за каналом: /watch <ссылка>" },
  { command: "unwatch", description: "Перестать следить (без аргумента — выбрать из списка)" },
  { command: "list", description: "Список каналов и их статусы" },
  { command: "sites", description: "Поддерживаемые сайты" },
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

async function doWatch(chatId, providerKey, channel) {
  const p = PROVIDERS[providerKey];
  await addSubscription(chatId, providerKey, channel);
  const status = await p.checkStatus(channel).catch(() => null);
  if (status) await store.set(K.status(providerKey, channel), status);

  const statusText =
    status === "online" ? "🟢 в эфире" : status === "offline" ? "⚪ офлайн" : "не удалось определить (проверю по расписанию)";
  return sendMessage(
    chatId,
    `Слежу за ${p.name}: ${channel}\nТекущий статус: ${statusText}\nПришлю сообщение, когда начнётся трансляция.` +
      (p.beta ? `\n\n⚠️ Поддержка ${p.name} экспериментальная — статус может определяться неточно.` : ""),
    { reply_markup: mainMenuKeyboard() }
  );
}

// Единая точка входа для «пользователь прислал ссылку/ник»
async function handleWatchInput(chatId, raw) {
  const r = resolveInput(raw);
  switch (r.type) {
    case "ok":
      await store.del(K.pending(chatId));
      return doWatch(chatId, r.provider, r.channel);
    case "pick": {
      await store.set(K.pending(chatId), r.username, 600);
      const buttons = providerKeys().map((k) => ({ text: PROVIDERS[k].name, callback_data: `pick:${k}` }));
      const rows = [];
      for (let i = 0; i < buttons.length; i += 2) rows.push(buttons.slice(i, i + 2));
      return sendMessage(chatId, `На каком сайте «${r.username}»?`, { reply_markup: { inline_keyboard: rows } });
    }
    case "unsupported":
      return sendMessage(chatId, `Сайт ${r.host} пока не поддерживается.\n\nПоддерживаются:\n${supportedSitesText()}`);
    default:
      return sendMessage(
        chatId,
        "Не смог распознать. Пришли ссылку на канал или username.\n" +
          "Можно указать сайт явно: /watch stripchat username\n\nПоддерживаются:\n" +
          supportedSitesText()
      );
  }
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
    // url-кнопка: по нажатию Telegram сразу открывает комнату
    rows.push([{ text: `${statusEmoji(status)} [${provider.name}] ${s.channel}`, url: provider.roomUrl(s.channel) }]);
  }
  rows.push([
    { text: "🔄 Обновить статусы", callback_data: "menu:listrefresh" },
    { text: "➕ Добавить", callback_data: "menu:add" },
  ]);

  return sendMessage(
    chatId,
    "Твои подписки (🟢 в эфире / ⚪ офлайн / ❔ ещё не проверено). Нажми на канал — откроется комната.\nОтписаться: /unwatch",
    { reply_markup: { inline_keyboard: rows } }
  );
}

// Клавиатура для отписки: одна кнопка на подписку
function unwatchKeyboard(subs) {
  return {
    inline_keyboard: subs
      .filter((s) => PROVIDERS[s.provider])
      .map((s) => [{ text: `❌ [${PROVIDERS[s.provider].name}] ${s.channel}`, callback_data: `unwatch:${s.provider}:${s.channel}` }]),
  };
}

async function sendUnwatchMenu(chatId, subs = null) {
  subs = subs || (await listSubscriptions(chatId));
  if (subs.length === 0) return sendMessage(chatId, "Ты ни за кем не следишь.");
  return sendMessage(chatId, "Выбери, от кого отписаться:", { reply_markup: unwatchKeyboard(subs) });
}

async function doUnwatch(chatId, raw) {
  const r = resolveInput(raw);
  if (r.type !== "ok" && r.type !== "pick") {
    return sendMessage(chatId, "Использование: /unwatch <ссылка или username> (или просто /unwatch — выбрать из списка)");
  }
  const username = r.type === "ok" ? r.channel : r.username;
  const forced = r.type === "ok" ? r.provider : null;

  // Ищем среди РЕАЛЬНЫХ подписок чата без учёта регистра: у Stripchat ник регистрозависимый,
  // и удалять нужно ровно ту строку, что лежит в Redis.
  const subs = (await listSubscriptions(chatId)).filter(
    (s) => (!forced || s.provider === forced) && s.channel.toLowerCase() === username.toLowerCase()
  );
  if (subs.length === 0) return sendMessage(chatId, `«${username}» нет в твоих подписках. Смотри /list.`);
  if (subs.length > 1) return sendUnwatchMenu(chatId, subs); // один ник на нескольких сайтах — пусть выберет

  const { provider, channel } = subs[0];
  await removeSubscription(chatId, provider, channel);
  return sendMessage(chatId, `Больше не слежу за ${PROVIDERS[provider]?.name || provider}: ${channel}.`);
}

function sendStartMenu(chatId) {
  return sendMessage(
    chatId,
    "Привет! Я слежу за трансляциями и пришлю уведомление, когда стрим начнётся.\n\n" +
      `Проверка идёт раз в ${Math.round((CHECK_INTERVAL_SEC / 60) * 10) / 10} мин.\n\n` +
      `Поддерживаемые сайты:\n${supportedSitesText()}\n\n` +
      "Просто пришли ссылку на канал — сайт определю сам.",
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
    return sendMessage(
      chatId,
      "Пришли ссылку на канал или username.\nЕсли пришлёшь только username — спрошу, на каком он сайте.\n\n" +
        `Поддерживаются:\n${supportedSitesText()}`
    );
  }
  if (data === "menu:list") {
    await answerCallback(cq.id);
    return sendList(chatId);
  }
  if (data === "menu:listrefresh") {
    await answerCallback(cq.id, "Проверяю...");
    return sendList(chatId, { refresh: true });
  }
  if (data.startsWith("pick:")) {
    const providerKey = data.slice(5);
    const username = await store.get(K.pending(chatId));
    if (!PROVIDERS[providerKey]) return answerCallback(cq.id);
    if (!username) {
      await answerCallback(cq.id, "Время вышло, пришли ссылку ещё раз");
      return;
    }
    await store.del(K.pending(chatId));
    await answerCallback(cq.id);
    return doWatch(chatId, providerKey, normalizeChannel(providerKey, username));
  }
  if (data.startsWith("unwatch:")) {
    const [, provider, channel] = data.split(":");
    const before = (await listSubscriptions(chatId)).some((x) => x.provider === provider && x.channel === channel);
    await removeSubscription(chatId, provider, channel);
    const after = (await listSubscriptions(chatId)).some((x) => x.provider === provider && x.channel === channel);
    console.log(`unwatch button chat=${chatId} ${provider}:${channel} была=${before} осталась=${after}`);
    await answerCallback(cq.id, `Отписан от ${channel}`);
    // обновляем то же сообщение: убираем нажатую кнопку
    const subs = await listSubscriptions(chatId);
    return tg("editMessageText", {
      chat_id: chatId,
      message_id: cq.message.message_id,
      text: subs.length ? "Выбери, от кого отписаться:" : "Подписок не осталось.",
      reply_markup: unwatchKeyboard(subs),
    }).catch(() => {});
  }
  return answerCallback(cq.id);
}

async function handleText(chatId, text) {
  const [cmdRaw, ...rest] = text.split(/\s+/);
  const cmd = cmdRaw.startsWith("/") ? cmdRaw.split("@")[0] : null;
  const arg = rest.join(" ");

  if (cmd) {
    await store.del(K.state(chatId));
    await store.del(K.pending(chatId));
    switch (cmd) {
      case "/start":
        return sendStartMenu(chatId);
      case "/watch":
        return arg ? handleWatchInput(chatId, arg) : sendMessage(chatId, "Использование: /watch <ссылка или username>\nИли: /watch stripchat username");
      case "/unwatch":
        return arg ? doUnwatch(chatId, arg) : sendUnwatchMenu(chatId);
      case "/list":
        return sendList(chatId);
      case "/dump": {
        if (!DEBUG_KEY || arg !== DEBUG_KEY) return sendMessage(chatId, "Нужно: /dump <DEBUG_KEY>");
        const keys = (await store.keys()).sort();
        const mine = await store.smembers(K.subs(chatId));
        return sendMessage(
          chatId,
          `chat id: ${chatId}\nхранилище: ${REDIS_URL ? "Redis" : "ПАМЯТЬ (REDIS_URL не задан!)"}\n\n` +
            `subs:${chatId}:\n${mine.map((x) => JSON.stringify(x)).join("\n") || "(пусто)"}\n\n` +
            `Все ключи (${keys.length}):\n${keys.slice(0, 60).join("\n")}`.slice(0, 3500)
        );
      }
      case "/sites":
        return sendMessage(chatId, `Поддерживаемые сайты:\n${supportedSitesText()}\n\n(β — экспериментальная поддержка)`);
      default:
        return sendMessage(chatId, "Не знаю такую команду.", { reply_markup: mainMenuKeyboard() });
    }
  }

  if ((await store.get(K.state(chatId))) === "awaiting_watch") {
    await store.del(K.state(chatId));
    return handleWatchInput(chatId, text);
  }
  // Ссылка на известный сайт без команды — тоже считаем запросом на слежение
  if (resolveInput(text).type === "ok") return handleWatchInput(chatId, text);

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

function startHttpServer() {
  http
    .createServer(async (req, res) => {
      const url = new URL(req.url, "http://localhost");
      if (url.pathname === "/debug" && DEBUG_KEY && url.searchParams.get("key") === DEBUG_KEY) {
        const user = url.searchParams.get("user");
        const site = url.searchParams.get("site") || "chaturbate";
        if (!user) {
          res.writeHead(400).end("need ?user= (and optionally &site=" + providerKeys().join("|") + ")");
          return;
        }
        if (!PROVIDERS[site]) {
          res.writeHead(400).end("unknown site, use one of: " + providerKeys().join(", "));
          return;
        }
        const result = await debugProvider(site, user);
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
    resolveInput,
    init: async () => {
      store = await createStore();
    },
  };
}
