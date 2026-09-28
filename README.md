# StreamPingBot (Node + Redis)

Один процесс делает всё: принимает сообщения Telegram (long polling), раз в
`CHECK_INTERVAL_SEC` секунд проверяет статусы каналов и шлёт уведомление, когда
стример вышел в эфир. Подписки и статусы лежат в Redis. Cloudflare Worker,
Render-чекер и внешний cron больше не нужны.

## Запуск в Docker

```bash
cp .env.example .env      # впиши BOT_TOKEN (остальное необязательно)
docker compose up -d --build
docker compose logs -f bot
```

Compose поднимает два контейнера: `bot` (Node) и `redis` (с AOF-персистентностью
в volume `redis-data`, подписки переживают перезапуск). `REDIS_URL` задаётся в
`docker-compose.yml` автоматически (`redis://redis:6379`).

Обновление: `git pull && docker compose up -d --build`.
Остановка: `docker compose down` (данные остаются; `-v` — удалить и их).

Переменные (`.env`): `BOT_TOKEN` — токен от @BotFather; `CHECK_INTERVAL_SEC` —
период проверки (по умолчанию 120); `DEBUG_KEY` — включает `/debug`.

Только бот без compose (внешний Redis):

```bash
docker build -t streampingbot .
docker run -d --restart unless-stopped --env-file .env \
  -e REDIS_URL=redis://host:6379 streampingbot
```

При старте бот сам снимает старый вебхук и регистрирует команды в меню.

## Проверка, что Chaturbate пускает с IP хостинга

Если задан `DEBUG_KEY`, открой в браузере
`http://<адрес-сервера>:3000/debug?key=<DEBUG_KEY>&user=<username>`.
`ajax_status: 200` и `resolved: "online"|"offline"` — всё работает.
`403` и HTML вместо JSON — IP хостинга заблокирован (нужен другой IP/прокси).

## Новая платформа

Добавь объект в `PROVIDERS` в `index.js` с `parseInput`, `roomUrl`,
`checkStatus` — остальная логика общая.
