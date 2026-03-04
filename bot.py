import os
import time
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

import requests
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
# основной «человеческий» смайл, который показываем в текстах
TIMER_EMOJI = "⌛️"

# список всех смайлов, которые для бота считаются «таймером»
TIMER_EMOJIS = {TIMER_EMOJI, "⌛", "⏳", "🤡", "🔥"}  # сюда можно добавить любые ещё

if not BOT_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


# ---------- База данных ----------

DB_PATH = "/data/reminder_bot.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            title TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            date INTEGER NOT NULL,
            has_timer_reaction INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, message_id)
        )
        """
    )
    conn.commit()
    conn.close()


def db_connect():
    return sqlite3.connect(DB_PATH)


def upsert_channel(chat_id, username, title):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO channels(chat_id, username, title)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            username = excluded.username,
            title = excluded.title
        """,
        (chat_id, username, title),
    )
    conn.commit()
    conn.close()


def insert_message(chat_id, message_id, date_ts):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO messages(chat_id, message_id, date)
        VALUES (?, ?, ?)
        """,
        (chat_id, message_id, date_ts),
    )
    conn.commit()
    conn.close()


def set_timer_reaction(chat_id, message_id, has_reaction: bool, date_ts: int | None = None):
    """
    Обновляет флаг has_timer_reaction.
    Если запись для (chat_id, message_id) ещё не существует — создаёт её.
    """
    conn = db_connect()
    cur = conn.cursor()

    # Сначала пытаемся обновить существующую запись
    cur.execute(
        """
        UPDATE messages
        SET has_timer_reaction = ?
        WHERE chat_id = ? AND message_id = ?
        """,
        (1 if has_reaction else 0, chat_id, message_id),
    )

    # Если ничего не обновили — создаём запись
    if cur.rowcount == 0:
        if date_ts is None:
            # если Telegram не прислал дату — берём текущее время
            date_ts = int(datetime.now(ZoneInfo(TIMEZONE_NAME)).timestamp())
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, date, has_timer_reaction)
            VALUES (?, ?, ?, ?)
            """,
            (chat_id, message_id, date_ts, 1 if has_reaction else 0),
        )

    conn.commit()
    conn.close()


def get_channels():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, username, title FROM channels")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_messages_with_timer_last_days(chat_id, days: int):
    now_ts = int(datetime.now(ZoneInfo(TIMEZONE_NAME)).timestamp())
    from_ts = now_ts - days * 24 * 3600
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id, date
        FROM messages
        WHERE chat_id = ? AND date >= ? AND has_timer_reaction = 1
        ORDER BY date
        """,
        (chat_id, from_ts),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_messages_with_timer_since(from_ts: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.chat_id, m.message_id, m.date, c.username, c.title
        FROM messages m
        JOIN channels c ON c.chat_id = m.chat_id
        WHERE m.date >= ? AND m.has_timer_reaction = 1
        ORDER BY m.date
        """,
        (from_ts,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_messages_with_timer_since_for_chat(chat_id: int, from_ts: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id, date
        FROM messages
        WHERE chat_id = ? AND date >= ? AND has_timer_reaction = 1
        ORDER BY date
        """,
        (chat_id, from_ts),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------- Вспомогательные функции ----------


def tg_request(method: str, params=None, json=None, timeout=30):
    url = f"{API_URL}/{method}"
    resp = requests.post(url, data=params, json=json, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data["result"]


def build_message_link(chat_id: int, message_id: int, username: str | None) -> str:
    if username:
        return f"https://t.me/{username}/{message_id}"
    # приватный канал: chat_id вида -1001234567890
    internal_id = str(abs(chat_id))
    if internal_id.startswith("100"):
        internal_id = internal_id[3:]
    return f"https://t.me/c/{internal_id}/{message_id}"


def now_moscow():
    return datetime.now(ZoneInfo(TIMEZONE_NAME))


def next_10_msk(from_dt: datetime | None = None) -> datetime:
    if from_dt is None:
        from_dt = now_moscow()
    target = from_dt.replace(hour=10, minute=0, second=0, microsecond=0)
    if target <= from_dt:
        target += timedelta(days=1)
    return target


def chunk_and_send(chat_id, text, parse_mode="Markdown"):
    if len(text) <= 4096:
        tg_request(
            "sendMessage",
            params={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
        )
        return
    # делим по строкам
    lines = text.split("\n")
    buf = ""
    for line in lines:
        line_with_nl = (buf + "\n" + line) if buf else line
        if len(line_with_nl) > 4096:
            tg_request(
                "sendMessage",
                params={"chat_id": chat_id, "text": buf, "parse_mode": parse_mode},
            )
            buf = line
        else:
            buf = line_with_nl
    if buf:
        tg_request(
            "sendMessage",
            params={"chat_id": chat_id, "text": buf, "parse_mode": parse_mode},
        )


def parse_command_and_args(text: str):
    text = text.strip()
    if not text.startswith("/"):
        return None, None
    parts = text.split(maxsplit=1)
    if not parts:
        return None, None
    cmd_raw = parts[0]
    args = parts[1].strip() if len(parts) > 1 else ""
    # отрезаем @botname, если есть
    cmd = cmd_raw.split("@", 1)[0]
    return cmd, args


# ---------- Обработка команд ----------


def handle_all_reminder_command(chat: dict, args: str):
    chat_id = chat["id"]
    chat_type = chat.get("type", "private")

    if not args:
        reply = (
            "Команда в формате:\n"
            "/all_reminder DD.MM.YYYY\n\n"
            "Например:\n"
            "/all_reminder 11.11.2025"
        )
        tg_request(
            "sendMessage",
            params={
                "chat_id": chat_id,
                "text": reply,
            },
        )
        return

    date_str = args.strip()
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        dt = dt.replace(tzinfo=ZoneInfo(TIMEZONE_NAME))
        from_ts = int(dt.timestamp())
    except ValueError:
        tg_request(
            "sendMessage",
            params={
                "chat_id": chat_id,
                "text": "Неверный формат даты. Используйте DD.MM.YYYY, например 11.11.2025",
            },
        )
        return

    if chat_type == "channel":
        # только посты этого канала
        rows = get_messages_with_timer_since_for_chat(chat_id, from_ts)
        if not rows:
            tg_request(
                "sendMessage",
                params={
                    "chat_id": chat_id,
                    "text": f"Постов с реакцией {TIMER_EMOJI} c {date_str} не найдено.",
                },
            )
            return

        username = chat.get("username")
        title = chat.get("title") or "канал"
        header = f"Посты с реакцией {TIMER_EMOJI} в канале {title} c {date_str}:\n\n"
        lines = [header]
        rows_sorted = sorted(rows, key=lambda x: x[1])
        for idx, (message_id, date_ts) in enumerate(rows_sorted, start=1):
            link = build_message_link(chat_id, message_id, username)
            dt_msg = datetime.fromtimestamp(date_ts, tz=ZoneInfo(TIMEZONE_NAME))
            dt_str = dt_msg.strftime("%d.%m.%Y %H:%M")
            line = f"{idx}) [{dt_str}]({link})"
            lines.append(line)
        text = "\n".join(lines)
        chunk_and_send(chat_id, text, parse_mode="Markdown")
    else:
        # личка / группа: агрегируем по всем каналам
        rows = get_messages_with_timer_since(from_ts)
        if not rows:
            tg_request(
                "sendMessage",
                params={
                    "chat_id": chat_id,
                    "text": f"Постов с реакцией {TIMER_EMOJI} c {date_str} не найдено.",
                },
            )
            return

        by_chat = {}
        for ch_id, message_id, date_ts, username, title in rows:
            by_chat.setdefault(
                ch_id, {"username": username, "title": title, "items": []}
            )
            by_chat[ch_id]["items"].append((message_id, date_ts))

        for ch_id, info in by_chat.items():
            username = info["username"]
            title = info["title"]
            items = sorted(info["items"], key=lambda x: x[1])
            header = f"Канал: {title}\nПосты с реакцией {TIMER_EMOJI} c {date_str}:\n\n"
            lines = [header]
            for idx, (message_id, date_ts) in enumerate(items, start=1):
                link = build_message_link(ch_id, message_id, username)
                dt_msg = datetime.fromtimestamp(date_ts, tz=ZoneInfo(TIMEZONE_NAME))
                dt_str = dt_msg.strftime("%d.%m.%Y %H:%М")
                line = f"{idx}) [{dt_str}]({link})"
                lines.append(line)
            text = "\n".join(lines)
            chunk_and_send(chat_id, text, parse_mode="Markdown")


def send_summary_for_channel(chat_id: int, username: str | None = None, title: str | None = None) -> bool:
    rows = get_messages_with_timer_last_days(chat_id, days=7)
    if not rows:
        return False

    # если нет username/title, пробуем взять из БД
    if username is None or title is None:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT username, title FROM channels WHERE chat_id = ?",
            (chat_id,),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            if username is None:
                username = row[0]
            if title is None:
                title = row[1]

    header_title = title or "канале"
    header = (
        f"Сводка постов с реакцией {TIMER_EMOJI} "
        f"за последние 7 дней в {header_title}:\n\n"
    )
    lines = [header]

    rows_sorted = sorted(rows, key=lambda x: x[1])
    for idx, (message_id, _date_ts) in enumerate(rows_sorted, start=1):
        link = build_message_link(chat_id, message_id, username)
        line = f"{idx}) [пост {idx}]({link})"
        lines.append(line)

    text = "\n".join(lines)
    chunk_and_send(chat_id, text, parse_mode="Markdown")
    return True


def handle_run_due_now_command(chat: dict):
    chat_id = chat["id"]
    chat_type = chat.get("type", "private")
    if chat_type == "channel":
        username = chat.get("username")
        title = chat.get("title")
        sent = send_summary_for_channel(chat_id, username, title)
        if not sent:
            tg_request(
                "sendMessage",
                params={
                    "chat_id": chat_id,
                    "text": f"За последнюю неделю нет постов с реакцией {TIMER_EMOJI}.",
                },
            )
    else:
        # во вне канала считаем, что команда запускает рассылку по всем каналам
        send_daily_summary()
        tg_request(
            "sendMessage",
            params={
                "chat_id": chat_id,
                "text": "Сводка за последнюю неделю отправлена во все каналы.",
            },
        )


# ---------- Обработка апдейтов ----------


def handle_channel_post(message: dict):
    chat = message["chat"]
    if chat.get("type") != "channel":
        return
    chat_id = chat["id"]
    username = chat.get("username")
    title = chat.get("title")

    # сохраняем/обновляем канал в БД всегда
    upsert_channel(chat_id, username, title)

    text = message.get("text") or message.get("caption") or ""
    cmd, args = parse_command_and_args(text) if text else (None, None)
    if cmd == "/all_reminder":
        handle_all_reminder_command(chat, args)
        return
    elif cmd == "/run_due_now":
        handle_run_due_now_command(chat)
        return

    # обычный пост канала
    message_id = message["message_id"]
    date_ts = message["date"]  # unix time
    insert_message(chat_id, message_id, date_ts)


def extract_timer_reaction_from_reaction_count(rc: dict) -> tuple[int, int, bool]:
    """
    Возвращает (chat_id, message_id, has_timer_reaction)
    на основе объекта message_reaction_count.
    """
    chat_id = rc["chat"]["id"]
    message_id = rc["message_id"]
    has_timer = False

    for item in rc.get("reactions", []):
        r_type = item.get("type", {})

        # Обычный юникод-эмодзи
        emoji = r_type.get("emoji")
        if emoji and emoji in TIMER_EMOJIS and item.get("total_count", 0) > 0:
            has_timer = True
            break

        # Если когда-нибудь захотите учитывать кастомные эмодзи (premium),
        # сюда можно будет добавить проверку r_type.get("custom_emoji_id").
    return chat_id, message_id, has_timer


def handle_message_reaction_count(update: dict):
    rc = update["message_reaction_count"]

    # Отладка: смотрим, что реально присылает Telegram
    try:
        print("message_reaction_count update:", json.dumps(update, ensure_ascii=False))
    except Exception:
        # на всякий случай, чтобы print не уронил бота
        print("message_reaction_count update (без json.dumps)")

    chat_id, message_id, has_timer = extract_timer_reaction_from_reaction_count(rc)
    date_ts = rc.get("date")
    set_timer_reaction(chat_id, message_id, has_timer, date_ts)


def handle_message(update: dict):
    """
    Личная переписка / группы: разбираем команды /all_reminder и /run_due_now
    """
    message = update["message"]
    chat = message["chat"]
    text = message.get("text", "") or ""
    cmd, args = parse_command_and_args(text) if text else (None, None)
    if cmd == "/all_reminder":
        handle_all_reminder_command(chat, args)
    elif cmd == "/run_due_now":
        handle_run_due_now_command(chat)


def handle_update(update: dict):
    if "channel_post" in update:
        handle_channel_post(update["channel_post"])
    elif "edited_channel_post" in update:
        handle_channel_post(update["edited_channel_post"])
    if "message_reaction_count" in update:
        handle_message_reaction_count(update)
    if "message" in update:
        handle_message(update)


# ---------- Ежедневная сводка ----------


def send_daily_summary():
    channels = get_channels()
    if not channels:
        return
    for chat_id, username, title in channels:
        send_summary_for_channel(chat_id, username, title)


# ---------- Main loop ----------


def main():
    init_db()
    offset = None
    next_summary_time = next_10_msk()

    print("Бот запущен.")
    while True:
        try:
            # проверяем, пора ли слать ежедневную сводку
            now = now_moscow()
            if now >= next_summary_time:
                print(f"Отправляем ежедневную сводку: {now}")
                try:
                    send_daily_summary()
                except Exception as e:
                    print(f"Ошибка при отправке сводки: {e}")
                next_summary_time = next_10_msk(now)

            allowed_updates = [
                "message",
                "channel_post",
                "edited_channel_post",
                "message_reaction",
                "message_reaction_count",
            ]

            params = {
                "timeout": 30,
                # Telegram ждёт JSON-строку с массивом типов апдейтов
                "allowed_updates": json.dumps(allowed_updates),
            }
            if offset is not None:
                params["offset"] = offset

            result = tg_request("getUpdates", params=params, timeout=35)
            for upd in result:
                offset = upd["update_id"] + 1
                try:
                    handle_update(upd)
                except Exception as e:
                    print(f"Ошибка при обработке update {upd.get('update_id')}: {e}")

        except requests.exceptions.RequestException as e:
            print(f"Сетевая ошибка: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
