import asyncio
import pytz
import os
from datetime import datetime, time, timedelta
from dotenv import load_dotenv

import asyncpg
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telegram import (
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Update,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

# ---------- настройки ----------
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN не найден в .env файле")

DATABASE_URL         = os.getenv("DATABASE_URL")   # postgres://user:pass@host:5432/dbname
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES               = ["https://www.googleapis.com/auth/calendar.readonly"]
SEARCH_TERM          = "Смена в хп"
TIMEZONE             = pytz.timezone("Europe/Moscow")

MIXES_PER_PAGE = 6

# ---------- FSM состояния ----------
WAITING_FOR_NAME, WAITING_FOR_INGREDIENTS = range(2)

# ---------- Стафф кальян ----------
# В .env пропиши: STAFF_ALLOWED_IDS=123456789,987654321
_raw_ids = os.getenv("STAFF_ALLOWED_IDS", "")
ALLOWED_IDS: list[int] = [int(i) for i in _raw_ids.split(",") if i.strip().isdigit()]
STAFF_PHASE_A = 10
STAFF_PHASE_B = 11

# ---------- PostgreSQL ----------
async def get_db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(DATABASE_URL)

async def init_db(pool: asyncpg.Pool):
    """Создаёт таблицы users и mixes, если их нет."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id         BIGINT PRIMARY KEY,
                username   TEXT,
                first_name TEXT,
                last_name  TEXT,
                date_added TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS mixes (
                id          SERIAL PRIMARY KEY,
                user_id     BIGINT      NOT NULL REFERENCES users(id),
                name        TEXT        NOT NULL,
                ingredients TEXT        NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)

async def save_mix(pool: asyncpg.Pool, user_id: int, name: str, ingredients: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO mixes (user_id, name, ingredients) VALUES ($1, $2, $3)",
            user_id, name, ingredients,
        )

async def delete_mix(pool: asyncpg.Pool, mix_id: int, user_id: int):
    """Удаляет микс по id, проверяя принадлежность пользователю."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM mixes WHERE id = $1 AND user_id = $2",
            mix_id, user_id,
        )

async def get_user_mixes(pool: asyncpg.Pool, user_id: int) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, ingredients FROM mixes WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )
    return [dict(r) for r in rows]

# ---------- google ----------
def get_calendar_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Файл {SERVICE_ACCOUNT_FILE} не найден.")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("calendar", "v3", credentials=credentials)

def check_shift_today() -> bool:
    service = get_calendar_service()
    msk = TIMEZONE
    now   = datetime.now(msk)
    start = msk.localize(datetime.combine(now, time.min))
    end   = msk.localize(datetime.combine(now, time.max))
    events = (
        service.events()
        .list(
            calendarId="mikserhipster4@gmail.com",
            timeMin=start.isoformat(),
            timeMax=end.isoformat(),
            singleEvents=True,
            orderBy="startTime",
            q=SEARCH_TERM,
        )
        .execute()
        .get("items", [])
    )
    return bool(events)

def check_shift_tomorrow() -> bool:
    service = get_calendar_service()
    msk      = TIMEZONE
    today    = datetime.now(msk)
    tomorrow = today + timedelta(days=1)
    start = msk.localize(datetime.combine(tomorrow, time.min))
    end   = msk.localize(datetime.combine(tomorrow, time.max))
    events = (
        service.events()
        .list(
            calendarId="mikserhipster4@gmail.com",
            timeMin=start.isoformat(),
            timeMax=end.isoformat(),
            singleEvents=True,
            orderBy="startTime",
            q=SEARCH_TERM,
        )
        .execute()
        .get("items", [])
    )
    return bool(events)

# ---------- команды и упоминания ----------
async def cmd_serezha(update: Update, _: ContextTypes.DEFAULT_TYPE):
    """/serezha или @bot_username -> ответ да/нет"""
    try:
        today = "Да" if check_shift_today() else "Нет"
    except Exception as e:
        today = f"Ошибка: {e}"
    await update.message.reply_text(f"Серёжа сегодня работает? {today}")

async def cmd_serezha_zavtra(update: Update, _: ContextTypes.DEFAULT_TYPE):
    try:
        answer = "Да" if check_shift_tomorrow() else "Нет"
        text = f"Серёжа завтра работает? {answer}"
    except Exception as e:
        text = f"Ошибка: {e}"
    await update.message.reply_text(text)

async def mention(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Любое сообщение, где упомянули бота."""
    bot_username = f"@{context.bot.username}"
    if update.message.text and (bot_username in update.message.text):
        await cmd_serezha(update, context)

# ---------- BROADCAST ----------
ADMIN_ID = 857683068

async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/broadcast <text> – рассылка от админа"""
    user = update.effective_user
    if user.id != ADMIN_ID:
        await update.message.reply_text("❌ Access denied")
        return

    text = update.message.text.partition(" ")[2]
    if not text:
        await update.message.reply_text("Usage: /broadcast <your text>")
        return

    pool: asyncpg.Pool = ctx.bot_data["db_pool"]
    users = await load_users(pool)
    ok, fail = 0, 0
    for u in users:
        try:
            await ctx.bot.send_message(chat_id=u["id"], text=text)
            ok += 1
        except Exception as e:
            fail += 1
            print("broadcast fail:", u["id"], e)

    await update.message.reply_text(f"📤 Рассылка завершена: {ok} отправлено, {fail} ошибок.")

async def register_user(pool: asyncpg.Pool, user):
    """Добавляет пользователя в таблицу users при любом обращении (upsert)."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (id, username, first_name, last_name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                username   = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name  = EXCLUDED.last_name
        """, user.id, user.username, user.first_name, user.last_name)

async def load_users(pool: asyncpg.Pool) -> list[dict]:
    """Возвращает всех пользователей из БД."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, username, first_name FROM users")
    return [dict(r) for r in rows]

# ---------- клавиатура ----------
def build_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Основная клавиатура. Кнопка «Стафф кальян» — только для ALLOWED_IDS."""
    rows = [
        ["Работает ли Серёжа сегодня?", "А завтра?"],
        ["Записать микс",               "Мои миксы"],
    ]
    if user_id in ALLOWED_IDS:
        rows.append(["Стафф кальян 🎰"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)

# Статичная клавиатура для мест, где user_id недоступен (FSM-шаги)
KEYBOARD = ReplyKeyboardMarkup(
    [
        ["Работает ли Серёжа сегодня?", "А завтра?"],
        ["Записать микс",               "Мои миксы"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

# ---------- /start ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool: asyncpg.Pool = context.bot_data["db_pool"]
    await register_user(pool, update.effective_user)
    await update.message.reply_text(
        "Привет! Нажми на кнопку, чтобы узнать, работает ли Серёжа сегодня или завтра.",
        reply_markup=build_main_keyboard(update.effective_user.id),
    )

# ---------- FSM: Записать микс ----------
async def mix_start(update: Update, _: ContextTypes.DEFAULT_TYPE) -> int:
    """Вход в FSM — просим название."""
    await update.message.reply_text(
        "🎧 Введи название микса:",
        reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True),
    )
    return WAITING_FOR_NAME

async def mix_got_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получили название — просим состав. Отмена → главный экран."""
    if update.message.text == "Отмена":
        context.user_data.pop("mix_name", None)
        await update.message.reply_text("❌ Запись микса отменена.", reply_markup=KEYBOARD)
        return ConversationHandler.END

    context.user_data["mix_name"] = update.message.text
    await update.message.reply_text(
        f"✅ Название «{update.message.text}» сохранено.\n\n🧪 Теперь введи состав микса:",
        reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True),
    )
    return WAITING_FOR_INGREDIENTS

async def mix_got_ingredients(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получили состав — сохраняем в PostgreSQL. Отмена → шаг назад (ввод названия)."""
    if update.message.text == "Отмена":
        await update.message.reply_text(
            "↩️ Возвращаемся к названию.\n\n🎧 Введи название микса:",
            reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True),
        )
        return WAITING_FOR_NAME

    name        = context.user_data.pop("mix_name", "—")
    ingredients = update.message.text
    user_id     = update.effective_user.id
    pool: asyncpg.Pool = context.bot_data["db_pool"]

    try:
        await save_mix(pool, user_id, name, ingredients)
        await update.message.reply_text(
            f"✅ Микс сохранён!\n\n🎧 Название: {name}\n🧪 Состав: {ingredients}",
            reply_markup=KEYBOARD,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при сохранении: {e}", reply_markup=KEYBOARD)

    return ConversationHandler.END

async def mix_cancel(update: Update, _: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Запись микса отменена.", reply_markup=KEYBOARD)
    return ConversationHandler.END

# ---------- Мои миксы + пагинация ----------
def build_mixes_keyboard(mixes: list[dict], page: int) -> InlineKeyboardMarkup:
    """Строит inline-клавиатуру со списком миксов и пагинацией."""
    total_pages = max(1, (len(mixes) + MIXES_PER_PAGE - 1) // MIXES_PER_PAGE)
    page        = max(0, min(page, total_pages - 1))

    page_mixes = mixes[page * MIXES_PER_PAGE : (page + 1) * MIXES_PER_PAGE]

    rows = []
    for mix in page_mixes:
        rows.append([
            InlineKeyboardButton(
                text=mix["name"],
                callback_data=f"mix_view:{mix['id']}",
            )
        ])

    # Строка пагинации
    nav = []
    nav.append(
        InlineKeyboardButton("‹", callback_data=f"mix_page:{page - 1}")
        if page > 0 else
        InlineKeyboardButton(" ", callback_data="mix_noop")
    )
    nav.append(InlineKeyboardButton(f"{page + 1} / {total_pages}", callback_data="mix_noop"))
    nav.append(
        InlineKeyboardButton("›", callback_data=f"mix_page:{page + 1}")
        if page < total_pages - 1 else
        InlineKeyboardButton(" ", callback_data="mix_noop")
    )
    rows.append(nav)

    return InlineKeyboardMarkup(rows)

async def my_mixes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает первую страницу миксов пользователя."""
    pool: asyncpg.Pool = context.bot_data["db_pool"]
    user_id = update.effective_user.id

    try:
        mixes = await get_user_mixes(pool, user_id)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при загрузке: {e}")
        return

    if not mixes:
        await update.message.reply_text("У тебя пока нет сохранённых миксов.", reply_markup=KEYBOARD)
        return

    context.user_data["mixes_cache"] = mixes
    keyboard = build_mixes_keyboard(mixes, page=0)
    await update.message.reply_text(f"🎧 Твои миксы ({len(mixes)} шт.):", reply_markup=keyboard)

async def mixes_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает inline-кнопки: пагинацию и просмотр микса."""
    query = update.callback_query
    await query.answer()

    data    = query.data
    pool: asyncpg.Pool = context.bot_data["db_pool"]
    user_id = query.from_user.id

    if data == "mix_noop":
        return

    if data.startswith("mix_page:"):
        page  = int(data.split(":")[1])
        mixes = context.user_data.get("mixes_cache") or await get_user_mixes(pool, user_id)
        context.user_data["mixes_cache"] = mixes
        keyboard = build_mixes_keyboard(mixes, page=page)
        await query.edit_message_reply_markup(reply_markup=keyboard)

    elif data.startswith("mix_view:"):
        mix_id = int(data.split(":")[1])
        mixes  = context.user_data.get("mixes_cache") or await get_user_mixes(pool, user_id)
        mix    = next((m for m in mixes if m["id"] == mix_id), None)
        if mix:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Удалить", callback_data=f"mix_delete_ask:{mix_id}")
            ]])
            await query.message.reply_text(
                f"🎧 Название: {mix['name']}\n🧪 Состав: {mix['ingredients']}",
                reply_markup=keyboard,
            )
        else:
            await query.message.reply_text("❌ Микс не найден.")

    elif data.startswith("mix_delete_ask:"):
        mix_id = int(data.split(":")[1])
        mixes  = context.user_data.get("mixes_cache") or await get_user_mixes(pool, user_id)
        mix    = next((m for m in mixes if m["id"] == mix_id), None)
        name   = mix["name"] if mix else "этот микс"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"mix_delete_confirm:{mix_id}"),
            InlineKeyboardButton("✖️ Отмена",      callback_data="mix_noop"),
        ]])
        await query.message.reply_text(
            f"Удалить «{name}»?",
            reply_markup=keyboard,
        )

    elif data.startswith("mix_delete_confirm:"):
        mix_id = int(data.split(":")[1])
        await delete_mix(pool, mix_id, user_id)

        # Инвалидируем кэш и обновляем список
        mixes = await get_user_mixes(pool, user_id)
        context.user_data["mixes_cache"] = mixes

        await query.message.reply_text("✅ Микс удалён.")

        if mixes:
            keyboard = build_mixes_keyboard(mixes, page=0)
            await query.message.reply_text(f"🎧 Твои миксы ({len(mixes)} шт.):", reply_markup=keyboard)
        else:
            await query.message.reply_text("У тебя больше нет сохранённых миксов.")

# ---------- Стафф кальян (закрытая функция) ----------
import random

# Два состояния: фаза А (основной табак) и фаза Б (додеп)
STAFF_PHASE_A = 10
STAFF_PHASE_B = 11

def _staff_kb_phase_a(has_complete_set: bool) -> InlineKeyboardMarkup:
    """Клавиатура фазы А: Крутим + Похуй некст [+ Enough]."""
    row = [
        InlineKeyboardButton("Крутим 🎰",   callback_data="staff_reroll_a"),
        InlineKeyboardButton("Похуй некст", callback_data="staff_next_a"),
    ]
    if has_complete_set:
        row.append(InlineKeyboardButton("Enough ✅", callback_data="staff_enough"))
    return InlineKeyboardMarkup([row])

def _staff_kb_phase_b() -> InlineKeyboardMarkup:
    """Клавиатура фазы Б: Крутим + Дальше."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Крутим 🎰", callback_data="staff_reroll_b"),
        InlineKeyboardButton("Дальше ➡️", callback_data="staff_next_b"),
    ]])

def _phase_a_text(x: int, y: int) -> str:
    return f"🌿 Основной табак\nВыпало: Полка {x}, Ряд {y}"

def _phase_b_text(ax: int, ay: int, bx: int, by: int) -> str:
    return (
        f"🌿 Основной: Полка {ax}, Ряд {ay}\n"
        f"📦 Додеп на контейнер\nВыпало: Полка {bx}, Ряд {by}"
    )

async def staff_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Вход в стафф-режим — только для ALLOWED_IDS."""
    if update.effective_user.id not in ALLOWED_IDS:
        return ConversationHandler.END

    x, y = random.randint(1, 5), random.randint(1, 4)
    context.user_data["staff_saved"]   = []   # список готовых пар (ax, ay, bx, by)
    context.user_data["staff_current"] = (x, y)  # текущая пара фазы А

    await update.message.reply_text(
        _phase_a_text(x, y),
        reply_markup=_staff_kb_phase_a(has_complete_set=False),
    )
    return STAFF_PHASE_A

async def staff_callback_a(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка фазы А: Крутим / Похуй некст / Enough."""
    query = update.callback_query
    await query.answer()

    if query.from_user.id not in ALLOWED_IDS:
        return STAFF_PHASE_A

    saved   = context.user_data.get("staff_saved", [])
    action  = query.data

    if action == "staff_reroll_a":
        x, y = random.randint(1, 5), random.randint(1, 4)
        context.user_data["staff_current"] = (x, y)
        await query.edit_message_text(
            _phase_a_text(x, y),
            reply_markup=_staff_kb_phase_a(has_complete_set=bool(saved)),
        )
        return STAFF_PHASE_A

    if action == "staff_next_a":
        # Фиксируем основной табак, переходим в фазу Б (додеп)
        ax, ay = context.user_data["staff_current"]
        context.user_data["staff_main"] = (ax, ay)
        bx, by = random.randint(1, 5), random.randint(1, 4)
        context.user_data["staff_dodep"] = (bx, by)
        await query.edit_message_text(
            _phase_b_text(ax, ay, bx, by),
            reply_markup=_staff_kb_phase_b(),
        )
        return STAFF_PHASE_B

    if action == "staff_enough":
        return await _staff_finish(query, context)

    return STAFF_PHASE_A

async def staff_callback_b(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка фазы Б: Крутим (додеп) / Дальше."""
    query = update.callback_query
    await query.answer()

    if query.from_user.id not in ALLOWED_IDS:
        return STAFF_PHASE_B

    action = query.data
    ax, ay = context.user_data["staff_main"]

    if action == "staff_reroll_b":
        bx, by = random.randint(1, 5), random.randint(1, 4)
        context.user_data["staff_dodep"] = (bx, by)
        await query.edit_message_text(
            _phase_b_text(ax, ay, bx, by),
            reply_markup=_staff_kb_phase_b(),
        )
        return STAFF_PHASE_B

    if action == "staff_next_b":
        # Сохраняем полный сет (основной + додеп), возвращаемся в фазу А
        bx, by = context.user_data["staff_dodep"]
        saved  = context.user_data.get("staff_saved", [])
        saved.append((ax, ay, bx, by))
        context.user_data["staff_saved"] = saved

        nx, ny = random.randint(1, 5), random.randint(1, 4)
        context.user_data["staff_current"] = (nx, ny)
        await query.edit_message_text(
            _phase_a_text(nx, ny),
            reply_markup=_staff_kb_phase_a(has_complete_set=True),
        )
        return STAFF_PHASE_A

    return STAFF_PHASE_B

async def _staff_finish(query, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершение: вывод итогового списка."""
    saved = context.user_data.get("staff_saved", [])
    lines = [
        f"Табак {i+1}: {ax} {ay} (Додеп: {bx} {by})"
        for i, (ax, ay, bx, by) in enumerate(saved)
    ]
    result = "📋 Итог:\n" + "\n".join(lines) if lines else "Ничего не выбрано."
    await query.edit_message_text(result)

    for key in ("staff_saved", "staff_current", "staff_main", "staff_dodep"):
        context.user_data.pop(key, None)

    return ConversationHandler.END

# ---------- Единая точка входа текстовых сообщений ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pool: asyncpg.Pool = context.bot_data["db_pool"]
    await register_user(pool, update.effective_user)
    text    = update.message.text
    user_id = update.effective_user.id
    kb      = build_main_keyboard(user_id)

    await mention(update, context)

    try:
        if text == "Работает ли Серёжа сегодня?":
            answer = "Да" if check_shift_today() else "Нет"
            await update.message.reply_text(answer, reply_markup=kb)
        elif text == "А завтра?":
            answer = "Да" if check_shift_tomorrow() else "Нет"
            await update.message.reply_text(answer, reply_markup=kb)
        elif text == "Мои миксы":
            await my_mixes(update, context)
        elif text == "Стафф кальян 🎰":
            # Защита: если текст введён вручную не из ALLOWED_IDS — игнор
            if user_id not in ALLOWED_IDS:
                return
            # Передаём управление FSM через entry_point
            await staff_start(update, context)
    except Exception as e:
        await update.message.reply_text(f"Произошла ошибка: {e}", reply_markup=kb)

# ---------- запуск ----------
async def post_init(app: Application):
    """Инициализация пула PostgreSQL после старта приложения."""
    pool = await get_db_pool()
    await init_db(pool)
    app.bot_data["db_pool"] = pool
    print("✅ PostgreSQL подключён, таблицы users и mixes готовы.")

async def post_shutdown(app: Application):
    pool = app.bot_data.get("db_pool")
    if pool:
        await pool.close()

def main():
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # FSM для записи микса
    mix_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Записать микс$"), mix_start)],
        states={
            WAITING_FOR_NAME:        [MessageHandler(filters.TEXT & ~filters.COMMAND, mix_got_name)],
            WAITING_FOR_INGREDIENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, mix_got_ingredients)],
        },
        fallbacks=[MessageHandler(filters.Regex("^Отмена$"), mix_cancel)],
    )

    # FSM для стафф кальяна (две фазы: основной табак + додеп)
    staff_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Стафф кальян 🎰$"), staff_start)],
        states={
            STAFF_PHASE_A: [CallbackQueryHandler(staff_callback_a, pattern="^staff_(reroll_a|next_a|enough)$")],
            STAFF_PHASE_B: [CallbackQueryHandler(staff_callback_b, pattern="^staff_(reroll_b|next_b)$")],
        },
        fallbacks=[],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(mix_conv)
    app.add_handler(staff_conv)
    app.add_handler(CallbackQueryHandler(mixes_callback, pattern=r"^mix_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("serezha", cmd_serezha))
    app.add_handler(CommandHandler("serezha_zavtra", cmd_serezha_zavtra))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    print("Бот запущен и готов к работе!")
    app.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Бот остановлен.")
