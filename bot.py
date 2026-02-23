import re
import os
import asyncio
import logging
import subprocess
import json
import zipfile
import random
from pathlib import Path
from datetime import datetime, date

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
import yt_dlp

# ─── ffmpeg — ищем системный или из imageio-ffmpeg ───────────────────────────

def _setup_ffmpeg():
    import shutil, glob

    # Попытка 1: системный ffmpeg
    p = shutil.which("ffmpeg")
    if p:
        logging.info(f"ffmpeg найден в системе: {p}")
        return

    # Попытка 2: imageio-ffmpeg
    try:
        import imageio_ffmpeg
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        os.environ["PATH"] = str(Path(ffmpeg_path).parent) + os.pathsep + os.environ.get("PATH", "")
        logging.info(f"ffmpeg найден через imageio-ffmpeg: {ffmpeg_path}")
        return
    except Exception as e:
        logging.warning(f"imageio-ffmpeg: {e}")

    # Попытка 3: nix store (Railway использует Nix)
    results = glob.glob("/nix/store/*/bin/ffmpeg")
    if results:
        os.environ["PATH"] = str(Path(results[0]).parent) + os.pathsep + os.environ.get("PATH", "")
        logging.info(f"ffmpeg найден в nix store: {results[0]}")
        return

    # Попытка 4: установить через apt-get
    logging.warning("ffmpeg не найден, пробуем установить через apt-get...")
    try:
        subprocess.run(["apt-get", "install", "-y", "-q", "ffmpeg"],
                      capture_output=True, timeout=120)
        p = shutil.which("ffmpeg")
        if p:
            logging.info(f"ffmpeg успешно установлен: {p}")
            return
    except Exception as e:
        logging.warning(f"apt-get не сработал: {e}")

    logging.error("ffmpeg НЕ НАЙДЕН — скачивание с мержем форматов не будет работать!")

_setup_ffmpeg()

# ─── Логирование ─────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Конфиг ──────────────────────────────────────────────────────────────────

BOT_TOKEN = "8322503182:AAF8C0Ojhu6OPCMLakURfWdm7TeycsCK9vQ"
BOT_USERNAME = os.environ.get("BOT_USERNAME", "balerndownloadsbot")
BOT_VERSION  = os.environ.get("BOT_VERSION", "1.1")
ADMIN_ID     = 649566280
DAILY_LIMIT  = 20
HISTORY_SIZE = 10
MAX_FILE_MB  = 50

DATA_DIR = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR = DATA_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
DATA_FILE = DATA_DIR / "data.json"

# Активные пользователи в текущей сессии: user_id -> lang
ACTIVE_USERS: dict[int, str] = {}

# ─── Патч-ноты ───────────────────────────────────────────────────────────────

PATCH_NOTES = {
    "1.1": {
        "ru": (
            "🆕 Обновление v1.1\n\n"
            "• 🎛 Интерактивное меню /menu — всё в одном месте\n"
            "• 🌍 Поддержка английского языка\n"
            "• 📊 Команда /me — твоя личная статистика\n"
            "• 📤 Кнопка «Поделиться ботом»\n"
            "• 🖼 Красивое приветствие с картинкой\n"
            "• 💾 История скачиваний сохраняется между сессиями"
        ),
        "en": (
            "🆕 Update v1.1\n\n"
            "• 🎛 Interactive menu /menu — everything in one place\n"
            "• 🌍 English language support\n"
            "• 📊 /me command — your personal stats\n"
            "• 📤 Share bot button\n"
            "• 🖼 Beautiful welcome message\n"
            "• 💾 Download history saved between sessions"
        ),
    },
}

# ─── Переводы ────────────────────────────────────────────────────────────────

TEXTS = {
    "ru": {
        "start_caption": (
            "👋 О, новый пользователь! Уже загружаю котиков... шучу.\n\n"
            "Я скачиваю видео из TikTok, YouTube, Twitter, VK и других платформ.\n"
            "Просто кинь ссылку и выбери что тебе нужно — видео, MP3, GIF или даже целый плейлист.\n\n"
            "Поехали! 🚀"
        ),
        "help": (
            "📌 Как пользоваться:\n\n"
            "1. Отправь ссылку на видео\n"
            "2. Выбери формат (видео / MP3 / GIF / плейлист)\n"
            "3. Выбери качество и уровень звука\n"
            "4. Выбери ориентацию, включи субтитры, обрежь видео или нажми «Скачать»\n\n"
            "⚠️ Лимит: 50 МБ и 20 скачиваний в день\n\n"
            "/menu — открыть меню\n"
            "/history — последние 10 ссылок\n"
            "/me — моя статистика"
        ),
        "history_empty": "📭 История пуста.",
        "history_title": "🕘 Последние скачивания (нажми чтобы скачать снова):",
        "blocked": "🚫 Ты заблокирован.",
        "limit": "⛔ Достигнут дневной лимит ({limit} скачиваний).\nВозвращайся завтра!",
        "no_url": "🔗 Пришли мне ссылку на видео.",
        "unsupported": "❌ Платформа не поддерживается.\nПоддерживаются: TikTok, YouTube, Twitter, VK, Twitch, Reddit.",
        "step1": "📦 Шаг 1 — выбери формат:",
        "remaining": "Осталось скачиваний сегодня: {remaining}",
        "me": (
            "👤 Твоя статистика:\n\n"
            "📥 Всего скачиваний: {total}\n"
            "❤️ Любимая платформа: {fav}\n"
            "📅 Сегодня: {today} из {limit}"
        ),
        "me_empty": "📭 Ты ещё ничего не скачивал!",
        "menu_title": "🎛 Главное меню:",
    },
    "en": {
        "start_caption": (
            "👋 Oh, a new user! Already loading cats... just kidding.\n\n"
            "I download videos from TikTok, YouTube, Twitter, VK and other platforms.\n"
            "Just send a link and choose what you need — video, MP3, GIF or even a whole playlist.\n\n"
            "Let's go! 🚀"
        ),
        "help": (
            "📌 How to use:\n\n"
            "1. Send a video link\n"
            "2. Choose format (video / MP3 / GIF / playlist)\n"
            "3. Choose quality and audio level\n"
            "4. Choose orientation, enable subtitles, trim or press Download\n\n"
            "⚠️ Limit: 50 MB and 20 downloads per day\n\n"
            "/menu — open menu\n"
            "/history — last 10 links\n"
            "/me — my statistics"
        ),
        "history_empty": "📭 History is empty.",
        "history_title": "🕘 Recent downloads (tap to download again):",
        "blocked": "🚫 You are blocked.",
        "limit": "⛔ Daily limit reached ({limit} downloads).\nCome back tomorrow!",
        "no_url": "🔗 Send me a video link.",
        "unsupported": "❌ Platform not supported.\nSupported: TikTok, YouTube, Twitter, VK, Twitch, Reddit.",
        "step1": "📦 Step 1 — choose format:",
        "remaining": "Downloads left today: {remaining}",
        "me": (
            "👤 Your statistics:\n\n"
            "📥 Total downloads: {total}\n"
            "❤️ Favourite platform: {fav}\n"
            "📅 Today: {today} of {limit}"
        ),
        "me_empty": "📭 You haven't downloaded anything yet!",
        "menu_title": "🎛 Main menu:",
    }
}

def get_lang(context) -> str:
    return context.user_data.get("lang", "ru")

def t(context, key: str, **kwargs) -> str:
    lang = get_lang(context)
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, key)
    return text.format(**kwargs) if kwargs else text

# ─── Хранилище данных ────────────────────────────────────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"stats": {}, "blocked": [], "downloads_today": {}, "last_reset": str(date.today())}

def save_data(data: dict):
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def get_data() -> dict:
    data = load_data()
    if data.get("last_reset") != str(date.today()):
        data["downloads_today"] = {}
        data["last_reset"] = str(date.today())
        save_data(data)
    return data

# ─── Платформы / качество / звук ─────────────────────────────────────────────

SUPPORTED_PATTERNS = [
    r"tiktok\.com", r"vm\.tiktok\.com",
    r"instagram\.com", r"instagr\.am",
    r"youtube\.com/shorts", r"youtu\.be",
    r"youtube\.com/watch",
    r"twitter\.com", r"x\.com",
    r"vk\.com", r"clips\.twitch\.tv",
    r"reddit\.com",
]

QUALITY_OPTIONS = {
    "360":  "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360][ext=mp4]/best[height<=360]",
    "480":  "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]",
    "720":  "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
    "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]",
    "best": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
}
QUALITY_LABELS = {"360": "360p", "480": "480p", "720": "720p HD", "1080": "1080p FHD", "best": "Макс."}

AUDIO_OPTIONS = {
    "mute":   (0.0, "🔇 Без звука"),
    "quiet":  (0.4, "🔉 Тише"),
    "normal": (1.0, "🔊 Обычный"),
    "loud":   (2.0, "📢 Громче"),
}

FUNNY_MESSAGES = [
    "🐱 Ищем котиков в интернете...",
    "🦠 Сканируем интернет на вирусы...",
    "🕵️ Следим за соседями через веб-камеру...",
    "🍕 Заказываем пиццу на твой адрес...",
    "🤖 Обучаем нейросеть на твоих фото...",
    "🐢 Ждём пока черепаха донесёт данные...",
    "☕ Завариваем кофе для сервера...",
    "🌍 Объезжаем весь интернет на велосипеде...",
    "👽 Связываемся с инопланетянами для помощи...",
    "🎲 Бросаем кубик чтобы решить что делать...",
    "🐿️ Белка грызёт оптоволокно, чуть подождём...",
    "💾 Перематываем кассету обратно...",
    "🧙 Читаем заклинания для ускорения загрузки...",
    "🏃 Курьер бежит с флешкой, уже близко...",
    "🔮 Предсказываем когда это закончится...",
    "🌊 Ныряем на дно океана за кабелем...",
    "🐧 Пингвины толкают сервер лапками...",
    "📡 Ловим сигнал со спутника над Антарктидой...",
    "🍌 Угощаем хомяков в датацентре бананами...",
    "⚙️ Крутим шестерёнки вручную...",
]

RETRY_DELAYS = [5, 15]


def is_supported_url(url: str) -> bool:
    return any(re.search(p, url, re.IGNORECASE) for p in SUPPORTED_PATTERNS)

def get_platform(url: str) -> str:
    mapping = [
        (r"tiktok\.com",               "TikTok"),
        (r"instagram\.com|instagr\.am","Instagram"),
        (r"youtube\.com|youtu\.be",    "YouTube"),
        (r"twitter\.com|x\.com",       "Twitter/X"),
        (r"vk\.com",                   "VK"),
        (r"twitch\.tv",                "Twitch"),
        (r"reddit\.com",               "Reddit"),
    ]
    for pattern, name in mapping:
        if re.search(pattern, url, re.IGNORECASE):
            return name
    return "Видео"

def make_progress_bar(percent: int) -> str:
    filled = int(percent / 10)
    return f"[{'█' * filled}{'░' * (10 - filled)}] {percent}%"

def get_funny_status(pct: int) -> str:
    return f"{random.choice(FUNNY_MESSAGES)}\n{make_progress_bar(pct)}"

# ─── Данные пользователей ─────────────────────────────────────────────────────

def update_stats(user_id: int, platform: str):
    data = get_data()
    uid = str(user_id)
    stats = data.setdefault("stats", {})
    stats["total"] = stats.get("total", 0) + 1
    platforms = stats.setdefault("platforms", {})
    platforms[platform] = platforms.get(platform, 0) + 1
    users = stats.setdefault("users", {})
    users[uid] = users.get(uid, 0) + 1
    user_platforms = data.setdefault("user_platforms", {})
    up = user_platforms.setdefault(uid, {})
    up[platform] = up.get(platform, 0) + 1
    save_data(data)

def check_limit(user_id: int) -> tuple[bool, int]:
    data = get_data()
    uid = str(user_id)
    today_count = data.get("downloads_today", {}).get(uid, 0)
    return today_count < DAILY_LIMIT, DAILY_LIMIT - today_count

def increment_limit(user_id: int):
    data = get_data()
    uid = str(user_id)
    dt = data.setdefault("downloads_today", {})
    dt[uid] = dt.get(uid, 0) + 1
    save_data(data)

def is_blocked(user_id: int) -> bool:
    return user_id in get_data().get("blocked", [])

def add_to_history(context: ContextTypes.DEFAULT_TYPE, url: str, platform: str):
    history = context.user_data.setdefault("history", [])
    history = [h for h in history if h.get("url") != url]
    history.insert(0, {"url": url, "platform": platform, "time": datetime.now().isoformat()})
    context.user_data["history"] = history[:HISTORY_SIZE]
    # Сохраняем в data.json
    uid = str(context.user_data.get("_uid", "0"))
    if uid != "0":
        data = get_data()
        h = data.setdefault("histories", {}).setdefault(uid, [])
        h = [x for x in h if x.get("url") != url]
        h.insert(0, {"url": url, "platform": platform, "time": datetime.now().isoformat()})
        data["histories"][uid] = h[:HISTORY_SIZE]
        save_data(data)

def load_history_from_db(user_id: int) -> list:
    return get_data().get("histories", {}).get(str(user_id), [])

# ─── Клавиатуры ──────────────────────────────────────────────────────────────

def main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⬇️ Скачать видео",    callback_data="menu_download"),
         InlineKeyboardButton("🕘 История",           callback_data="menu_history")],
        [InlineKeyboardButton("📊 Моя статистика",   callback_data="menu_me"),
         InlineKeyboardButton("📋 Патч-ноут",        callback_data="menu_patchnote")],
        [InlineKeyboardButton("❓ Помощь",            callback_data="menu_help"),
         InlineKeyboardButton("🌍 Язык",             callback_data="menu_lang")],
        [InlineKeyboardButton("📤 Поделиться",       switch_inline_query=f"Скачиваю видео из TikTok, YouTube и не только! @{BOT_USERNAME}")],
    ]
    if is_admin:
        rows.append([
            InlineKeyboardButton("📊 Стат. бота",    callback_data="menu_stats"),
            InlineKeyboardButton("🚫 Блокировки",    callback_data="menu_blocks"),
        ])
        rows.append([
            InlineKeyboardButton("📢 Разослать патч-ноут", callback_data="menu_sendpatch"),
        ])
    return InlineKeyboardMarkup(rows)

def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="menu_back")]])

def lang_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("◀️ Назад",   callback_data="menu_back")],
    ])

def admin_blocks_keyboard(blocked: list) -> InlineKeyboardMarkup:
    rows = []
    for uid in blocked[:10]:
        rows.append([InlineKeyboardButton(f"🔓 {uid}", callback_data=f"adm_unblock_{uid}")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_back")])
    return InlineKeyboardMarkup(rows)

def format_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Видео (MP4)",        callback_data="fmt_video"),
         InlineKeyboardButton("🎵 Только аудио (MP3)", callback_data="fmt_audio")],
        [InlineKeyboardButton("🌀 GIF",                callback_data="fmt_gif"),
         InlineKeyboardButton("📋 Плейлист (ZIP)",     callback_data="fmt_playlist")],
    ])

def quality_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("360p",            callback_data="quality_360"),
         InlineKeyboardButton("480p",            callback_data="quality_480")],
        [InlineKeyboardButton("720p HD",         callback_data="quality_720"),
         InlineKeyboardButton("1080p FHD",       callback_data="quality_1080")],
        [InlineKeyboardButton("🏆 Максимальное", callback_data="quality_best")],
    ])

def audio_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔇 Без звука", callback_data="audio_mute"),
         InlineKeyboardButton("🔉 Тише",      callback_data="audio_quiet")],
        [InlineKeyboardButton("🔊 Обычный",   callback_data="audio_normal"),
         InlineKeyboardButton("📢 Громче",    callback_data="audio_loud")],
    ])

def orientation_keyboard(subs_on: bool = False) -> InlineKeyboardMarkup:
    subs_label = "📝 Субтитры: ВКЛ ✅" if subs_on else "📝 Субтитры: ВЫКЛ ❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Оригинал",           callback_data="orient_original"),
         InlineKeyboardButton("⬛ Квадрат (1:1)",      callback_data="orient_square")],
        [InlineKeyboardButton("🖼 Горизонталь (16:9)", callback_data="orient_landscape")],
        [InlineKeyboardButton(subs_label,              callback_data="orient_toggle_subs")],
        [InlineKeyboardButton("✂️ Обрезать видео",     callback_data="orient_trim")],
        [InlineKeyboardButton("⬇️ Скачать сейчас",    callback_data="orient_download")],
    ])

def trim_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Обрезать видео", callback_data="trim_yes"),
         InlineKeyboardButton("⏭ Без обрезки",    callback_data="trim_no")],
    ])

def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить", callback_data="cancel_download")]])

def history_keyboard(history: list) -> InlineKeyboardMarkup:
    buttons = []
    for i, item in enumerate(history):
        platform = item.get("platform", "Видео")
        ts = item.get("time", "")[:10]
        buttons.append([InlineKeyboardButton(f"{platform} • {ts}", callback_data=f"history_{i}")])
    buttons.append([InlineKeyboardButton("❌ Закрыть", callback_data="history_close")])
    return InlineKeyboardMarkup(buttons)

# ─── Утилиты ffmpeg ───────────────────────────────────────────────────────────

def ffmpeg_run(cmd: list) -> bool:
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        logger.error(f"ffmpeg error: {result.stderr.decode()}")
    return result.returncode == 0

def apply_audio(input_path: Path, volume: float) -> Path:
    if volume == 1.0:
        return input_path
    output_path = input_path.with_stem(input_path.stem + "_audio")
    if volume == 0.0:
        cmd = ["ffmpeg", "-y", "-i", str(input_path), "-an", "-c:v", "copy", str(output_path)]
    else:
        cmd = ["ffmpeg", "-y", "-i", str(input_path), "-filter:a", f"volume={volume}", "-c:v", "copy", str(output_path)]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path

def apply_orientation(input_path: Path, orient: str) -> Path:
    if orient == "original":
        return input_path
    output_path = input_path.with_stem(input_path.stem + "_orient")
    if orient == "square":
        vf = "crop=min(iw\\,ih):min(iw\\,ih),scale=720:720"
    else:
        vf = "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2"
    cmd = ["ffmpeg", "-y", "-i", str(input_path), "-vf", vf, "-c:a", "copy", str(output_path)]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path

def apply_trim(input_path: Path, start: str, end: str) -> Path:
    output_path = input_path.with_stem(input_path.stem + "_trim")
    cmd = ["ffmpeg", "-y", "-i", str(input_path), "-ss", start, "-to", end, "-c", "copy", str(output_path)]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path

def convert_to_gif(input_path: Path) -> Path:
    output_path = input_path.with_suffix(".gif")
    cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        "-vf", "fps=15,scale=480:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
        "-loop", "0", str(output_path)
    ]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path

def time_str_valid(t: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", t.strip()))

# ─── Скачивание ───────────────────────────────────────────────────────────────

async def download_video(url, quality, output_path, status_msg, cancel_flag, fmt="video") -> Path | None:
    format_str = QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"])
    if fmt == "audio":
        format_str = "bestaudio/best"

    format_with_fallback = format_str + "/best/bestvideo+bestaudio"
    last_update = {"pct": -1}
    loop = asyncio.get_event_loop()

    def progress_hook(d):
        if cancel_flag.get("cancelled"):
            raise Exception("CANCELLED")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                pct = int(downloaded / total * 100)
                if pct - last_update["pct"] >= 10:
                    last_update["pct"] = pct
                    asyncio.run_coroutine_threadsafe(
                        status_msg.edit_text(
                            f"⏳ {get_funny_status(pct)}",
                            reply_markup=cancel_keyboard()
                        ),
                        loop
                    )

    ydl_opts = {
        "outtmpl": str(output_path / "%(id)s.%(ext)s"),
        "format": format_with_fallback,
        "merge_output_format": "mp4" if fmt != "audio" else None,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        "cookiefile": "cookies.txt" if Path("cookies.txt").exists() else None,
        "socket_timeout": 30,
    }

    if fmt == "audio":
        ydl_opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]

    def _download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            p = Path(filename)
            if fmt == "audio":
                mp3 = p.with_suffix(".mp3")
                if mp3.exists():
                    return mp3
            if not p.exists():
                mp4 = p.with_suffix(".mp4")
                if mp4.exists():
                    return mp4
            return p

    for attempt in range(3):
        if cancel_flag.get("cancelled"):
            return None
        try:
            if attempt > 0:
                delay = RETRY_DELAYS[attempt - 1]
                retry_msgs = [
                    f"🔄 Попытка {attempt + 1}/3... Сервер прикидывается мёртвым",
                    f"🔄 Попытка {attempt + 1}/3... Будим сервер снова",
                ]
                asyncio.run_coroutine_threadsafe(
                    status_msg.edit_text(
                        f"⏳ {random.choice(retry_msgs)}, ждём {delay} сек...",
                        reply_markup=cancel_keyboard()
                    ),
                    loop
                )
                await asyncio.sleep(delay)
                last_update["pct"] = -1

            return await loop.run_in_executor(None, _download)

        except Exception as e:
            err = str(e)
            if "CANCELLED" in err:
                return None
            logger.error(f"Попытка {attempt + 1}/3 не удалась: {e}")
            if "Requested format is not available" in err or ("format" in err.lower() and "available" in err.lower()):
                return None
            if attempt == 2:
                return None

    return None

async def download_playlist(url, quality, output_path, status_msg, cancel_flag) -> Path | None:
    playlist_dir = output_path / "playlist_tmp"
    playlist_dir.mkdir(exist_ok=True)
    count = {"n": 0}
    loop = asyncio.get_event_loop()

    def progress_hook(d):
        if cancel_flag.get("cancelled"):
            raise Exception("CANCELLED")
        if d["status"] == "finished":
            count["n"] += 1
            asyncio.run_coroutine_threadsafe(
                status_msg.edit_text(
                    f"⏳ Скачиваю плейлист...\nСкачано видео: {count['n']}",
                    reply_markup=cancel_keyboard()
                ),
                loop
            )

    ydl_opts = {
        "outtmpl": str(playlist_dir / "%(playlist_index)s_%(title)s.%(ext)s"),
        "format": QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"]),
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "noplaylist": False,
        "playlistend": 20,
    }

    def _download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        zip_path = output_path / "playlist.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            for f in playlist_dir.iterdir():
                zf.write(f, f.name)
        return zip_path

    try:
        zip_path = await loop.run_in_executor(None, _download)
        for f in playlist_dir.iterdir():
            f.unlink()
        playlist_dir.rmdir()
        return zip_path
    except Exception as e:
        logger.error(f"Ошибка плейлиста: {e}")
        return None

async def _add_subtitles(url: str, video_path: Path, platform: str) -> tuple[Path, str | None]:
    if platform == "TikTok":
        return video_path, "⚠️ TikTok не поддерживает субтитры"

    output_path = video_path.with_stem(video_path.stem + "_sub")
    ydl_opts = {
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitlesformat": "srt",
        "outtmpl": str(video_path.with_suffix("")),
        "quiet": True,
        "no_warnings": True,
        "cookiefile": "cookies.txt" if Path("cookies.txt").exists() else None,
    }
    loop = asyncio.get_event_loop()

    def _dl_subs():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    try:
        await loop.run_in_executor(None, _dl_subs)
        srt_files = list(video_path.parent.glob(video_path.stem + "*.srt"))
        if not srt_files:
            return video_path, "⚠️ Субтитры недоступны для этого видео"
        srt_file = srt_files[0]
        cmd = ["ffmpeg", "-y", "-i", str(video_path), "-vf", f"subtitles={str(srt_file)}", "-c:a", "copy", str(output_path)]
        ok = ffmpeg_run(cmd)
        srt_file.unlink(missing_ok=True)
        if ok and output_path.exists():
            return output_path, None
        return video_path, "⚠️ Не удалось вшить субтитры"
    except Exception as e:
        err = str(e)
        if "429" in err:
            warn = "⚠️ Субтитры: слишком много запросов (429)"
        else:
            warn = "⚠️ Субтитры недоступны"
        logger.error(f"Ошибка субтитров: {e}")
        return video_path, warn

async def _notify_admin(user, platform, fmt, context):
    try:
        fmt_labels = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF", "playlist": "📋 Плейлист"}
        name = user.full_name or "Неизвестный"
        username = f"@{user.username}" if user.username else "нет username"
        await context.bot.send_message(
            ADMIN_ID,
            f"📥 Новое скачивание!\n\n"
            f"👤 {name} ({username})\n"
            f"🆔 {user.id}\n"
            f"📱 Платформа: {platform}\n"
            f"📦 Формат: {fmt_labels.get(fmt, fmt)}"
        )
    except Exception:
        pass

# ─── Основной обработчик текста ───────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    text = update.message.text.strip()

    # Режим ввода времени обрезки
    if context.user_data.get("waiting_trim"):
        if context.user_data.get("trim_start") is None:
            if not time_str_valid(text):
                await update.message.reply_text("❌ Неверный формат. Введи время, например: 0:15 или 1:30:00")
                return
            context.user_data["trim_start"] = text
            await update.message.reply_text(f"✅ Начало: {text}\n\nТеперь введи время конца:")
        else:
            if not time_str_valid(text):
                await update.message.reply_text("❌ Неверный формат. Введи время, например: 0:45 или 2:00:00")
                return
            context.user_data["trim_end"] = text
            context.user_data["waiting_trim"] = False

            fmt = context.user_data.get("format", "video")
            if fmt == "gif":
                context.user_data["subtitles"] = False
                status_msg = await update.message.reply_text(
                    f"⏳ Скачиваю...\n{make_progress_bar(0)}", reply_markup=cancel_keyboard()
                )
                await _run_download(update.effective_user, status_msg, context)
            else:
                subs_on = context.user_data.get("subtitles", False)
                platform = context.user_data.get("platform", "Видео")
                ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
                trim_s = context.user_data.get("trim_start", "")
                trim_e = context.user_data.get("trim_end", "")
                await update.message.reply_text(
                    f"✅ Обрезка: {trim_s} → {trim_e}\n\n"
                    f"🎬 {platform} • {ql}\n\n"
                    f"📐 Выбери ориентацию и нажми «Скачать»:",
                    reply_markup=orientation_keyboard(subs_on)
                )
        return

    # Регистрируем пользователя
    ACTIVE_USERS[user.id] = get_lang(context)

    if is_blocked(user.id):
        await update.message.reply_text(t(context, "blocked"))
        return

    allowed, remaining = check_limit(user.id)
    if not allowed:
        await update.message.reply_text(t(context, "limit", limit=DAILY_LIMIT))
        return

    urls = re.findall(r"https?://[^\s]+", text)
    if not urls:
        await update.message.reply_text(t(context, "no_url"))
        return

    url = urls[0]
    if not is_supported_url(url):
        await update.message.reply_text(t(context, "unsupported"))
        return

    platform = get_platform(url)
    context.user_data["pending_url"] = url
    context.user_data["platform"] = platform
    context.user_data["cancel_flag"] = {"cancelled": False}
    context.user_data["_uid"] = str(user.id)
    context.user_data["trim_start"] = None
    context.user_data["trim_end"] = None
    context.user_data["subtitles"] = False
    context.user_data["waiting_trim"] = False

    if "history_loaded" not in context.user_data:
        context.user_data["history"] = load_history_from_db(user.id)
        context.user_data["history_loaded"] = True

    await update.message.reply_text(
        f"🎬 Видео с {platform}\n"
        f"{t(context, 'remaining', remaining=remaining)}\n\n"
        f"{t(context, 'step1')}",
        reply_markup=format_keyboard()
    )

# ─── Команды ─────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ACTIVE_USERS[user.id] = get_lang(context)
    is_admin = user.id == ADMIN_ID

    photo_url = "https://i.imgur.com/4M34hi2.png"
    try:
        await update.message.reply_photo(
            photo=photo_url,
            caption=t(context, "start_caption"),
            reply_markup=main_menu_keyboard(is_admin)
        )
    except Exception:
        await update.message.reply_text(
            t(context, "start_caption"),
            reply_markup=main_menu_keyboard(is_admin)
        )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ACTIVE_USERS[user.id] = get_lang(context)
    await update.message.reply_text(
        t(context, "menu_title"),
        reply_markup=main_menu_keyboard(user.id == ADMIN_ID)
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(t(context, "help"))

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if "history_loaded" not in context.user_data:
        context.user_data["history"] = load_history_from_db(update.effective_user.id)
        context.user_data["history_loaded"] = True
    history = context.user_data.get("history", [])
    if not history:
        await update.message.reply_text(t(context, "history_empty"))
        return
    await update.message.reply_text(t(context, "history_title"), reply_markup=history_keyboard(history))

async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    data = get_data()
    uid = str(user.id)
    stats = data.get("stats", {})
    user_total = stats.get("users", {}).get(uid, 0)
    if user_total == 0:
        await update.message.reply_text(t(context, "me_empty"))
        return
    user_platforms = data.get("user_platforms", {}).get(uid, {})
    fav = max(user_platforms.items(), key=lambda x: x[1])[0] if user_platforms else "—"
    today_count = data.get("downloads_today", {}).get(uid, 0)
    await update.message.reply_text(t(context, "me", total=user_total, fav=fav, today=today_count, limit=DAILY_LIMIT))

async def patchnote_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    notes = PATCH_NOTES.get(BOT_VERSION)
    if not notes:
        await update.message.reply_text(f"📋 Версия {BOT_VERSION} — нет патч-нотов.")
        return
    lang = get_lang(context)
    await update.message.reply_text(notes.get(lang, notes.get("ru", "")))

async def sendpatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа.")
        return
    version = context.args[0] if context.args else BOT_VERSION
    notes = PATCH_NOTES.get(version)
    if not notes:
        versions = ", ".join(PATCH_NOTES.keys())
        await update.message.reply_text(f"❌ Нет патч-нота для версии {version}.\nДоступные: {versions}")
        return
    if not ACTIVE_USERS:
        await update.message.reply_text("📭 Нет активных пользователей.")
        return
    status = await update.message.reply_text(f"📤 Рассылаю v{version} для {len(ACTIVE_USERS)} польз...")
    sent = 0
    failed = 0
    for uid, lang in ACTIVE_USERS.items():
        if uid == ADMIN_ID:
            continue
        try:
            text = notes.get(lang, notes.get("ru", ""))
            await context.bot.send_message(chat_id=uid, text=text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Не удалось отправить {uid}: {e}")
            failed += 1
    await status.edit_text(f"✅ Патч-ноут v{version} разослан!\n📨 Отправлено: {sent}\n❌ Не доставлено: {failed}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа.")
        return
    data = get_data()
    stats = data.get("stats", {})
    total = stats.get("total", 0)
    platforms = stats.get("platforms", {})
    top = sorted(platforms.items(), key=lambda x: x[1], reverse=True)
    top_str = "\n".join(f"  {p}: {c}" for p, c in top[:5]) or "  —"
    await update.message.reply_text(
        f"📊 Статистика:\n\n"
        f"Всего скачиваний: {total}\n"
        f"Уникальных пользователей: {len(stats.get('users', {}))}\n"
        f"Активных в сессии: {len(ACTIVE_USERS)}\n"
        f"Заблокировано: {len(data.get('blocked', []))}\n\n"
        f"Топ платформы:\n{top_str}"
    )

async def block_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /block <user_id>")
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверный ID.")
        return
    data = get_data()
    blocked = data.setdefault("blocked", [])
    if uid not in blocked:
        blocked.append(uid)
        save_data(data)
        await update.message.reply_text(f"✅ Пользователь {uid} заблокирован.")
    else:
        await update.message.reply_text("Уже заблокирован.")

async def unblock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /unblock <user_id>")
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверный ID.")
        return
    data = get_data()
    blocked = data.get("blocked", [])
    if uid in blocked:
        blocked.remove(uid)
        save_data(data)
        await update.message.reply_text(f"✅ Пользователь {uid} разблокирован.")
    else:
        await update.message.reply_text("Не был заблокирован.")

# ─── Callback-обработчики ─────────────────────────────────────────────────────

async def safe_edit(query, text, reply_markup=None):
    """Редактирует сообщение — текст или caption (для фото)."""
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception:
        try:
            await query.edit_message_caption(caption=text, reply_markup=reply_markup)
        except Exception:
            try:
                await query.message.reply_text(text, reply_markup=reply_markup)
            except Exception:
                pass

async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    is_admin = user.id == ADMIN_ID
    action = query.data.replace("menu_", "")

    if action == "back":
        try:
            await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin))
        except Exception:
            await query.edit_message_caption(caption=t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin))

    elif action == "download":
        await safe_edit(query, "🔗 Отправь мне ссылку на видео из TikTok, YouTube, Twitter, VK и других платформ.")

    elif action == "history":
        if "history_loaded" not in context.user_data:
            context.user_data["history"] = load_history_from_db(user.id)
            context.user_data["history_loaded"] = True
        history = context.user_data.get("history", [])
        if not history:
            await safe_edit(query, t(context, "history_empty"), reply_markup=back_keyboard())
        else:
            kb = InlineKeyboardMarkup(
                history_keyboard(history).inline_keyboard +
                [[InlineKeyboardButton("◀️ Назад", callback_data="menu_back")]]
            )
            await safe_edit(query, t(context, "history_title"), reply_markup=kb)

    elif action == "me":
        data = get_data()
        uid = str(user.id)
        stats = data.get("stats", {})
        user_total = stats.get("users", {}).get(uid, 0)
        if user_total == 0:
            text = t(context, "me_empty")
        else:
            user_platforms = data.get("user_platforms", {}).get(uid, {})
            fav = max(user_platforms.items(), key=lambda x: x[1])[0] if user_platforms else "—"
            today_count = data.get("downloads_today", {}).get(uid, 0)
            text = t(context, "me", total=user_total, fav=fav, today=today_count, limit=DAILY_LIMIT)
        await safe_edit(query, text, reply_markup=back_keyboard())

    elif action == "patchnote":
        notes = PATCH_NOTES.get(BOT_VERSION)
        lang = get_lang(context)
        text = notes.get(lang, notes.get("ru", "")) if notes else f"Версия {BOT_VERSION} — нет патч-нотов."
        await safe_edit(query, text, reply_markup=back_keyboard())

    elif action == "help":
        await safe_edit(query, t(context, "help"), reply_markup=back_keyboard())

    elif action == "lang":
        await safe_edit(query, "🌍 Выбери язык / Choose language:", reply_markup=lang_menu_keyboard())

    elif action == "stats" and is_admin:
        data = get_data()
        stats = data.get("stats", {})
        total = stats.get("total", 0)
        platforms = stats.get("platforms", {})
        top = sorted(platforms.items(), key=lambda x: x[1], reverse=True)
        top_str = "\n".join(f"  {p}: {c}" for p, c in top[:5]) or "  —"
        await safe_edit(query, 
            f"📊 Статистика бота:\n\n"
            f"Всего скачиваний: {total}\n"
            f"Уникальных пользователей: {len(stats.get('users', {}))}\n"
            f"Активных в сессии: {len(ACTIVE_USERS)}\n"
            f"Заблокировано: {len(data.get('blocked', []))}\n\n"
            f"Топ платформы:\n{top_str}",
            reply_markup=back_keyboard()
        )

    elif action == "blocks" and is_admin:
        data = get_data()
        blocked = data.get("blocked", [])
        if not blocked:
            await safe_edit(query, "✅ Заблокированных нет.", reply_markup=back_keyboard())
        else:
            await safe_edit(query, 
                f"🚫 Заблокировано: {len(blocked)}\nНажми чтобы разблокировать:",
                reply_markup=admin_blocks_keyboard(blocked)
            )

    elif action == "sendpatch" and is_admin:
        notes = PATCH_NOTES.get(BOT_VERSION)
        if not notes:
            await safe_edit(query, f"❌ Нет патч-нота для v{BOT_VERSION}.", reply_markup=back_keyboard())
            return
        if not ACTIVE_USERS:
            await safe_edit(query, "📭 Нет активных пользователей.", reply_markup=back_keyboard())
            return
        await safe_edit(query, f"📤 Рассылаю v{BOT_VERSION}...")
        sent = 0
        failed = 0
        for uid, lang in ACTIVE_USERS.items():
            if uid == ADMIN_ID:
                continue
            try:
                text = notes.get(lang, notes.get("ru", ""))
                await query.get_bot().send_message(chat_id=uid, text=text)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.warning(f"Не удалось отправить {uid}: {e}")
                failed += 1
        await safe_edit(query, 
            f"✅ Разослано!\n📨 Доставлено: {sent}\n❌ Не доставлено: {failed}",
            reply_markup=back_keyboard()
        )

async def handle_adm_unblock_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        return
    uid = int(query.data.replace("adm_unblock_", ""))
    data = get_data()
    blocked = data.get("blocked", [])
    if uid in blocked:
        blocked.remove(uid)
        save_data(data)
    if not blocked:
        await safe_edit(query, "✅ Все разблокированы.", reply_markup=back_keyboard())
    else:
        await safe_edit(query, 
            f"✅ Разблокировано! Осталось: {len(blocked)}",
            reply_markup=admin_blocks_keyboard(blocked)
        )

async def handle_lang_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = query.data.replace("lang_", "")
    context.user_data["lang"] = lang
    ACTIVE_USERS[query.from_user.id] = lang
    data = get_data()
    data.setdefault("user_langs", {})[str(query.from_user.id)] = lang
    save_data(data)
    is_admin = query.from_user.id == ADMIN_ID
    try:
        await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin))
    except Exception:
        await query.edit_message_caption(caption=t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin))

async def handle_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "history_close":
        await query.delete_message()
        return

    idx = int(query.data.replace("history_", ""))
    history = context.user_data.get("history", [])
    if idx >= len(history):
        await safe_edit(query, "❌ Запись не найдена.")
        return

    item = history[idx]
    context.user_data["pending_url"] = item["url"]
    context.user_data["platform"] = item["platform"]
    context.user_data["cancel_flag"] = {"cancelled": False}
    context.user_data["trim_start"] = None
    context.user_data["trim_end"] = None
    context.user_data["subtitles"] = False
    context.user_data["waiting_trim"] = False

    allowed, remaining = check_limit(update.effective_user.id)
    if not allowed:
        await safe_edit(query, "⛔ Дневной лимит исчерпан.")
        return

    await safe_edit(query, 
        f"🎬 Повтор: {item['platform']}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard()
    )

async def handle_format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    fmt = query.data.replace("fmt_", "")
    context.user_data["format"] = fmt
    platform = context.user_data.get("platform", "Видео")

    if fmt == "audio":
        context.user_data["quality"] = "best"
        await safe_edit(query, f"🎵 {platform} • MP3\n\n🔊 Шаг 2 — уровень звука:", reply_markup=audio_keyboard())
    elif fmt == "gif":
        context.user_data["quality"] = "480"
        context.user_data["audio"] = "mute"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"🌀 {platform} • GIF\n\n✂️ Хочешь обрезать?", reply_markup=trim_keyboard())
    elif fmt == "playlist":
        context.user_data["audio"] = "normal"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"📋 {platform} • Плейлист\n\n📐 Шаг 2 — качество:", reply_markup=quality_keyboard())
    else:
        await safe_edit(query, f"🎬 {platform} • Видео\n\n📐 Шаг 2 — качество:", reply_markup=quality_keyboard())

async def handle_quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    quality = query.data.replace("quality_", "")
    context.user_data["quality"] = quality
    fmt = context.user_data.get("format", "video")
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(quality, quality)

    if fmt == "playlist":
        await safe_edit(query, f"⏳ Скачиваю плейлист...\nСкачано видео: 0", reply_markup=cancel_keyboard())
        await _run_download(query.from_user, query.message, context)
        return

    await safe_edit(query, f"🎬 {platform} • {ql}\n\n🔊 Шаг 3 — уровень звука:", reply_markup=audio_keyboard())

async def handle_audio_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    audio = query.data.replace("audio_", "")
    context.user_data["audio"] = audio
    fmt = context.user_data.get("format", "video")
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
    _, al = AUDIO_OPTIONS.get(audio, (1.0, "🔊 Обычный"))

    if fmt == "audio":
        context.user_data["orientation"] = "original"
        context.user_data["subtitles"] = False
        await safe_edit(query, f"⏳ Скачиваю...\n{make_progress_bar(0)}", reply_markup=cancel_keyboard())
        await _run_download(query.from_user, query.message, context)
        return

    subs_on = context.user_data.get("subtitles", False)
    await safe_edit(query, 
        f"🎬 {platform} • {ql} • {al}\n\n"
        f"📐 Шаг 4 — ориентация и доп. настройки:",
        reply_markup=orientation_keyboard(subs_on)
    )

async def handle_orientation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
    _, al = AUDIO_OPTIONS.get(context.user_data.get("audio", "normal"), (1.0, "🔊 Обычный"))

    if data == "orient_toggle_subs":
        current = context.user_data.get("subtitles", False)
        context.user_data["subtitles"] = not current
        subs_on = context.user_data["subtitles"]
        trim_s = context.user_data.get("trim_start")
        trim_e = context.user_data.get("trim_end")
        trim_info = f"\n✂️ Обрезка: {trim_s} → {trim_e}" if trim_s and trim_e else ""
        await safe_edit(query, 
            f"🎬 {platform} • {ql} • {al}{trim_info}\n\nВыбери ориентацию или нажми «Скачать»:",
            reply_markup=orientation_keyboard(subs_on)
        )
        return

    if data == "orient_trim":
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, 
            "✂️ Введи время начала обрезки (М:СС)\nНапример: 0:15 или 1:30"
        )
        return

    if data == "orient_download":
        if "orientation" not in context.user_data:
            context.user_data["orientation"] = "original"
        await safe_edit(query, f"⏳ Скачиваю...\n{make_progress_bar(0)}", reply_markup=cancel_keyboard())
        await _run_download(query.from_user, query.message, context)
        return

    orient = data.replace("orient_", "")
    context.user_data["orientation"] = orient
    orient_labels = {"original": "📱 Оригинал", "square": "⬛ Квадрат", "landscape": "🖼 Горизонталь"}
    subs_on = context.user_data.get("subtitles", False)
    trim_s = context.user_data.get("trim_start")
    trim_e = context.user_data.get("trim_end")
    trim_info = f"\n✂️ Обрезка: {trim_s} → {trim_e}" if trim_s and trim_e else ""
    await safe_edit(query, 
        f"🎬 {platform} • {ql} • {al}\n"
        f"📐 {orient_labels.get(orient, orient)}{trim_info}\n\n"
        f"Нажми «Скачать» или измени опции:",
        reply_markup=orientation_keyboard(subs_on)
    )

async def handle_trim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "trim_no":
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        context.user_data["subtitles"] = False
        await safe_edit(query, f"⏳ Скачиваю...\n{make_progress_bar(0)}", reply_markup=cancel_keyboard())
        await _run_download(query.from_user, query.message, context)
    else:
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, 
            "✂️ Введи время начала обрезки (М:СС)\nНапример: 0:15 или 1:30"
        )

async def handle_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Отменяю...")
    flag = context.user_data.get("cancel_flag", {})
    flag["cancelled"] = True
    await safe_edit(query, "❌ Загрузка отменена.")

# ─── Финальное скачивание ─────────────────────────────────────────────────────

async def _run_download(user, status_msg, context: ContextTypes.DEFAULT_TYPE):
    url         = context.user_data.get("pending_url")
    quality     = context.user_data.get("quality", "best")
    fmt         = context.user_data.get("format", "video")
    audio       = context.user_data.get("audio", "normal")
    orient      = context.user_data.get("orientation", "original")
    trim_s      = context.user_data.get("trim_start")
    trim_e      = context.user_data.get("trim_end")
    subtitles   = context.user_data.get("subtitles", False)
    platform    = context.user_data.get("platform", "Видео")
    cancel_flag = context.user_data.get("cancel_flag", {"cancelled": False})
    volume, audio_label = AUDIO_OPTIONS.get(audio, (1.0, "🔊 Обычный"))
    ql = QUALITY_LABELS.get(quality, quality)
    files_to_clean = []

    if not url:
        await status_msg.edit_text("❌ Ссылка устарела. Отправь заново.")
        return

    try:
        if fmt == "playlist":
            zip_path = await download_playlist(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag)
            if cancel_flag.get("cancelled") or not zip_path or not zip_path.exists():
                await status_msg.edit_text("❌ Не удалось скачать плейлист.")
                return
            await status_msg.edit_text("📤 Отправляю архив...")
            with open(zip_path, "rb") as f:
                await status_msg.reply_document(document=f, caption=f"✅ Плейлист • {ql}")
            await status_msg.delete()
            zip_path.unlink(missing_ok=True)
            update_stats(user.id, platform)
            increment_limit(user.id)
            add_to_history(context, url, platform)
            await _notify_admin(user, platform, fmt, context)
            return

        file_path = await download_video(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag, fmt)

        if cancel_flag.get("cancelled"):
            await status_msg.edit_text("❌ Загрузка отменена.")
            return

        if not file_path or not file_path.exists():
            await status_msg.edit_text(
                "❌ Не удалось скачать.\n• Приватный аккаунт\n• Видео удалено\n• Требуется авторизация"
            )
            return

        files_to_clean.append(file_path)
        current = file_path

        if trim_s and trim_e and fmt != "audio":
            await status_msg.edit_text("✂️ Обрезаю видео...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_trim, current, trim_s, trim_e)
            if current != file_path:
                files_to_clean.append(current)

        if fmt == "gif":
            await status_msg.edit_text("🌀 Конвертирую в GIF...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, convert_to_gif, current)
            if current not in files_to_clean:
                files_to_clean.append(current)

        if fmt == "video" and orient != "original":
            await status_msg.edit_text("📐 Применяю ориентацию...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_orientation, current, orient)
            if current not in files_to_clean:
                files_to_clean.append(current)

        subs_warning = None
        if subtitles and fmt == "video":
            await status_msg.edit_text("📝 Добавляю субтитры...")
            new, subs_warning = await _add_subtitles(url, current, platform)
            if new != current and new not in files_to_clean:
                files_to_clean.append(new)
            current = new

        if fmt == "video" and volume != 1.0:
            await status_msg.edit_text("🎚️ Обрабатываю звук...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_audio, current, volume)
            if current not in files_to_clean:
                files_to_clean.append(current)

        if current.stat().st_size > MAX_FILE_MB * 1024 * 1024:
            await status_msg.edit_text(f"❌ Файл больше {MAX_FILE_MB} МБ. Попробуй качество пониже.")
            return

        await status_msg.edit_text("📤 Отправляю...")

        if fmt == "audio":
            caption = f"🎵 {platform} • MP3"
        elif fmt == "gif":
            caption = f"🌀 {platform} • GIF"
        else:
            caption = f"✅ {platform} • {ql}"
            if audio_label != "🔊 Обычный":
                caption += f" • {audio_label}"
        if subs_warning:
            caption += f"\n{subs_warning}"

        with open(current, "rb") as f:
            if fmt == "audio":
                await status_msg.reply_audio(audio=f, caption=caption)
            elif fmt == "gif":
                await status_msg.reply_animation(animation=f, caption=caption)
            else:
                await status_msg.reply_video(video=f, caption=caption, supports_streaming=True)

        await status_msg.delete()
        update_stats(user.id, platform)
        increment_limit(user.id)
        add_to_history(context, url, platform)
        await _notify_admin(user, platform, fmt, context)

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        try:
            await status_msg.edit_text("❌ Ошибка при обработке.")
        except Exception:
            pass
    finally:
        for f in files_to_clean:
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass

# ─── Запуск ───────────────────────────────────────────────────────────────────

def main() -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",     start))
    app.add_handler(CommandHandler("menu",      menu_command))
    app.add_handler(CommandHandler("help",      help_command))
    app.add_handler(CommandHandler("history",   history_command))
    app.add_handler(CommandHandler("me",        me_command))
    app.add_handler(CommandHandler("patchnote", patchnote_command))
    app.add_handler(CommandHandler("sendpatch", sendpatch_command))
    app.add_handler(CommandHandler("stats",     stats_command))
    app.add_handler(CommandHandler("block",     block_command))
    app.add_handler(CommandHandler("unblock",   unblock_command))

    app.add_handler(CallbackQueryHandler(handle_menu_callback,        pattern="^menu_"))
    app.add_handler(CallbackQueryHandler(handle_adm_unblock_callback, pattern="^adm_unblock_"))
    app.add_handler(CallbackQueryHandler(handle_lang_callback,        pattern="^lang_"))
    app.add_handler(CallbackQueryHandler(handle_history_callback,     pattern="^history_"))
    app.add_handler(CallbackQueryHandler(handle_format_callback,      pattern="^fmt_"))
    app.add_handler(CallbackQueryHandler(handle_quality_callback,     pattern="^quality_"))
    app.add_handler(CallbackQueryHandler(handle_audio_callback,       pattern="^audio_"))
    app.add_handler(CallbackQueryHandler(handle_orientation_callback, pattern="^orient_"))
    app.add_handler(CallbackQueryHandler(handle_trim_callback,        pattern="^trim_"))
    app.add_handler(CallbackQueryHandler(handle_cancel_callback,      pattern="^cancel_download"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Бот запущен...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
