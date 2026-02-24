import re
import os
import asyncio
import logging
import subprocess
import json
import zipfile
import random
import shutil
import glob as glob_module
import sys
from pathlib import Path
from datetime import datetime, date

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
import yt_dlp

# ─── ffmpeg ───────────────────────────────────────────────────────────────────

def _setup_ffmpeg():
    if shutil.which("ffmpeg"):
        logging.info("ffmpeg найден в системе")
        return
    try:
        import imageio_ffmpeg
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        os.environ["PATH"] = str(Path(ffmpeg_path).parent) + os.pathsep + os.environ.get("PATH", "")
        logging.info(f"ffmpeg найден через imageio-ffmpeg: {ffmpeg_path}")
        return
    except Exception as e:
        logging.warning(f"imageio-ffmpeg: {e}")
    results = glob_module.glob("/nix/store/*/bin/ffmpeg")
    if results:
        os.environ["PATH"] = str(Path(results[0]).parent) + os.pathsep + os.environ.get("PATH", "")
        logging.info(f"ffmpeg найден в nix store: {results[0]}")
        return
    try:
        subprocess.run(["apt-get", "install", "-y", "-q", "ffmpeg"], capture_output=True, timeout=120)
        if shutil.which("ffmpeg"):
            logging.info("ffmpeg установлен через apt-get")
            return
    except Exception as e:
        logging.warning(f"apt-get: {e}")
    logging.error("ffmpeg НЕ НАЙДЕН!")

_setup_ffmpeg()

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Конфиг ───────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "TOKEN_HERE")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "balerndownloadsbot")
BOT_VERSION  = os.environ.get("BOT_VERSION", "1.3")
ADMIN_ID     = int(os.environ.get("ADMIN_ID", "123456789"))
DAILY_LIMIT  = 20
HISTORY_SIZE = 10
MAX_FILE_MB  = 50

DATA_DIR     = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR = DATA_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
DATA_FILE    = DATA_DIR / "data.json"

# ─── Redis & Webhook конфиг ──────────────────────────────────────────────────

REDIS_URL      = os.environ.get("REDIS_URL")          # redis://... или None
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL")         # https://app.railway.app
WEBHOOK_PORT   = int(os.environ.get("PORT", "8443"))   # Railway выставляет PORT сам
WEBHOOK_PATH   = f"/webhook/{BOT_TOKEN}"

ACTIVE_USERS: dict[int, str] = {}

# Очередь загрузок: user_id -> asyncio.Lock
DOWNLOAD_LOCKS: dict[int, asyncio.Lock] = {}
DOWNLOAD_QUEUE: asyncio.Queue = asyncio.Queue()

# ─── Патч-ноты ────────────────────────────────────────────────────────────────

PATCH_NOTES = {
    "1.1": {
        "ru": (
            "🆕 Обновление v1.1\n\n"
            "• 🎛 Интерактивное меню /menu\n"
            "• 🌍 Поддержка английского языка\n"
            "• 📊 Команда /me — личная статистика\n"
            "• 📤 Кнопка «Поделиться ботом»\n"
            "• 💾 История скачиваний"
        ),
        "en": (
            "🆕 Update v1.1\n\n"
            "• 🎛 Interactive menu /menu\n"
            "• 🌍 English language support\n"
            "• 📊 /me command — personal stats\n"
            "• 📤 Share bot button\n"
            "• 💾 Download history"
        ),
    },
    "1.2": {
        "ru": (
            "🆕 Обновление v1.2\n\n"
            "• 🖼 Скачивание обложки видео\n"
            "• ⭕ Конвертация в кружочек Telegram\n"
            "• 📋 Очередь загрузок — больше не отказывает\n"
            "• ⚡ Ускорение и замедление видео\n"
            "• 🎬 Поддержка обычных YouTube видео (не только Shorts)"
        ),
        "en": (
            "🆕 Update v1.2\n\n"
            "• 🖼 Download video thumbnail\n"
            "• ⭕ Convert to Telegram video note (circle)\n"
            "• 📋 Download queue — no more rejections\n"
            "• ⚡ Speed up or slow down video\n"
            "• 🎬 Regular YouTube videos supported (not only Shorts)"
        ),
    },
    "1.3": {
        "ru": (
            "🆕 Обновление v1.3\n\n"
            "• 🚀 Webhook — бот реагирует мгновенно\n"
            "• 🗄 Redis — история и статистика не сбрасываются при перезапуске\n"
            "• 📦 Автосжатие видео >50 МБ вместо отказа\n"
            "• 🔄 Автообновление yt-dlp раз в неделю\n"
            "• 📋 Очередь переживает перезапуск сервера"
        ),
        "en": (
            "🆕 Update v1.3\n\n"
            "• 🚀 Webhook — instant bot responses\n"
            "• 🗄 Redis — history and stats survive restarts\n"
            "• 📦 Auto-compress videos >50 MB instead of failing\n"
            "• 🔄 Auto-update yt-dlp weekly\n"
            "• 📋 Download queue survives server restarts"
        ),
    },
}

# ─── Переводы ─────────────────────────────────────────────────────────────────

TEXTS = {
    "ru": {
        "start_caption": (
            "👋 О, новый пользователь! Уже загружаю котиков... шучу.\n\n"
            "Я скачиваю видео из TikTok, YouTube, Twitter, VK и других платформ.\n"
            "Просто кинь ссылку — выбери формат, качество и получи файл!\n\n"
            "Поехали! 🚀"
        ),
        "help": (
            "📌 Как пользоваться:\n\n"
            "1. Отправь ссылку на видео\n"
            "2. Выбери формат: видео / MP3 / GIF / кружочек / обложка / плейлист\n"
            "3. Выбери качество и уровень звука\n"
            "4. Ориентация, субтитры, обрезка, скорость — или сразу «Скачать»\n\n"
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
        "remaining": "Осталось сегодня: {remaining}",
        "fmt_video": "🎬 Видео (MP4)",
        "fmt_audio": "🎵 Аудио (MP3)",
        "fmt_gif":   "🌀 GIF",
        "fmt_circle":"⭕ Кружочек",
        "fmt_thumb": "🖼 Обложка",
        "fmt_playlist":"📋 Плейлист (ZIP)",
        "step_quality": "📐 Шаг 2 — качество:",
        "step_audio":   "🔊 Шаг 3 — уровень звука:",
        "step_orient":  "📐 Шаг 4 — ориентация, субтитры, скорость:",
        "step_speed":   "⚡ Выбери скорость воспроизведения:",
        "step_trim":    "✂️ Хочешь обрезать?",
        "step_trim_gif":"✂️ Хочешь обрезать? (макс. 60 сек)",
        "circle_menu": "⭕ Кружочек готов к скачке\n\n📐 Настрой параметры или скачай:",
        "circle_speed": "⚡ Скорость: {speed}x",
        "circle_audio": "🔊 Звук: {label}",
        "circle_download": "⬇️ Скачать кружочек",
        "trim_yes": "✂️ Обрезать видео",
        "trim_no":  "⏭ Без обрезки",
        "orient_original":  "📱 Оригинал",
        "orient_square":    "⬛ Квадрат (1:1)",
        "orient_landscape": "🖼 Горизонталь (16:9)",
        "subs_on":   "📝 Субтитры: ВКЛ ✅",
        "subs_off":  "📝 Субтитры: ВЫКЛ ❌",
        "speed_btn": "⚡ Скорость: {speed}x",
        "trim_btn":  "✂️ Обрезать видео",
        "download_btn": "⬇️ Скачать сейчас",
        "audio_mute":   "🔇 Без звука",
        "audio_quiet":  "🔉 Тише",
        "audio_normal": "🔊 Обычный",
        "audio_loud":   "📢 Громче",
        "q360": "360p", "q480": "480p", "q720": "720p HD", "q1080": "1080p FHD", "qbest": "🏆 Максимальное",
        "speed_half": "🐢 0.5x", "speed_075": "🐌 0.75x", "speed_1": "▶️ 1x",
        "speed_15": "🐇 1.5x", "speed_2": "⚡ 2x",
        "cancel_btn": "❌ Отменить",
        "back_btn": "◀️ Назад",
        "downloading": "⏳ Скачиваю...",
        "trim_enter_start": "✂️ Введи время начала обрезки (М:СС)\nНапример: 0:15 или 1:30",
        "trim_enter_end": "Теперь введи время конца:",
        "trim_start_ok": "✅ Начало: {start}\n\n",
        "trim_invalid": "❌ Неверный формат. Например: 0:15 или 1:30:00",
        "me": (
            "👤 Твоя статистика:\n\n"
            "📥 Всего скачиваний: {total}\n"
            "❤️ Любимая платформа: {fav}\n"
            "📅 Сегодня: {today} из {limit}"
        ),
        "me_empty": "📭 Ты ещё ничего не скачивал!",
        "menu_title": "🎛 Главное меню:",
        "queued": "⏳ Ты в очереди ({pos}). Подожди...",
    },
    "en": {
        "start_caption": (
            "👋 Oh, a new user! Already loading cats... just kidding.\n\n"
            "I download videos from TikTok, YouTube, Twitter, VK and more.\n"
            "Just send a link — choose format, quality and get your file!\n\n"
            "Let's go! 🚀"
        ),
        "help": (
            "📌 How to use:\n\n"
            "1. Send a video link\n"
            "2. Choose format: video / MP3 / GIF / circle / thumbnail / playlist\n"
            "3. Choose quality and audio level\n"
            "4. Orientation, subtitles, trim, speed — or just Download\n\n"
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
        "fmt_video": "🎬 Video (MP4)",
        "fmt_audio": "🎵 Audio (MP3)",
        "fmt_gif":   "🌀 GIF",
        "fmt_circle":"⭕ Circle",
        "fmt_thumb": "🖼 Thumbnail",
        "fmt_playlist":"📋 Playlist (ZIP)",
        "step_quality": "📐 Step 2 — quality:",
        "step_audio":   "🔊 Step 3 — audio level:",
        "step_orient":  "📐 Step 4 — orientation, subtitles, speed:",
        "step_speed":   "⚡ Choose playback speed:",
        "step_trim":    "✂️ Do you want to trim?",
        "step_trim_gif":"✂️ Do you want to trim? (max 60 sec)",
        "circle_menu": "⭕ Circle ready\n\n📐 Adjust settings or download:",
        "circle_speed": "⚡ Speed: {speed}x",
        "circle_audio": "🔊 Audio: {label}",
        "circle_download": "⬇️ Download circle",
        "trim_yes": "✂️ Trim video",
        "trim_no":  "⏭ No trim",
        "orient_original":  "📱 Original",
        "orient_square":    "⬛ Square (1:1)",
        "orient_landscape": "🖼 Landscape (16:9)",
        "subs_on":   "📝 Subtitles: ON ✅",
        "subs_off":  "📝 Subtitles: OFF ❌",
        "speed_btn": "⚡ Speed: {speed}x",
        "trim_btn":  "✂️ Trim video",
        "download_btn": "⬇️ Download now",
        "audio_mute":   "🔇 Mute",
        "audio_quiet":  "🔉 Quiet",
        "audio_normal": "🔊 Normal",
        "audio_loud":   "📢 Loud",
        "q360": "360p", "q480": "480p", "q720": "720p HD", "q1080": "1080p FHD", "qbest": "🏆 Maximum",
        "speed_half": "🐢 0.5x", "speed_075": "🐌 0.75x", "speed_1": "▶️ 1x",
        "speed_15": "🐇 1.5x", "speed_2": "⚡ 2x",
        "cancel_btn": "❌ Cancel",
        "back_btn": "◀️ Back",
        "downloading": "⏳ Downloading...",
        "trim_enter_start": "✂️ Enter start time (M:SS)\nExample: 0:15 or 1:30",
        "trim_enter_end": "Now enter end time:",
        "trim_start_ok": "✅ Start: {start}\n\n",
        "trim_invalid": "❌ Invalid format. Example: 0:15 or 1:30:00",
        "queued": "⏳ You are in queue ({pos}). Please wait...",
        "me": (
            "👤 Your statistics:\n\n"
            "📥 Total downloads: {total}\n"
            "❤️ Favourite platform: {fav}\n"
            "📅 Today: {today} of {limit}"
        ),
        "me_empty": "📭 You haven't downloaded anything yet!",
        "menu_title": "🎛 Main menu:",
        "queued": "⏳ You are in queue ({pos}). Please wait...",
    }
}

def get_lang(context) -> str:
    return context.user_data.get("lang", "ru")

def t(context, key: str, **kwargs) -> str:
    lang = get_lang(context)
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, key)
    return text.format(**kwargs) if kwargs else text

# ─── Хранилище данных (Redis + JSON fallback) ────────────────────────────────

class Storage:
    """Прозрачная обёртка: Redis если доступен, иначе data.json."""
    _redis = None

    @classmethod
    def init(cls):
        if not REDIS_URL:
            logger.info("REDIS_URL не задан — используем data.json")
            return
        try:
            import redis as redis_lib
            cls._redis = redis_lib.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
            cls._redis.ping()
            logger.info("✅ Redis подключён")
            # Мигрируем существующий data.json в Redis при первом запуске
            if DATA_FILE.exists() and not cls._redis.exists("bot:data"):
                try:
                    raw = DATA_FILE.read_text(encoding="utf-8")
                    cls._redis.set("bot:data", raw)
                    logger.info("data.json мигрирован в Redis")
                except Exception as e:
                    logger.warning(f"Миграция data.json → Redis: {e}")
        except Exception as e:
            logger.warning(f"Redis недоступен, используем data.json: {e}")
            cls._redis = None

    @classmethod
    def load(cls) -> dict:
        if cls._redis:
            try:
                raw = cls._redis.get("bot:data")
                if raw:
                    return json.loads(raw)
            except Exception as e:
                logger.warning(f"Redis load error: {e}")
        # Fallback — файл
        if DATA_FILE.exists():
            try:
                return json.loads(DATA_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"stats": {}, "blocked": [], "downloads_today": {}, "last_reset": str(date.today())}

    @classmethod
    def save(cls, data: dict):
        serialized = json.dumps(data, ensure_ascii=False, indent=2)
        if cls._redis:
            try:
                cls._redis.set("bot:data", serialized)
                return
            except Exception as e:
                logger.warning(f"Redis save error: {e}")
        # Fallback — файл
        try:
            DATA_FILE.write_text(serialized, encoding="utf-8")
        except Exception as e:
            logger.error(f"Не удалось сохранить data.json: {e}")


def load_data() -> dict:
    return Storage.load()

def save_data(data: dict):
    Storage.save(data)

def get_data() -> dict:
    data = load_data()
    if data.get("last_reset") != str(date.today()):
        data["downloads_today"] = {}
        data["last_reset"] = str(date.today())
        save_data(data)
    return data

# ─── Платформы ────────────────────────────────────────────────────────────────

SUPPORTED_PATTERNS = [
    r"tiktok\.com", r"vm\.tiktok\.com",
    r"instagram\.com", r"instagr\.am",
    r"youtube\.com/shorts", r"youtube\.com/watch", r"youtu\.be",
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

SPEED_OPTIONS = {
    "0.5": "🐢 0.5x (замедлить)",
    "0.75": "🐌 0.75x",
    "1.0": "▶️ 1x (обычная)",
    "1.5": "🐇 1.5x",
    "2.0": "⚡ 2x (ускорить)",
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
        (r"instagram\.com|instagr\.am", "Instagram"),
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
    plats = stats.setdefault("platforms", {})
    plats[platform] = plats.get(platform, 0) + 1
    users = stats.setdefault("users", {})
    users[uid] = users.get(uid, 0) + 1
    up = data.setdefault("user_platforms", {}).setdefault(uid, {})
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

# ─── Клавиатуры ───────────────────────────────────────────────────────────────

MENU_LABELS = {
    "ru": {
        "download":  "⬇️ Скачать видео",
        "history":   "🕘 История",
        "me":        "📊 Моя статистика",
        "patchnote": "📋 Патч-ноут",
        "help":      "❓ Помощь",
        "lang":      "🌍 Язык",
        "share":     "📤 Поделиться",
        "stats":     "📊 Стат. бота",
        "blocks":    "🚫 Блокировки",
        "sendpatch": "📢 Разослать патч-ноут",
        "share_text": f"Скачиваю видео из TikTok, YouTube и не только! @balerndownloadsbot",
    },
    "en": {
        "download":  "⬇️ Download video",
        "history":   "🕘 History",
        "me":        "📊 My stats",
        "patchnote": "📋 Patch notes",
        "help":      "❓ Help",
        "lang":      "🌍 Language",
        "share":     "📤 Share bot",
        "stats":     "📊 Bot stats",
        "blocks":    "🚫 Blocks",
        "sendpatch": "📢 Send patch note",
        "share_text": f"Download videos from TikTok, YouTube and more! @balerndownloadsbot",
    },
}

def main_menu_keyboard(is_admin: bool = False, lang: str = "ru") -> InlineKeyboardMarkup:
    L = MENU_LABELS.get(lang, MENU_LABELS["ru"])
    rows = [
        [InlineKeyboardButton(L["download"],  callback_data="menu_download"),
         InlineKeyboardButton(L["history"],   callback_data="menu_history")],
        [InlineKeyboardButton(L["me"],        callback_data="menu_me"),
         InlineKeyboardButton(L["patchnote"], callback_data="menu_patchnote")],
        [InlineKeyboardButton(L["help"],      callback_data="menu_help"),
         InlineKeyboardButton(L["lang"],      callback_data="menu_lang")],
        [InlineKeyboardButton(L["share"],     switch_inline_query=L["share_text"])],
    ]
    if is_admin:
        rows.append([
            InlineKeyboardButton(L["stats"],     callback_data="menu_stats"),
            InlineKeyboardButton(L["blocks"],    callback_data="menu_blocks"),
        ])
        rows.append([InlineKeyboardButton(L["sendpatch"], callback_data="menu_sendpatch")])
    return InlineKeyboardMarkup(rows)

def patchnote_keyboard(version: str) -> InlineKeyboardMarkup:
    versions = list(PATCH_NOTES.keys())
    idx = versions.index(version) if version in versions else len(versions) - 1
    rows = []
    nav = []
    if idx > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"patch_nav_{versions[idx-1]}"))
    nav.append(InlineKeyboardButton(f"v{version}", callback_data="patch_noop"))
    if idx < len(versions) - 1:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"patch_nav_{versions[idx+1]}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_back")])
    return InlineKeyboardMarkup(rows)

def persistent_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🎛 Меню")]],
        resize_keyboard=True,
        is_persistent=True
    )

def back_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([[InlineKeyboardButton(T["back_btn"], callback_data="menu_back")]])

def lang_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("◀️ Назад",    callback_data="menu_back")],
    ])

def admin_blocks_keyboard(blocked: list) -> InlineKeyboardMarkup:
    rows = []
    for uid in blocked[:10]:
        rows.append([InlineKeyboardButton(f"🔓 {uid}", callback_data=f"adm_unblock_{uid}")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_back")])
    return InlineKeyboardMarkup(rows)

def format_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["fmt_video"],    callback_data="fmt_video"),
         InlineKeyboardButton(T["fmt_audio"],    callback_data="fmt_audio")],
        [InlineKeyboardButton(T["fmt_gif"],      callback_data="fmt_gif"),
         InlineKeyboardButton(T["fmt_circle"],   callback_data="fmt_circle")],
        [InlineKeyboardButton(T["fmt_thumb"],    callback_data="fmt_thumb"),
         InlineKeyboardButton(T["fmt_playlist"], callback_data="fmt_playlist")],
    ])

def quality_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["q360"],  callback_data="quality_360"),
         InlineKeyboardButton(T["q480"],  callback_data="quality_480")],
        [InlineKeyboardButton(T["q720"],  callback_data="quality_720"),
         InlineKeyboardButton(T["q1080"], callback_data="quality_1080")],
        [InlineKeyboardButton(T["qbest"], callback_data="quality_best")],
    ])

def audio_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["audio_mute"],   callback_data="audio_mute"),
         InlineKeyboardButton(T["audio_quiet"],  callback_data="audio_quiet")],
        [InlineKeyboardButton(T["audio_normal"], callback_data="audio_normal"),
         InlineKeyboardButton(T["audio_loud"],   callback_data="audio_loud")],
    ])

def speed_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["speed_half"], callback_data="speed_0.5"),
         InlineKeyboardButton(T["speed_075"],  callback_data="speed_0.75")],
        [InlineKeyboardButton(T["speed_1"],    callback_data="speed_1.0"),
         InlineKeyboardButton(T["speed_15"],   callback_data="speed_1.5")],
        [InlineKeyboardButton(T["speed_2"],    callback_data="speed_2.0")],
    ])

def circle_menu_keyboard(speed: str = "1.0", audio: str = "normal", lang: str = "ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    _, audio_label = AUDIO_OPTIONS.get(audio, (1.0, "🔊 Обычный"))
    # Для English адаптируем label
    audio_labels_en = {"mute": "🔇 Mute", "quiet": "🔉 Quiet", "normal": "🔊 Normal", "loud": "📢 Loud"}
    if lang == "en":
        audio_label = audio_labels_en.get(audio, audio_label)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["circle_speed"].format(speed=speed), callback_data="circle_speed"),
         InlineKeyboardButton(T["circle_audio"].format(label=audio_label), callback_data="circle_audio")],
        [InlineKeyboardButton(T["circle_download"], callback_data="circle_download")],
    ])

def orientation_keyboard(subs_on: bool = False, speed: str = "1.0", lang: str = "ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    subs_label  = T["subs_on"] if subs_on else T["subs_off"]
    speed_label = T["speed_btn"].format(speed=speed)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["orient_original"],  callback_data="orient_original"),
         InlineKeyboardButton(T["orient_square"],    callback_data="orient_square")],
        [InlineKeyboardButton(T["orient_landscape"], callback_data="orient_landscape")],
        [InlineKeyboardButton(subs_label,            callback_data="orient_toggle_subs"),
         InlineKeyboardButton(speed_label,           callback_data="orient_speed")],
        [InlineKeyboardButton(T["trim_btn"],         callback_data="orient_trim")],
        [InlineKeyboardButton(T["download_btn"],     callback_data="orient_download")],
    ])

def trim_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["trim_yes"], callback_data="trim_yes"),
         InlineKeyboardButton(T["trim_no"],  callback_data="trim_no")],
    ])

def cancel_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([[InlineKeyboardButton(T["cancel_btn"], callback_data="cancel_download")]])

def history_keyboard(history: list) -> InlineKeyboardMarkup:
    buttons = []
    for i, item in enumerate(history):
        platform = item.get("platform", "Видео")
        ts = item.get("time", "")[:10]
        buttons.append([InlineKeyboardButton(f"{platform} • {ts}", callback_data=f"history_{i}")])
    buttons.append([InlineKeyboardButton("❌ Закрыть", callback_data="history_close")])
    return InlineKeyboardMarkup(buttons)

# ─── Утилиты ffmpeg ───────────────────────────────────────────────────────────

def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None

def ffmpeg_run(cmd: list) -> bool:
    if not ffmpeg_available():
        logger.warning("ffmpeg недоступен")
        return False
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

def apply_speed(input_path: Path, speed: float) -> Path:
    """Ускоряет или замедляет видео. speed=2.0 — вдвое быстрее, 0.5 — вдвое медленнее."""
    if speed == 1.0:
        return input_path
    output_path = input_path.with_stem(input_path.stem + "_speed")
    # atempo поддерживает только 0.5..2.0, для крайних значений нужна цепочка
    if speed >= 0.5:
        atempo = f"atempo={speed}"
    else:
        atempo = f"atempo=0.5,atempo={speed/0.5}"
    cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        "-vf", f"setpts={1/speed}*PTS",
        "-af", atempo,
        "-c:v", "libx264", "-preset", "fast",
        str(output_path)
    ]
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

def convert_to_circle(input_path: Path) -> Path:
    """Конвертирует видео в кружочек Telegram: квадрат 384x384, макс 60 сек."""
    output_path = input_path.with_stem(input_path.stem + "_circle").with_suffix(".mp4")
    cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=384:384",
        "-t", "60",
        "-c:v", "libx264", "-preset", "fast",
        "-c:a", "aac", "-b:a", "128k",
        str(output_path)
    ]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path

def compress_video(input_path: Path, target_mb: float = 45.0) -> Path:
    """Сжимает видео до target_mb через двухпроходный VBR или CRF."""
    output_path = input_path.with_stem(input_path.stem + "_compressed").with_suffix(".mp4")
    # Получаем длительность через ffprobe
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(input_path)],
            capture_output=True, text=True, timeout=30
        )
        duration = float(probe.stdout.strip())
    except Exception:
        duration = 0

    if duration > 0:
        # Целевой битрейт: target_mb * 8 * 1024 / duration (kbps), минус 128 на аудио
        target_kbps = max(200, int(target_mb * 8 * 1024 / duration) - 128)
        cmd = [
            "ffmpeg", "-y", "-i", str(input_path),
            "-c:v", "libx264", "-b:v", f"{target_kbps}k",
            "-c:a", "aac", "-b:a", "128k",
            "-preset", "fast", "-movflags", "+faststart",
            str(output_path)
        ]
    else:
        # Fallback: CRF 28 без точного контроля размера
        cmd = [
            "ffmpeg", "-y", "-i", str(input_path),
            "-c:v", "libx264", "-crf", "28",
            "-c:a", "aac", "-b:a", "96k",
            "-preset", "fast",
            str(output_path)
        ]
    ffmpeg_run(cmd)
    return output_path if output_path.exists() else input_path


def time_str_valid(t: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", t.strip()))

# ─── Скачивание ───────────────────────────────────────────────────────────────

async def download_thumbnail(url: str, output_path: Path) -> Path | None:
    """Скачивает обложку (thumbnail) видео."""
    ydl_opts = {
        "skip_download": True,
        "writethumbnail": True,
        "outtmpl": str(output_path / "thumb_%(id)s"),
        "quiet": True,
        "no_warnings": True,
        "http_headers": {"User-Agent": "com.google.ios.youtube/19.29.1 CFNetwork/1568.100.1 Darwin/24.0.0"},
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "android", "web"],
            }
        },
    }
    loop = asyncio.get_event_loop()

    def _dl():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            # yt-dlp сохраняет thumbnail с расширением из URL
            thumb_id = info.get("id", "thumb")
            for ext in ["jpg", "jpeg", "png", "webp"]:
                p = output_path / f"thumb_{thumb_id}.{ext}"
                if p.exists():
                    return p
            # Ищем любой файл с thumb_
            files = list(output_path.glob(f"thumb_{thumb_id}*"))
            return files[0] if files else None

    try:
        return await loop.run_in_executor(None, _dl)
    except Exception as e:
        logger.error(f"Ошибка скачивания thumbnail: {e}")
        return None

async def download_video(url, quality, output_path, status_msg, cancel_flag, fmt="video", lang="ru") -> Path | None:
    format_str = QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"])
    if fmt == "audio":
        format_str = "bestaudio/best"
    elif fmt in ("gif", "circle"):
        format_str = "best[ext=mp4]/best[ext=webm]/best"

    format_with_fallback = format_str + "/best"
    last_update = {"pct": -1}
    loop = asyncio.get_running_loop()

    def progress_hook(d):
        try:
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
                                reply_markup=cancel_keyboard(lang)
                            ),
                            loop
                        )
        except Exception as hook_err:
            if "CANCELLED" in str(hook_err):
                raise  # пробрасываем отмену
            logger.warning(f"progress_hook error (ignored): {hook_err}")

    ydl_opts = {
        "outtmpl": str(output_path / "%(id)s.%(ext)s"),
        "format": format_with_fallback,
        "merge_output_format": "mp4" if fmt not in ("audio",) else None,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "http_headers": {"User-Agent": "com.google.ios.youtube/19.29.1 CFNetwork/1568.100.1 Darwin/24.0.0"},
        "cookiefile": "cookies.txt" if Path("cookies.txt").exists() else None,
        "socket_timeout": 30,
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "android", "web"],
            }
        },
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
                asyncio.run_coroutine_threadsafe(
                    status_msg.edit_text(
                        f"⏳ 🔄 Попытка {attempt + 1}/3, ждём {delay} сек...",
                        reply_markup=cancel_keyboard(lang)
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
            if attempt == 2:
                return None

    return None

async def download_playlist(url, quality, output_path, status_msg, cancel_flag, lang="ru") -> Path | None:
    playlist_dir = output_path / "playlist_tmp"
    playlist_dir.mkdir(exist_ok=True)
    count = {"n": 0}
    loop = asyncio.get_running_loop()

    def progress_hook(d):
        try:
            if cancel_flag.get("cancelled"):
                raise Exception("CANCELLED")
            if d["status"] == "finished":
                count["n"] += 1
                asyncio.run_coroutine_threadsafe(
                    status_msg.edit_text(
                        f"⏳ Скачиваю плейлист...\nСкачано видео: {count['n']}",
                        reply_markup=cancel_keyboard(lang)
                    ),
                    loop
                )
        except Exception as hook_err:
            if "CANCELLED" in str(hook_err):
                raise
            logger.warning(f"playlist progress_hook error (ignored): {hook_err}")

    ydl_opts = {
        "outtmpl": str(playlist_dir / "%(playlist_index)s_%(title)s.%(ext)s"),
        "format": QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"]),
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "noplaylist": False,
        "playlistend": 20,
        "http_headers": {"User-Agent": "com.google.ios.youtube/19.29.1 CFNetwork/1568.100.1 Darwin/24.0.0"},
        "cookiefile": "cookies.txt" if Path("cookies.txt").exists() else None,
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "android", "web"],
            }
        },
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
    if not ffmpeg_available():
        return video_path, "⚠️ Субтитры недоступны — ffmpeg не установлен"

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
        "http_headers": {"User-Agent": "com.google.ios.youtube/19.29.1 CFNetwork/1568.100.1 Darwin/24.0.0"},
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "android", "web"],
            }
        },
    }
    loop = asyncio.get_event_loop()

    def _dl_subs():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    for attempt in range(3):
        try:
            if attempt > 0:
                await asyncio.sleep(5 * attempt)
            await loop.run_in_executor(None, _dl_subs)
            srt_files = list(video_path.parent.glob(video_path.stem + "*.srt"))
            if not srt_files:
                return video_path, "⚠️ Субтитры недоступны для этого видео"
            srt_file = srt_files[0]
            srt_str = str(srt_file).replace("\\", "/").replace(":", "\\:")
            cmd = ["ffmpeg", "-y", "-i", str(video_path), "-vf", f"subtitles={srt_str}", "-c:a", "copy", str(output_path)]
            ok = ffmpeg_run(cmd)
            srt_file.unlink(missing_ok=True)
            if ok and output_path.exists():
                return output_path, None
            return video_path, "⚠️ Не удалось вшить субтитры"
        except Exception as e:
            err = str(e)
            if "429" in err:
                if attempt < 2:
                    continue
                return video_path, "⚠️ YouTube блокирует субтитры — попробуй позже"
            logger.error(f"Ошибка субтитров: {e}")
            return video_path, "⚠️ Субтитры недоступны"
    return video_path, "⚠️ Не удалось получить субтитры"

async def _notify_admin(user, platform, fmt, context):
    try:
        fmt_labels = {
            "video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF",
            "circle": "⭕ Кружочек", "thumb": "🖼 Обложка", "playlist": "📋 Плейлист"
        }
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

# ─── safe_edit ────────────────────────────────────────────────────────────────

async def safe_edit(query, text, reply_markup=None):
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

# ─── Команды ──────────────────────────────────────────────────────────────────

# URL картинки меню — можно заменить на свою ссылку или оставить imgur
MENU_PHOTO_URL = os.environ.get("MENU_PHOTO_URL", "https://i.imgur.com/4M34hi2.png")
# Кэш file_id после первой отправки — Telegram хранит файл на своих серверах
_MENU_PHOTO_FILE_ID: str | None = None

async def get_menu_photo() -> str:
    """Возвращает file_id если уже загружали, иначе оригинальный URL."""
    return _MENU_PHOTO_FILE_ID or MENU_PHOTO_URL

async def send_menu_photo(target, caption: str, reply_markup, context) -> None:
    """Отправляет меню с картинкой. Кэширует file_id после первой отправки."""
    global _MENU_PHOTO_FILE_ID
    photo = _MENU_PHOTO_FILE_ID or MENU_PHOTO_URL
    try:
        if hasattr(target, 'reply_photo'):
            msg = await target.reply_photo(photo=photo, caption=caption, reply_markup=reply_markup)
        else:
            msg = await context.bot.send_photo(chat_id=target.id, photo=photo, caption=caption, reply_markup=reply_markup)
        # Кэшируем file_id для быстрой повторной отправки
        if not _MENU_PHOTO_FILE_ID and msg.photo:
            _MENU_PHOTO_FILE_ID = msg.photo[-1].file_id
            logger.info(f"Меню фото закэшировано: {_MENU_PHOTO_FILE_ID}")
    except Exception as e:
        logger.warning(f"Не удалось отправить фото меню: {e}")
        # Fallback — текст без фото
        text = caption
        if hasattr(target, 'reply_text'):
            await target.reply_text(text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=target.id, text=text, reply_markup=reply_markup)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ACTIVE_USERS[user.id] = get_lang(context)
    context.user_data["_uid"] = str(user.id)

    # Загружаем сохранённый язык
    data = get_data()
    saved_lang = data.get("user_langs", {}).get(str(user.id))
    if saved_lang and "lang" not in context.user_data:
        context.user_data["lang"] = saved_lang

    is_admin = user.id == ADMIN_ID
    lang = get_lang(context)

    await update.message.reply_text("👇", reply_markup=persistent_menu_keyboard())
    await send_menu_photo(update.message, t(context, "start_caption"), main_menu_keyboard(is_admin, lang), context)

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ACTIVE_USERS[user.id] = get_lang(context)
    lang = get_lang(context)
    await send_menu_photo(update.message, t(context, "menu_title"), main_menu_keyboard(user.id == ADMIN_ID, lang), context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(t(context, "help"))

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    db_history = load_history_from_db(user.id)
    session_history = context.user_data.get("history", [])
    seen, merged = set(), []
    for item in session_history + db_history:
        if item.get("url") not in seen:
            seen.add(item.get("url"))
            merged.append(item)
    history = merged[:HISTORY_SIZE]
    context.user_data["history"] = history
    if not history:
        await update.message.reply_text(t(context, "history_empty"))
        return
    await update.message.reply_text(t(context, "history_title"), reply_markup=history_keyboard(history))

async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    data = get_data()
    uid = str(user.id)
    user_total = data.get("stats", {}).get("users", {}).get(uid, 0)
    if user_total == 0:
        await update.message.reply_text(t(context, "me_empty"))
        return
    user_platforms = data.get("user_platforms", {}).get(uid, {})
    fav = max(user_platforms.items(), key=lambda x: x[1])[0] if user_platforms else "—"
    today_count = data.get("downloads_today", {}).get(uid, 0)
    await update.message.reply_text(
        t(context, "me", total=user_total, fav=fav, today=today_count, limit=DAILY_LIMIT)
    )

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
        await update.message.reply_text(f"❌ Нет патч-нота для v{version}.")
        return
    if not ACTIVE_USERS:
        await update.message.reply_text("📭 Нет активных пользователей.")
        return
    status = await update.message.reply_text(f"📤 Рассылаю для {len(ACTIVE_USERS)} пользователей...")
    sent, failed = 0, 0
    for uid, lang in ACTIVE_USERS.items():
        if uid == ADMIN_ID:
            continue
        try:
            text = notes.get(lang, notes.get("ru", ""))
            await context.bot.send_message(chat_id=uid, text=text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await status.edit_text(f"✅ Разослано!\n📨 Отправлено: {sent}\n❌ Не доставлено: {failed}")

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
        f"Заблокировано: {len(data.get('blocked', []))}\n"
        f"Активных в сессии: {len(ACTIVE_USERS)}\n\n"
        f"Топ платформы:\n{top_str}"
    )

async def block_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
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
        await update.message.reply_text(f"✅ {uid} заблокирован.")
    else:
        await update.message.reply_text("Уже заблокирован.")

async def unblock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
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
        await update.message.reply_text(f"✅ {uid} разблокирован.")
    else:
        await update.message.reply_text("Не был заблокирован.")

# ─── Обработчик текста ────────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    text = update.message.text.strip()

    # Кнопка Меню
    if text == "🎛 Меню":
        ACTIVE_USERS[user.id] = get_lang(context)
        lang = get_lang(context)
        await send_menu_photo(update.message, t(context, "menu_title"), main_menu_keyboard(user.id == ADMIN_ID, lang), context)
        return

    # Ввод времени обрезки
    if context.user_data.get("waiting_trim"):
        if context.user_data.get("trim_start") is None:
            if not time_str_valid(text):
                await update.message.reply_text(t(context, "trim_invalid"))
                return
            context.user_data["trim_start"] = text
            await update.message.reply_text(f"✅ Начало: {text}\n\nТеперь введи время конца:")
        else:
            if not time_str_valid(text):
                await update.message.reply_text("❌ Неверный формат. Например: 0:45 или 2:00:00")
                return
            context.user_data["trim_end"] = text
            context.user_data["waiting_trim"] = False
            fmt = context.user_data.get("format", "video")
            if fmt == "gif":
                status_msg = await update.message.reply_text(
                    f"{t(context, 'downloading')}\n{make_progress_bar(0)}", reply_markup=cancel_keyboard(get_lang(context))
                )
                await _run_download(user, status_msg, context)
            elif fmt == "circle":
                speed = context.user_data.get("speed", "1.0")
                audio = context.user_data.get("audio", "normal")
                await update.message.reply_text(
                    t(context, "circle_menu"),
                    reply_markup=circle_menu_keyboard(speed, audio, get_lang(context))
                )
            else:
                subs_on = context.user_data.get("subtitles", False)
                speed = context.user_data.get("speed", "1.0")
                platform = context.user_data.get("platform", "Видео")
                ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
                _, al = AUDIO_OPTIONS.get(context.user_data.get("audio", "normal"), (1.0, ""))
                trim_s = context.user_data.get("trim_start", "")
                trim_e = context.user_data.get("trim_end", "")
                await update.message.reply_text(
                    f"🎬 {platform} • {ql} • {al}\n✂️ Обрезка: {trim_s} → {trim_e}\n\nВыбери ориентацию:",
                    reply_markup=orientation_keyboard(subs_on, speed, get_lang(context))
                )
        return

    # Ввод ID для блокировки/разблокировки
    if context.user_data.get("admin_action") and user.id == ADMIN_ID:
        action = context.user_data.pop("admin_action")
        try:
            uid = int(text.strip())
        except ValueError:
            await update.message.reply_text("❌ Неверный ID.")
            return
        data = get_data()
        blocked = data.setdefault("blocked", [])
        if action == "block":
            if uid not in blocked:
                blocked.append(uid)
                save_data(data)
                await update.message.reply_text(f"✅ {uid} заблокирован.")
            else:
                await update.message.reply_text("Уже заблокирован.")
        else:
            if uid in blocked:
                blocked.remove(uid)
                save_data(data)
                await update.message.reply_text(f"✅ {uid} разблокирован.")
            else:
                await update.message.reply_text("Не был заблокирован.")
        return

    # Обычный режим — ждём ссылку
    ACTIVE_USERS[user.id] = get_lang(context)
    context.user_data["_uid"] = str(user.id)

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
    context.user_data["trim_start"] = None
    context.user_data["trim_end"] = None
    context.user_data["subtitles"] = False
    context.user_data["waiting_trim"] = False
    context.user_data["speed"] = "1.0"

    # Загружаем историю
    db_history = load_history_from_db(user.id)
    session_history = context.user_data.get("history", [])
    seen, merged = set(), []
    for item in session_history + db_history:
        if item.get("url") not in seen:
            seen.add(item.get("url"))
            merged.append(item)
    context.user_data["history"] = merged[:HISTORY_SIZE]

    await update.message.reply_text(
        f"🎬 Видео с {platform}\n"
        f"{t(context, 'remaining', remaining=remaining)}\n\n"
        f"{t(context, 'step1')}",
        reply_markup=format_keyboard(get_lang(context))
    )

# ─── Callback: меню ───────────────────────────────────────────────────────────

async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    action = query.data.replace("menu_", "")
    user = query.from_user
    is_admin = user.id == ADMIN_ID
    lang = get_lang(context)

    if action == "back":
        try:
            await query.edit_message_caption(
                caption=t(context, "menu_title"),
                reply_markup=main_menu_keyboard(is_admin, lang)
            )
        except Exception:
            await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin, lang))

    elif action == "download":
        await safe_edit(query, t(context, "no_url"))

    elif action == "history":
        db_history = load_history_from_db(user.id)
        session_history = context.user_data.get("history", [])
        seen, merged = set(), []
        for item in session_history + db_history:
            if item.get("url") not in seen:
                seen.add(item.get("url"))
                merged.append(item)
        history = merged[:HISTORY_SIZE]
        context.user_data["history"] = history
        if not history:
            await safe_edit(query, t(context, "history_empty"), reply_markup=back_keyboard(get_lang(context)))
        else:
            kb = InlineKeyboardMarkup(
                history_keyboard(history).inline_keyboard +
                [[InlineKeyboardButton("◀️ Назад", callback_data="menu_back")]]
            )
            await safe_edit(query, t(context, "history_title"), reply_markup=kb)

    elif action == "me":
        data = get_data()
        uid = str(user.id)
        user_total = data.get("stats", {}).get("users", {}).get(uid, 0)
        if user_total == 0:
            text = t(context, "me_empty")
        else:
            user_platforms = data.get("user_platforms", {}).get(uid, {})
            fav = max(user_platforms.items(), key=lambda x: x[1])[0] if user_platforms else "—"
            today_count = data.get("downloads_today", {}).get(uid, 0)
            text = t(context, "me", total=user_total, fav=fav, today=today_count, limit=DAILY_LIMIT)
        await safe_edit(query, text, reply_markup=back_keyboard(get_lang(context)))

    elif action == "patchnote":
        version = BOT_VERSION
        notes = PATCH_NOTES.get(version)
        text = notes.get(lang, notes.get("ru", "")) if notes else f"📋 Версия {version} — нет патч-нотов."
        await safe_edit(query, text, reply_markup=patchnote_keyboard(version))

    elif action == "help":
        await safe_edit(query, t(context, "help"), reply_markup=back_keyboard(get_lang(context)))

    elif action == "lang":
        await safe_edit(query, "🌍 Выбери язык / Choose language:", reply_markup=lang_menu_keyboard())

    elif action == "stats" and is_admin:
        data = get_data()
        stats = data.get("stats", {})
        total = stats.get("total", 0)
        platforms = stats.get("platforms", {})
        top = sorted(platforms.items(), key=lambda x: x[1], reverse=True)
        top_str = "\n".join(f"  {p}: {c}" for p, c in top[:5]) or "  —"
        await safe_edit(
            query,
            f"📊 Статистика:\n\nВсего скачиваний: {total}\n"
            f"Пользователей: {len(stats.get('users', {}))}\n"
            f"Заблокировано: {len(data.get('blocked', []))}\n"
            f"Активных в сессии: {len(ACTIVE_USERS)}\n\n"
            f"Топ платформы:\n{top_str}",
            reply_markup=back_keyboard(get_lang(context))
        )

    elif action == "blocks" and is_admin:
        data = get_data()
        blocked = data.get("blocked", [])
        if not blocked:
            await safe_edit(query, "✅ Заблокированных нет.", reply_markup=back_keyboard(get_lang(context)))
        else:
            await safe_edit(query, f"🚫 Заблокировано: {len(blocked)}", reply_markup=admin_blocks_keyboard(blocked))

    elif action == "sendpatch" and is_admin:
        notes = PATCH_NOTES.get(BOT_VERSION)
        if not notes:
            await safe_edit(query, f"❌ Нет патч-нота для v{BOT_VERSION}.", reply_markup=back_keyboard(get_lang(context)))
            return
        if not ACTIVE_USERS:
            await safe_edit(query, "📭 Нет активных пользователей в этой сессии.", reply_markup=back_keyboard(get_lang(context)))
            return
        await safe_edit(query, f"📤 Рассылаю для {len(ACTIVE_USERS)} пользователей...")
        sent, failed = 0, 0
        for uid, ulang in ACTIVE_USERS.items():
            if uid == ADMIN_ID:
                continue
            try:
                text = notes.get(ulang, notes.get("ru", ""))
                await context.bot.send_message(chat_id=uid, text=text)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await safe_edit(
            query,
            f"✅ Патч-ноут v{BOT_VERSION} разослан!\n📨 Отправлено: {sent}\n❌ Не доставлено: {failed}",
            reply_markup=back_keyboard(get_lang(context))
        )

    elif action == "block_input" and is_admin:
        context.user_data["admin_action"] = "block"
        await safe_edit(query, "🚫 Введи ID пользователя для блокировки:")

    elif action == "unblock_input" and is_admin:
        context.user_data["admin_action"] = "unblock"
        await safe_edit(query, "✅ Введи ID пользователя для разблокировки:")

# ─── Callback: навигация патч-нотов ──────────────────────────────────────────

async def handle_patch_nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query.data == "patch_noop":
        await query.answer()
        return
    await query.answer()
    version = query.data.replace("patch_nav_", "")
    lang = get_lang(context)
    notes = PATCH_NOTES.get(version)
    text = notes.get(lang, notes.get("ru", "")) if notes else f"📋 Версия {version} — нет патч-нотов."
    await safe_edit(query, text, reply_markup=patchnote_keyboard(version))

# ─── Callback: блокировка ─────────────────────────────────────────────────────

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
    await safe_edit(query, f"✅ {uid} разблокирован.", reply_markup=back_keyboard(get_lang(context)))

# ─── Callback: язык ───────────────────────────────────────────────────────────

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
        await query.edit_message_caption(
            caption=t(context, "menu_title"),
            reply_markup=main_menu_keyboard(is_admin, lang)
        )
    except Exception:
        await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin, lang))

# ─── Callback: история ────────────────────────────────────────────────────────

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
    allowed, remaining = check_limit(query.from_user.id)
    if not allowed:
        await safe_edit(query, "⛔ Дневной лимит исчерпан.")
        return

    context.user_data["pending_url"] = item["url"]
    context.user_data["platform"] = item["platform"]
    context.user_data["cancel_flag"] = {"cancelled": False}
    context.user_data["trim_start"] = None
    context.user_data["trim_end"] = None
    context.user_data["subtitles"] = False
    context.user_data["waiting_trim"] = False
    context.user_data["speed"] = "1.0"
    await safe_edit(
        query,
        f"🎬 {item['platform']}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard(get_lang(context))
    )

# ─── Callback: формат ─────────────────────────────────────────────────────────

async def handle_format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    fmt = query.data.replace("fmt_", "")
    context.user_data["format"] = fmt
    platform = context.user_data.get("platform", "Видео")

    if fmt == "audio":
        context.user_data["quality"] = "best"
        await safe_edit(query, f"🎵 {platform} • MP3\n\n{t(context, 'step_audio')}", reply_markup=audio_keyboard(get_lang(context)))

    elif fmt == "gif":
        context.user_data["quality"] = "480"
        context.user_data["audio"] = "mute"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"🌀 {platform} • GIF\n\n{t(context, 'step_trim')}", reply_markup=trim_keyboard(get_lang(context)))

    elif fmt == "circle":
        context.user_data["quality"] = "480"
        context.user_data["audio"] = "normal"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"⭕ {platform} • {t(context, 'fmt_circle')}\n\n{t(context, 'step_trim_gif')}", reply_markup=trim_keyboard(get_lang(context)))

    elif fmt == "thumb":
        # Обложка — сразу скачиваем
        await safe_edit(query, f"⏳ Скачиваю обложку...", reply_markup=cancel_keyboard(get_lang(context)))
        await _run_download(query.from_user, query.message, context)

    elif fmt == "playlist":
        context.user_data["audio"] = "normal"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"📋 {platform} • {t(context, 'fmt_playlist')}\n\n{t(context, 'step_quality')}", reply_markup=quality_keyboard(get_lang(context)))

    else:  # video
        await safe_edit(query, f"🎬 {platform} • {t(context, 'fmt_video')}\n\n{t(context, 'step_quality')}", reply_markup=quality_keyboard(get_lang(context)))

# ─── Callback: качество ───────────────────────────────────────────────────────

async def handle_quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    quality = query.data.replace("quality_", "")
    context.user_data["quality"] = quality
    fmt = context.user_data.get("format", "video")
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(quality, quality)

    if fmt == "playlist":
        await safe_edit(query, f"⏳ Скачиваю плейлист...\nСкачано видео: 0", reply_markup=cancel_keyboard(get_lang(context)))
        await _run_download(query.from_user, query.message, context)
        return

    await safe_edit(query, f"🎬 {platform} • {ql}\n\n{t(context, 'step_audio')}", reply_markup=audio_keyboard(get_lang(context)))

# ─── Callback: аудио ──────────────────────────────────────────────────────────

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
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}", reply_markup=cancel_keyboard(get_lang(context)))
        await _run_download(query.from_user, query.message, context)
        return

    speed = context.user_data.get("speed", "1.0")
    subs_on = context.user_data.get("subtitles", False)

    # Если вернулись из меню кружочка — возвращаемся туда
    if context.user_data.pop("circle_audio_return", False):
        await safe_edit(query, t(context, "circle_menu"),
                        reply_markup=circle_menu_keyboard(speed, audio, get_lang(context)))
        return

    await safe_edit(
        query,
        f"🎬 {platform} • {ql} • {al}\n\n{t(context, 'step_orient')}",
        reply_markup=orientation_keyboard(subs_on, speed, get_lang(context))
    )

# ─── Callback: скорость ───────────────────────────────────────────────────────

async def handle_speed_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    speed = query.data.replace("speed_", "")
    context.user_data["speed"] = speed
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
    _, al = AUDIO_OPTIONS.get(context.user_data.get("audio", "normal"), (1.0, ""))
    subs_on = context.user_data.get("subtitles", False)
    speed_label = SPEED_OPTIONS.get(speed, speed)
    # Если вернулись из меню кружочка — возвращаемся туда
    if context.user_data.pop("circle_speed_return", False):
        audio = context.user_data.get("audio", "normal")
        await safe_edit(query, t(context, "circle_menu"),
                        reply_markup=circle_menu_keyboard(speed, audio, get_lang(context)))
        return

    await safe_edit(
        query,
        f"🎬 {platform} • {ql} • {al}\n⚡ Скорость: {speed_label}\n\nВыбери ориентацию или скачай:",
        reply_markup=orientation_keyboard(subs_on, speed, get_lang(context))
    )

# ─── Callback: ориентация ─────────────────────────────────────────────────────

async def handle_orientation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    platform = context.user_data.get("platform", "Видео")
    ql = QUALITY_LABELS.get(context.user_data.get("quality", "best"), "")
    _, al = AUDIO_OPTIONS.get(context.user_data.get("audio", "normal"), (1.0, ""))
    subs_on = context.user_data.get("subtitles", False)
    speed = context.user_data.get("speed", "1.0")

    def trim_info():
        ts = context.user_data.get("trim_start")
        te = context.user_data.get("trim_end")
        return f"\n✂️ {ts} → {te}" if ts and te else ""

    if data == "orient_toggle_subs":
        context.user_data["subtitles"] = not subs_on
        subs_on = context.user_data["subtitles"]
        await safe_edit(
            query,
            f"🎬 {platform} • {ql} • {al}{trim_info()}\n\nВыбери ориентацию или скачай:",
            reply_markup=orientation_keyboard(subs_on, speed, get_lang(context))
        )

    elif data == "orient_speed":
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(get_lang(context)))

    elif data == "orient_trim":
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, t(context, "trim_enter_start"))

    elif data == "orient_download":
        if "orientation" not in context.user_data:
            context.user_data["orientation"] = "original"
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}", reply_markup=cancel_keyboard(get_lang(context)))
        await _run_download(query.from_user, query.message, context)

    else:
        orient = data.replace("orient_", "")
        context.user_data["orientation"] = orient
        orient_labels = {"original": t(context,"orient_original"), "square": t(context,"orient_square"), "landscape": t(context,"orient_landscape")}
        await safe_edit(
            query,
            f"🎬 {platform} • {ql} • {al}\n"
            f"📐 {orient_labels.get(orient, orient)}{trim_info()}\n\n"
            f"Нажми «Скачать» или измени опции:",
            reply_markup=orientation_keyboard(subs_on, speed, get_lang(context))
        )

# ─── Callback: обрезка ────────────────────────────────────────────────────────

async def handle_trim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    fmt = context.user_data.get("format", "video")
    if query.data == "trim_no":
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        context.user_data["subtitles"] = False
        if fmt == "circle":
            speed = context.user_data.get("speed", "1.0")
            audio = context.user_data.get("audio", "normal")
            await safe_edit(query, t(context, "circle_menu"),
                            reply_markup=circle_menu_keyboard(speed, audio, get_lang(context)))
        else:
            await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}", reply_markup=cancel_keyboard(get_lang(context)))
            await _run_download(query.from_user, query.message, context)
    else:
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, t(context, "trim_enter_start"))

# ─── Callback: меню кружочка ──────────────────────────────────────────────────

async def handle_circle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    lang = get_lang(context)

    if data == "circle_speed":
        # Открываем выбор скорости, потом вернёмся в circle_menu через speed callback
        context.user_data["circle_speed_return"] = True
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(lang))
        return

    if data == "circle_audio":
        # Открываем выбор громкости
        context.user_data["circle_audio_return"] = True
        await safe_edit(query, t(context, "step_audio"), reply_markup=audio_keyboard(lang))
        return

    if data == "circle_download":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)

# ─── Callback: отмена ─────────────────────────────────────────────────────────

async def handle_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Отменяю...")
    flag = context.user_data.get("cancel_flag", {})
    flag["cancelled"] = True
    await safe_edit(query, "❌ Загрузка отменена.")

# ─── Финальное скачивание ─────────────────────────────────────────────────────

async def _run_download(user, status_msg, context: ContextTypes.DEFAULT_TYPE):
    user_id = user.id

    # ── Очередь: один активный Download на пользователя ──
    if user_id not in DOWNLOAD_LOCKS:
        DOWNLOAD_LOCKS[user_id] = asyncio.Lock()

    lock = DOWNLOAD_LOCKS[user_id]
    if lock.locked():
        # Уже идёт скачивание — ставим в очередь
        pos = DOWNLOAD_QUEUE.qsize() + 1
        # Сохраняем задачу в Redis для персистентности
        if Storage._redis:
            try:
                task_data = json.dumps({
                    "user_id": user_id,
                    "chat_id": status_msg.chat_id,
                    "user_data": {
                        k: v for k, v in context.user_data.items()
                        if isinstance(v, (str, int, float, bool, list, dict, type(None)))
                    }
                })
                Storage._redis.rpush("bot:queue:pending", task_data)
            except Exception:
                pass
        try:
            await status_msg.edit_text(t(context, "queued", pos=pos))
        except Exception:
            pass
        await DOWNLOAD_QUEUE.put((user, status_msg, dict(context.user_data)))
        return

    async with lock:
        await _do_download(user, status_msg, context)
        # Убираем из Redis-очереди если есть
        if Storage._redis:
            try:
                Storage._redis.lrem("bot:queue:pending", 1, str(user_id))
            except Exception:
                pass

    # Обрабатываем следующего в очереди
    if not DOWNLOAD_QUEUE.empty():
        try:
            queued_user, queued_msg, queued_data = await asyncio.wait_for(
                DOWNLOAD_QUEUE.get(), timeout=1
            )
            context.user_data.update(queued_data)
            await _do_download(queued_user, queued_msg, context)
        except asyncio.TimeoutError:
            pass

async def _do_download(user, status_msg, context: ContextTypes.DEFAULT_TYPE):
    url         = context.user_data.get("pending_url")
    quality     = context.user_data.get("quality", "best")
    fmt         = context.user_data.get("format", "video")
    audio       = context.user_data.get("audio", "normal")
    orient      = context.user_data.get("orientation", "original")
    trim_s      = context.user_data.get("trim_start")
    trim_e      = context.user_data.get("trim_end")
    subtitles   = context.user_data.get("subtitles", False)
    platform    = context.user_data.get("platform", "Видео")
    speed_str   = context.user_data.get("speed", "1.0")
    cancel_flag = context.user_data.get("cancel_flag", {"cancelled": False})
    volume, audio_label = AUDIO_OPTIONS.get(audio, (1.0, "🔊 Обычный"))
    speed = float(speed_str)
    ql = QUALITY_LABELS.get(quality, quality)
    files_to_clean = []

    if not url:
        await status_msg.edit_text("❌ Ссылка устарела. Отправь заново.")
        return

    try:
        # ── Обложка ──
        if fmt == "thumb":
            await status_msg.edit_text("🖼 Скачиваю обложку...")
            thumb_path = await download_thumbnail(url, DOWNLOAD_DIR)
            if not thumb_path or not thumb_path.exists():
                await status_msg.edit_text("❌ Не удалось получить обложку видео.")
                return
            files_to_clean.append(thumb_path)
            with open(thumb_path, "rb") as f:
                await status_msg.reply_photo(photo=f, caption=f"🖼 {platform} • Обложка")
            await status_msg.delete()
            update_stats(user.id, platform)
            increment_limit(user.id)
            add_to_history(context, url, platform)
            return

        # ── Плейлист ──
        if fmt == "playlist":
            zip_path = await download_playlist(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag, lang=context.user_data.get("lang", "ru"))
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

        # ── Видео/GIF/Кружочек/MP3 ──
        file_path = await download_video(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag, fmt, lang=context.user_data.get("lang", "ru"))

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

        # Обрезка
        if trim_s and trim_e and fmt != "audio":
            await status_msg.edit_text("✂️ Обрезаю видео...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_trim, current, trim_s, trim_e)
            if current != file_path:
                files_to_clean.append(current)

        # GIF
        if fmt == "gif":
            await status_msg.edit_text("🌀 Конвертирую в GIF...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, convert_to_gif, current)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Кружочек
        elif fmt == "circle":
            await status_msg.edit_text("⭕ Конвертирую в кружочек...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, convert_to_circle, current)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Ориентация
        if fmt == "video" and orient != "original":
            await status_msg.edit_text("📐 Применяю ориентацию...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_orientation, current, orient)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Субтитры
        subs_warning = None
        if subtitles and fmt == "video":
            await status_msg.edit_text("📝 Добавляю субтитры...")
            await asyncio.sleep(3)
            new, subs_warning = await _add_subtitles(url, current, platform)
            if new != current and new not in files_to_clean:
                files_to_clean.append(new)
            current = new

        # Скорость
        if speed != 1.0 and fmt in ("video", "circle"):
            await status_msg.edit_text(f"⚡ Применяю скорость {speed}x...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_speed, current, speed)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Громкость
        if fmt == "video" and volume != 1.0:
            await status_msg.edit_text("🎚️ Обрабатываю звук...")
            loop = asyncio.get_event_loop()
            current = await loop.run_in_executor(None, apply_audio, current, volume)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Проверка размера — сжимаем вместо отказа
        if current.stat().st_size > MAX_FILE_MB * 1024 * 1024:
            size_mb = current.stat().st_size / 1024 / 1024
            if fmt in ("video", "circle") and ffmpeg_available():
                await status_msg.edit_text(
                    f"📦 Файл {size_mb:.1f} МБ — сжимаю до {MAX_FILE_MB} МБ..."
                )
                loop = asyncio.get_event_loop()
                compressed = await loop.run_in_executor(
                    None, compress_video, current, float(MAX_FILE_MB - 3)
                )
                if compressed != current:
                    files_to_clean.append(compressed)
                    current = compressed
                # Если всё равно большой — отказываем
                if current.stat().st_size > MAX_FILE_MB * 1024 * 1024:
                    await status_msg.edit_text(
                        f"❌ Не удалось сжать до {MAX_FILE_MB} МБ. Попробуй качество пониже."
                    )
                    return
            else:
                await status_msg.edit_text(
                    f"❌ Файл {size_mb:.1f} МБ — больше {MAX_FILE_MB} МБ. Попробуй качество пониже."
                )
                return

        await status_msg.edit_text("📤 Отправляю...")

        # Caption
        if fmt == "audio":
            caption = f"🎵 {platform} • MP3"
        elif fmt == "gif":
            caption = f"🌀 {platform} • GIF"
        elif fmt == "circle":
            caption = f"⭕ {platform} • Кружочек"
        else:
            caption = f"✅ {platform} • {ql}"
            if audio_label != "🔊 Обычный":
                caption += f" • {audio_label}"
        if speed != 1.0:
            caption += f" • {speed}x"
        if subs_warning:
            caption += f"\n{subs_warning}"

        # Отправка
        with open(current, "rb") as f:
            if fmt == "audio":
                await status_msg.reply_audio(audio=f, caption=caption)
            elif fmt == "gif":
                await status_msg.reply_animation(animation=f, caption=caption)
            elif fmt == "circle":
                await status_msg.reply_video_note(video_note=f)
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

# ─── Фоновые задачи ──────────────────────────────────────────────────────────

async def task_ytdlp_update_once(context=None):
    """Обновляет yt-dlp один раз. Вызывается из PTB job_queue."""
    try:
        logger.info("🔄 Обновляю yt-dlp...")
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp",
                 "--break-system-packages"],
                capture_output=True, text=True, timeout=120
            )
        )
        if result.returncode == 0:
            ver_result = subprocess.run(
                [sys.executable, "-m", "yt_dlp", "--version"],
                capture_output=True, text=True
            )
            ver = ver_result.stdout.strip()
            logger.info(f"✅ yt-dlp обновлён до {ver}")
            try:
                await app_ref.bot.send_message(
                    ADMIN_ID,
                    f"✅ yt-dlp автоматически обновлён до версии {ver}"
                )
            except Exception:
                pass
        else:
            logger.warning(f"yt-dlp update failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Ошибка обновления yt-dlp: {e}")


async def task_redis_queue():
    """Обрабатывает персистентную очередь из Redis при старте."""
    if not Storage._redis:
        return
    try:
        # При рестарте достаём незавершённые задачи из Redis-очереди
        pending = Storage._redis.lrange("bot:queue:pending", 0, -1)
        if pending:
            logger.info(f"Найдено {len(pending)} незавершённых задач в Redis-очереди")
            Storage._redis.delete("bot:queue:pending")
    except Exception as e:
        logger.warning(f"Redis queue init error: {e}")


# Глобальная ссылка на app для использования в фоновых задачах
app_ref = None


def main() -> None:
    global app_ref
    asyncio.set_event_loop(asyncio.new_event_loop())

    # Инициализируем Redis
    Storage.init()

    app = Application.builder().token(BOT_TOKEN).build()
    app_ref = app

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
    app.add_handler(CallbackQueryHandler(handle_speed_callback,       pattern="^speed_"))
    app.add_handler(CallbackQueryHandler(handle_orientation_callback, pattern="^orient_"))
    app.add_handler(CallbackQueryHandler(handle_circle_callback,      pattern="^circle_"))
    app.add_handler(CallbackQueryHandler(handle_trim_callback,        pattern="^trim_"))
    app.add_handler(CallbackQueryHandler(handle_patch_nav_callback,   pattern="^patch_"))
    app.add_handler(CallbackQueryHandler(handle_cancel_callback,      pattern="^cancel_download"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Регистрируем фоновые задачи через job_queue PTB (корректно завершаются при стопе)
    async def _start_background(app):
        # yt-dlp автообновление раз в неделю через PTB job_queue
        app.job_queue.run_repeating(
            callback=lambda ctx: asyncio.ensure_future(task_ytdlp_update_once(ctx)),
            interval=7 * 24 * 3600,
            first=7 * 24 * 3600,  # первый запуск через неделю
            name="ytdlp_update"
        )
        await task_redis_queue()

    app.post_init = _start_background

    logger.info(f"Бот v{BOT_VERSION} запущен...")

    if WEBHOOK_URL:
        # ── Webhook режим (продакшн Railway) ──────────────────────────────────
        # Railway принимает трафик на 443 снаружи и пробрасывает на PORT внутри
        # WEBHOOK_URL должен быть без порта: https://xxx.railway.app
        webhook_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        logger.info(f"Запуск в Webhook режиме: {webhook_url} (внутренний порт={WEBHOOK_PORT})")
        try:
            app.run_webhook(
                listen="0.0.0.0",
                port=WEBHOOK_PORT,
                url_path=WEBHOOK_PATH,
                webhook_url=webhook_url,
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                secret_token=BOT_TOKEN[:20],  # защита от левых запросов
            )
        except Exception as e:
            logger.error(f"Webhook не запустился: {e} — падаю в Polling")
            app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    else:
        # ── Polling режим (локальная разработка) ──────────────────────────────
        logger.info("Запуск в Polling режиме (нет WEBHOOK_URL)")
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
