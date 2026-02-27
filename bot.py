"""
Telegram Video Downloader Bot v1.7
Refactored: все баги исправлены, код реструктурирован.
"""

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
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, date, time as dtime

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes,
)
import yt_dlp

# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "TOKEN_HERE")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "balerndownloadsbot")
BOT_VERSION  = os.environ.get("BOT_VERSION", "1.7")
ADMIN_ID     = int(os.environ.get("ADMIN_ID", "123456789"))
DAILY_LIMIT  = 20
HISTORY_SIZE = 10
MAX_FILE_MB  = 50

DATA_DIR     = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR = DATA_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
DATA_FILE    = DATA_DIR / "data.json"

REDIS_URL    = os.environ.get("REDIS_URL")

# Фото меню
_ASSETS_DIR = Path(__file__).parent
MENU_GIF_FILE = _ASSETS_DIR / "welcome.gif"


def _menu_photo_path(theme: str, lang: str) -> Path:
    """Возвращает путь к картинке меню по теме и языку."""
    return _ASSETS_DIR / f"menu_{theme}_{lang}.png"

# ═══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# FFMPEG
# ═══════════════════════════════════════════════════════════════════════════════

FFMPEG_LOCATION: str = ""


def _setup_ffmpeg():
    """Находит ffmpeg — без тяжёлых загрузок."""
    global FFMPEG_LOCATION

    def _set(directory: str):
        global FFMPEG_LOCATION
        os.environ["PATH"] = directory + os.pathsep + os.environ.get("PATH", "")
        FFMPEG_LOCATION = directory

    # 1. Системный PATH (Docker apt-get, nixpacks)
    ff = shutil.which("ffmpeg")
    if ff:
        loc = str(Path(ff).parent)
        _set(loc)
        logger.info("✅ ffmpeg: %s", loc)
        return

    # 2. Стандартные пути (если PATH неполный)
    for d in ["/usr/bin", "/usr/local/bin", "/opt/bin"]:
        if (Path(d) / "ffmpeg").exists():
            _set(d)
            logger.info("✅ ffmpeg: %s", d)
            return

    # 3. Nix store (Railway, Render)
    results = glob_module.glob("/nix/store/*/bin/ffmpeg")
    if results:
        loc = str(Path(results[0]).parent)
        _set(loc)
        logger.info("✅ ffmpeg nix: %s", loc)
        return

    # 4. imageio-ffmpeg (pip)
    try:
        import imageio_ffmpeg
        ff_exe = imageio_ffmpeg.get_ffmpeg_exe()
        if ff_exe and Path(ff_exe).exists():
            # Бинарник может называться ffmpeg-linux64-v4.2.2, а не "ffmpeg"
            # Создаём симлинки в /tmp/ffmpeg_bin/
            link_dir = Path("/tmp/ffmpeg_bin")
            link_dir.mkdir(exist_ok=True)
            ffmpeg_link = link_dir / "ffmpeg"
            ffprobe_link = link_dir / "ffprobe"
            # ffmpeg симлинк/копия
            if not ffmpeg_link.exists():
                try:
                    ffmpeg_link.symlink_to(ff_exe)
                except Exception:
                    try:
                        import shutil as sh
                        sh.copy2(ff_exe, str(ffmpeg_link))
                        os.chmod(str(ffmpeg_link), 0o755)
                    except Exception as e2:
                        logger.warning("imageio symlink/copy failed: %s", e2)
            # ffprobe
            if not ffprobe_link.exists():
                ff_dir = Path(ff_exe).parent
                probe_candidates = list(ff_dir.glob("ffprobe*"))
                if probe_candidates:
                    try:
                        ffprobe_link.symlink_to(probe_candidates[0])
                    except Exception:
                        pass
            _set(str(link_dir))
            logger.info("✅ ffmpeg imageio: %s → %s", ff_exe, link_dir)
            return
    except ImportError:
        pass
    except Exception as e:
        logger.warning("imageio-ffmpeg ошибка: %s", e)

    # 5. static-ffmpeg (pip)
    try:
        import static_ffmpeg
        static_ffmpeg.add_paths()
        ff = shutil.which("ffmpeg")
        if ff:
            _set(str(Path(ff).parent))
            logger.info("✅ ffmpeg static: %s", ff)
            return
    except ImportError:
        pass
    except Exception as e:
        logger.warning("static-ffmpeg ошибка: %s", e)

    logger.warning("⚠️ ffmpeg НЕ НАЙДЕН — обработка видео ограничена")


_setup_ffmpeg()

# Диагностика ffmpeg при запуске
def _test_ffmpeg():
    """Проверяет что ffmpeg реально работает."""
    logger.warning("🔍 FFMPEG диагностика:")
    logger.warning("  FFMPEG_LOCATION = %s", FFMPEG_LOCATION)
    logger.warning("  shutil.which('ffmpeg') = %s", shutil.which("ffmpeg"))

    # Проверяем симлинк
    if FFMPEG_LOCATION:
        link = Path(FFMPEG_LOCATION) / "ffmpeg"
        logger.warning("  symlink %s exists=%s", link, link.exists())
        if link.exists():
            try:
                logger.warning("  symlink target = %s", link.resolve())
            except Exception:
                pass

    # Пробуем запустить
    ff = shutil.which("ffmpeg")
    if not ff and FFMPEG_LOCATION:
        ff = str(Path(FFMPEG_LOCATION) / "ffmpeg")
    if ff:
        try:
            r = subprocess.run([ff, "-version"], capture_output=True, text=True, timeout=5)
            first_line = r.stdout.split("\n")[0] if r.stdout else "no output"
            logger.warning("  ✅ ffmpeg работает: %s", first_line)
        except Exception as e:
            logger.error("  ❌ ffmpeg НЕ запускается: %s", e)
    else:
        logger.error("  ❌ ffmpeg бинарник не найден")

_test_ffmpeg()

# ═══════════════════════════════════════════════════════════════════════════════
# ХРАНИЛИЩЕ (Redis + JSON fallback)
# ═══════════════════════════════════════════════════════════════════════════════

class Storage:
    _redis = None

    @classmethod
    def init(cls):
        if not REDIS_URL:
            logger.info("REDIS_URL не задан — используем data.json")
            return
        try:
            import redis as redis_lib
            cls._redis = redis_lib.from_url(
                REDIS_URL, decode_responses=True,
                socket_timeout=10, socket_connect_timeout=10,
            )
            cls._redis.ping()
            logger.info("✅ Redis подключён")
            # Миграция data.json → Redis
            if DATA_FILE.exists() and not cls._redis.exists("bot:data"):
                try:
                    cls._redis.set("bot:data", DATA_FILE.read_text("utf-8"))
                    logger.info("data.json мигрирован в Redis")
                except Exception as e:
                    logger.warning("Миграция data.json → Redis: %s", e)
        except Exception as e:
            logger.warning("Redis недоступен: %s", e)
            cls._redis = None

    @classmethod
    def load(cls) -> dict:
        default = {
            "stats": {}, "blocked": [],
            "downloads_today": {}, "last_reset": str(date.today()),
        }
        if cls._redis:
            try:
                raw = cls._redis.get("bot:data")
                if raw:
                    return json.loads(raw)
            except Exception as e:
                logger.warning("Redis load: %s", e)
        if DATA_FILE.exists():
            try:
                return json.loads(DATA_FILE.read_text("utf-8"))
            except Exception:
                pass
        return default

    @classmethod
    def save(cls, data: dict):
        s = json.dumps(data, ensure_ascii=False, indent=2)
        if cls._redis:
            try:
                cls._redis.set("bot:data", s)
                return
            except Exception as e:
                logger.warning("Redis save: %s", e)
        try:
            DATA_FILE.write_text(s, encoding="utf-8")
        except Exception as e:
            logger.error("Не удалось сохранить data.json: %s", e)


def get_data() -> dict:
    data = Storage.load()
    if data.get("last_reset") != str(date.today()):
        data["downloads_today"] = {}
        data["last_reset"] = str(date.today())
        Storage.save(data)
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

# user_id → lang — ограничиваем до 10 000 записей
ACTIVE_USERS: dict[int, str] = {}
MAX_ACTIVE_USERS = 10_000

# Очередь загрузок
DOWNLOAD_LOCKS: dict[int, asyncio.Lock] = {}
DOWNLOAD_QUEUE: asyncio.Queue = asyncio.Queue()

# Кэш file_id фото меню
_PHOTO_CACHE: dict[str, str] = {}
_GIF_FILE_ID: str | None = None

# Глобальная ссылка на Application
app_ref: Application | None = None

# Счётчик ошибок
_error_state = {"count": 0, "last_alert": 0.0}


def track_user(user_id: int, lang: str):
    """Добавляет пользователя в ACTIVE_USERS с лимитом."""
    if len(ACTIVE_USERS) >= MAX_ACTIVE_USERS:
        # Удаляем самых старых
        to_remove = list(ACTIVE_USERS.keys())[:1000]
        for uid in to_remove:
            ACTIVE_USERS.pop(uid, None)
    ACTIVE_USERS[user_id] = lang


# ═══════════════════════════════════════════════════════════════════════════════
# ПЛАТФОРМЫ И КОНСТАНТЫ
# ═══════════════════════════════════════════════════════════════════════════════

SUPPORTED_PATTERNS = [
    r"tiktok\.com", r"vm\.tiktok\.com",
    r"instagram\.com", r"instagr\.am",
    r"youtube\.com/shorts", r"youtube\.com/watch", r"youtu\.be",
    r"twitter\.com", r"x\.com",
    r"vk\.com", r"clips\.twitch\.tv",
    r"reddit\.com",
    r"pinterest\.com", r"pin\.it",
    r"twitch\.tv/videos", r"twitch\.tv/.+/clip",
    r"soundcloud\.com",
    r"vimeo\.com",
    r"dailymotion\.com", r"dai\.ly",
    r"music\.yandex\.(ru|com)", r"music\.yandex\.kz",
    r"open\.spotify\.com",
    r"t\.me/addstickers/",
    r"youtube\.com/channel", r"youtube\.com/@", r"youtube\.com/c/",
    r"youtube\.com/playlist",
]

STICKER_PATTERN    = r"t\.me/addstickers/"
SPOTIFY_PATTERN    = r"open\.spotify\.com"
YANDEX_PATTERN     = r"music\.yandex"

QUALITY_OPTIONS = {
    "360":  "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360]",
    "480":  "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480]",
    "720":  "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]",
    "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]",
    "best": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
}
QUALITY_LABELS = {
    "360": "360p", "480": "480p", "720": "720p HD",
    "1080": "1080p FHD", "best": "Макс.",
}

AUDIO_OPTIONS = {
    "mute":   (0.0, "🔇"),
    "quiet":  (0.4, "🔉"),
    "normal": (1.0, "🔊"),
    "loud":   (2.0, "📢"),
}

SPEED_OPTIONS = {
    "0.5": "🐢 0.5x", "0.75": "🐌 0.75x", "1.0": "▶️ 1x",
    "1.5": "🐇 1.5x", "2.0": "⚡ 2x",
}

FUNNY_MESSAGES = [
    "🐱 Ищем котиков...", "🕵️ Скачиваю у секретных агентов...",
    "🍕 Заказываем пиццу серверу...", "🐢 Черепаха несёт данные...",
    "☕ Завариваем кофе серверу...", "🌍 Объезжаем интернет...",
    "👽 Связываемся с инопланетянами...", "🎲 Бросаем кубик...",
    "🐿️ Белка грызёт оптоволокно...", "💾 Перематываем кассету...",
    "🧙 Читаем заклинания...", "🏃 Курьер бежит с флешкой...",
    "🐧 Пингвины толкают сервер...", "📡 Ловим сигнал...",
    "⚙️ Крутим шестерёнки...",
]

RETRY_DELAYS = [5, 15]


# ═══════════════════════════════════════════════════════════════════════════════
# ПЕРЕВОДЫ (ИСПРАВЛЕНЫ — без дубликатов ключей)
# ═══════════════════════════════════════════════════════════════════════════════

TEXTS = {
    "ru": {
        "start_caption": (
            "👋 Привет! Добро пожаловать!\n\n"
            "🎬 Я скачиваю видео и музыку из:\n"
            "TikTok • YouTube • Instagram • Twitter\n"
            "VK • Reddit • Pinterest • Twitch\n"
            "Vimeo • SoundCloud • Dailymotion\n\n"
            "💡 Просто кинь ссылку — выбери формат,\n"
            "качество и получи файл прямо в Telegram!\n\n"
            "🎵 Форматы: MP4, MP3, WAV, FLAC, GIF, кружочек\n"
            "✂️ Обрезка, скорость, субтитры и многое другое\n\n"
            "Поехали! 🚀"
        ),
        "help": (
            "📌 Как пользоваться\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ Отправь ссылку на видео\n"
            "2️⃣ Выбери формат: видео / MP3 / WAV / FLAC / GIF / кружочек / 🗿 шакал\n"
            "3️⃣ Выбери качество и уровень звука\n"
            "4️⃣ Ориентация, субтитры, скорость, басс-буст — или «Скачать»\n\n"
            "🌀 GIF — поддержка скорости!\n"
            "🔊 Басс-буст — мощный бас на видео\n"
            "🗿 Шакал — видео в убитом качестве (мем)\n\n"
            "🎭 Стикерпаки: отправь ссылку t.me/addstickers/...\n"
            "🔍 Поиск YouTube: кнопка в меню\n"
            "🔗 Объединить видео: кнопка в меню\n\n"
            "⚠️ Лимит: 50 МБ и 20 скачиваний в день\n\n"
            "📍 /menu — меню\n"
            "📍 /history — история\n"
            "📍 /me — статистика"
        ),
        "history_empty": "📭 История пуста.",
        "history_title": "🕘 Последние скачивания:",
        "blocked": "🚫 Ты заблокирован.",
        "limit": "⛔ Лимит ({limit} скачиваний).\nВозвращайся завтра!",
        "no_url": "🔗 Пришли мне ссылку на видео.",
        "unsupported": (
            "❌ Платформа не поддерживается.\n"
            "Поддерживаются: TikTok, YouTube, Instagram, Twitter, VK, "
            "Reddit, Pinterest, Vimeo, Dailymotion, Twitch, SoundCloud"
        ),
        "step1": "📦 Шаг 1 — выбери формат:",
        "remaining": "Осталось сегодня: {remaining}",
        "fmt_video": "🎬 Видео (MP4)",
        "fmt_audio": "🎵 MP3",
        "fmt_wav": "🎵 WAV",
        "fmt_flac": "🎵 FLAC",
        "fmt_sticker": "🎭 Стикерпак (ZIP)",
        "fmt_gif": "🌀 GIF",
        "fmt_circle": "⭕ Кружочек",
        "fmt_thumb": "🖼 Обложка",
        "fmt_playlist": "📋 Плейлист (ZIP)",
        "fmt_shakal": "🗿 Шакал",
        "audio_fmt_title": "🎵 Выбери формат аудио:",
        "sticker_downloading": "🎭 Скачиваю стикерпак...",
        "sticker_done": "🎭 Стикерпак готов! {n} стикеров",
        "sticker_not_found": "❌ Стикерпак не найден.",
        "search_enter": "🔍 Введи запрос для поиска по YouTube:",
        "searching": "🔍 Ищу...",
        "search_no_results": "❌ Ничего не найдено.",
        "search_results": "🔍 Результаты поиска:",
        "spotify_not_supported": "⚠️ Spotify не поддерживается — используй YouTube Music.",
        "pinterest_hint": "📌 Pinterest: только видео-пины.",
        "yandex_geo_error": "❌ Яндекс Музыка недоступна (геоблок 451).",
        "sticker_enter": "🎭 Отправь ссылку: t.me/addstickers/ИмяПака",
        "step_quality": "📐 Шаг 2 — качество:",
        "step_audio": "🔊 Шаг 3 — уровень звука:",
        "step_orient": "📐 Шаг 4 — ориентация, субтитры, скорость:",
        "step_speed": "⚡ Выбери скорость:",
        "step_trim": "✂️ Хочешь обрезать?",
        "step_trim_gif": "✂️ Обрезать? (макс. 60 сек)",
        "circle_menu": "⭕ Кружочек — настрой или скачай:",
        "circle_speed": "⚡ {speed}x",
        "circle_audio": "🔊 {label}",
        "circle_download": "⬇️ Скачать",
        "gif_menu": "🌀 GIF — настрой или скачай:",
        "gif_speed": "⚡ {speed}x",
        "gif_download": "⬇️ Скачать",
        "bass_on": "🔊 Басс-буст ✅",
        "bass_off": "🔊 Басс-буст ❌",
        "shakal_menu": "🗿 Шакал — настрой или скачай:",
        "shakal_speed": "⚡ {speed}x",
        "shakal_bass_on": "🔊 Басс ✅",
        "shakal_bass_off": "🔊 Басс ❌",
        "shakal_download": "⬇️ Шакалить",
        "trim_yes": "✂️ Обрезать",
        "trim_no": "⏭ Без обрезки",
        "orient_original": "📱 Оригинал",
        "orient_square": "⬛ 1:1",
        "orient_landscape": "🖼 16:9",
        "subs_on": "📝 Субтитры ✅",
        "subs_off": "📝 Субтитры ❌",
        "speed_btn": "⚡ {speed}x",
        "trim_btn": "✂️ Обрезать",
        "download_btn": "⬇️ Скачать",
        "audio_mute": "🔇 Без звука",
        "audio_quiet": "🔉 Тише",
        "audio_normal": "🔊 Обычный",
        "audio_loud": "📢 Громче",
        "q360": "360p", "q480": "480p", "q720": "720p HD",
        "q1080": "1080p FHD", "qbest": "🏆 Макс.",
        "speed_half": "🐢 0.5x", "speed_075": "🐌 0.75x", "speed_1": "▶️ 1x",
        "speed_15": "🐇 1.5x", "speed_2": "⚡ 2x",
        "cancel_btn": "❌ Отмена",
        "back_btn": "◀️ Назад",
        "downloading": "⏳ Скачиваю...",
        "trim_enter_start": "✂️ Введи начало (М:СС):\nНапример: 0:15 или 1:30",
        "trim_enter_end": "Теперь введи конец:",
        "trim_invalid": "❌ Неверный формат. Пример: 0:15",
        "queued": "⏳ Очередь ({pos}). Подожди...",
        "preview_loading": "🔍 Получаю информацию...",
        "preview_download": "⬇️ Скачать",
        "preview_cancelled": "❌ Отменено.",
        "merge_start": (
            "🎬 Объединение видео\n\n"
            "Отправляй видео как файл (📎 → Файл), по одному.\n"
            "Когда все — нажми «Объединить»."
        ),
        "merge_btn": "🔗 Объединить",
        "merge_cancel_btn": "❌ Отмена",
        "merge_received": "✅ Видео {n} получено.",
        "merge_processing": "🔗 Объединяю...",
        "merge_done": "✅ Видео объединены!",
        "merge_need_two": "❌ Нужно минимум 2 видео.",
        "limit_reset": "🔄 Лимит сброшен! Доступно {limit} скачиваний.",
        "me": (
            "👤 Твоя статистика\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "📥 Всего скачано: {total}\n"
            "❤️ Любимая платформа: {fav}\n"
            "📅 Сегодня: {today} из {limit}\n\n"
            "Продолжай! 🔥"
        ),
        "me_empty": "📭 Ты ещё ничего не скачивал!",
        "menu_title": "🎛 Главное меню\n\nВыбери действие 👇",
        "settings_title": "⚙️ Настройки",
        "theme_toggle": "🎨 Тема: {theme}",
        "theme_light": "☀️ Светлая",
        "theme_dark": "🌙 Тёмная",
        "theme_changed": "🎨 Тема: {theme}",
        "default_fmt": "📦 Формат: {fmt}",
        "default_fmt_title": "📦 Выбери формат:",
        "default_quality": "📐 Качество: {q}",
        "pref_saved": "✅ Сохранено!",
        "settings_info": (
            "⚙️ Настройки\n"
            "━━━━━━━━━━━━━━━\n\n"
            "🎨 Тема: {theme}\n"
            "📦 Формат: {fmt}\n"
            "📐 Качество: {quality}\n\n"
            "Нажми кнопку чтобы изменить 👇"
        ),
    },
    "en": {
        "start_caption": (
            "👋 Hey! Welcome!\n\n"
            "🎬 I download videos and music from:\n"
            "TikTok • YouTube • Instagram • Twitter\n"
            "VK • Reddit • Pinterest • Twitch\n"
            "Vimeo • SoundCloud • Dailymotion\n\n"
            "💡 Just send a link — pick format,\n"
            "quality and get the file right in Telegram!\n\n"
            "🎵 Formats: MP4, MP3, WAV, FLAC, GIF, circle\n"
            "✂️ Trim, speed, subtitles and more\n\n"
            "Let's go! 🚀"
        ),
        "help": (
            "📌 How to use\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ Send a video link\n"
            "2️⃣ Choose format: video / MP3 / WAV / FLAC / GIF / circle / 🗿 deep-fried\n"
            "3️⃣ Choose quality and audio level\n"
            "4️⃣ Orientation, subs, speed, bass boost — or Download\n\n"
            "🌀 GIF — speed control!\n"
            "🔊 Bass boost — heavy bass on videos\n"
            "🗿 Deep-fried — max compression meme quality\n\n"
            "🎭 Stickerpacks: send t.me/addstickers/... link\n"
            "🔍 YouTube search: button in menu\n"
            "🔗 Merge videos: button in menu\n\n"
            "⚠️ Limit: 50 MB and 20 downloads per day\n\n"
            "📍 /menu — menu\n"
            "📍 /history — history\n"
            "📍 /me — stats"
        ),
        "history_empty": "📭 History is empty.",
        "history_title": "🕘 Recent downloads:",
        "blocked": "🚫 You are blocked.",
        "limit": "⛔ Daily limit ({limit} downloads).\nCome back tomorrow!",
        "no_url": "🔗 Send me a video link.",
        "unsupported": (
            "❌ Platform not supported.\n"
            "Supported: TikTok, YouTube, Instagram, Twitter, VK, "
            "Reddit, Pinterest, Vimeo, Dailymotion, Twitch, SoundCloud"
        ),
        "step1": "📦 Step 1 — choose format:",
        "remaining": "Left today: {remaining}",
        "fmt_video": "🎬 Video (MP4)",
        "fmt_audio": "🎵 MP3",
        "fmt_wav": "🎵 WAV",
        "fmt_flac": "🎵 FLAC",
        "fmt_sticker": "🎭 Stickerpack (ZIP)",
        "fmt_gif": "🌀 GIF",
        "fmt_circle": "⭕ Circle",
        "fmt_thumb": "🖼 Thumbnail",
        "fmt_playlist": "📋 Playlist (ZIP)",
        "fmt_shakal": "🗿 Deep-fried",
        "audio_fmt_title": "🎵 Choose audio format:",
        "sticker_downloading": "🎭 Downloading stickerpack...",
        "sticker_done": "🎭 Stickerpack ready! {n} stickers",
        "sticker_not_found": "❌ Stickerpack not found.",
        "search_enter": "🔍 Enter a YouTube search query:",
        "searching": "🔍 Searching...",
        "search_no_results": "❌ No results found.",
        "search_results": "🔍 Search results:",
        "spotify_not_supported": "⚠️ Spotify not supported — use YouTube Music.",
        "pinterest_hint": "📌 Pinterest: video pins only.",
        "yandex_geo_error": "❌ Yandex Music unavailable (geo-block 451).",
        "sticker_enter": "🎭 Send a link: t.me/addstickers/PackName",
        "step_quality": "📐 Step 2 — quality:",
        "step_audio": "🔊 Step 3 — audio level:",
        "step_orient": "📐 Step 4 — orientation, subs, speed:",
        "step_speed": "⚡ Choose speed:",
        "step_trim": "✂️ Trim?",
        "step_trim_gif": "✂️ Trim? (max 60 sec)",
        "circle_menu": "⭕ Circle — adjust or download:",
        "circle_speed": "⚡ {speed}x",
        "circle_audio": "🔊 {label}",
        "circle_download": "⬇️ Download",
        "gif_menu": "🌀 GIF — adjust or download:",
        "gif_speed": "⚡ {speed}x",
        "gif_download": "⬇️ Download",
        "bass_on": "🔊 Bass boost ✅",
        "bass_off": "🔊 Bass boost ❌",
        "shakal_menu": "🗿 Deep-fried — adjust or download:",
        "shakal_speed": "⚡ {speed}x",
        "shakal_bass_on": "🔊 Bass ✅",
        "shakal_bass_off": "🔊 Bass ❌",
        "shakal_download": "⬇️ Deep-fry",
        "trim_yes": "✂️ Trim",
        "trim_no": "⏭ No trim",
        "orient_original": "📱 Original",
        "orient_square": "⬛ 1:1",
        "orient_landscape": "🖼 16:9",
        "subs_on": "📝 Subs ✅",
        "subs_off": "📝 Subs ❌",
        "speed_btn": "⚡ {speed}x",
        "trim_btn": "✂️ Trim",
        "download_btn": "⬇️ Download",
        "audio_mute": "🔇 Mute",
        "audio_quiet": "🔉 Quiet",
        "audio_normal": "🔊 Normal",
        "audio_loud": "📢 Loud",
        "q360": "360p", "q480": "480p", "q720": "720p HD",
        "q1080": "1080p FHD", "qbest": "🏆 Max",
        "speed_half": "🐢 0.5x", "speed_075": "🐌 0.75x", "speed_1": "▶️ 1x",
        "speed_15": "🐇 1.5x", "speed_2": "⚡ 2x",
        "cancel_btn": "❌ Cancel",
        "back_btn": "◀️ Back",
        "downloading": "⏳ Downloading...",
        "trim_enter_start": "✂️ Enter start time (M:SS):\nExample: 0:15 or 1:30",
        "trim_enter_end": "Now enter end time:",
        "trim_invalid": "❌ Invalid format. Example: 0:15",
        "queued": "⏳ Queue ({pos}). Please wait...",
        "preview_loading": "🔍 Getting video info...",
        "preview_download": "⬇️ Download",
        "preview_cancelled": "❌ Cancelled.",
        "merge_start": (
            "🎬 Video merge mode\n\n"
            "Send videos as files (📎 → File), one by one.\n"
            "When done — press «Merge»."
        ),
        "merge_btn": "🔗 Merge",
        "merge_cancel_btn": "❌ Cancel",
        "merge_received": "✅ Video {n} received.",
        "merge_processing": "🔗 Merging...",
        "merge_done": "✅ Videos merged!",
        "merge_need_two": "❌ Need at least 2 videos.",
        "limit_reset": "🔄 Limit reset! {limit} downloads available.",
        "me": (
            "👤 Your stats\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "📥 Total downloads: {total}\n"
            "❤️ Favorite platform: {fav}\n"
            "📅 Today: {today} of {limit}\n\n"
            "Keep going! 🔥"
        ),
        "me_empty": "📭 No downloads yet!",
        "menu_title": "🎛 Main menu\n\nChoose an action 👇",
        "settings_title": "⚙️ Settings",
        "theme_toggle": "🎨 Theme: {theme}",
        "theme_light": "☀️ Light",
        "theme_dark": "🌙 Dark",
        "theme_changed": "🎨 Theme: {theme}",
        "default_fmt": "📦 Format: {fmt}",
        "default_fmt_title": "📦 Choose format:",
        "default_quality": "📐 Quality: {q}",
        "pref_saved": "✅ Saved!",
        "settings_info": (
            "⚙️ Settings\n"
            "━━━━━━━━━━━━━━━\n\n"
            "🎨 Theme: {theme}\n"
            "📦 Format: {fmt}\n"
            "📐 Quality: {quality}\n\n"
            "Tap a button to change 👇"
        ),
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# ПАТЧ-НОТЫ
# ═══════════════════════════════════════════════════════════════════════════════

PATCH_NOTES = {
    "1.1": {
        "ru": (
            "🎉 Обновление v1.1\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "🎛 Добавлено главное меню /menu\n"
            "🌍 Теперь есть English!\n"
            "📊 Личная статистика /me\n"
            "📤 Кнопка «Поделиться» с друзьями\n"
            "🕘 История скачиваний\n\n"
            "Спасибо, что пользуешься! 💜"
        ),
        "en": (
            "🎉 Update v1.1\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "🎛 Main menu /menu added\n"
            "🌍 Russian + English support!\n"
            "📊 Personal stats /me\n"
            "📤 Share with friends button\n"
            "🕘 Download history\n\n"
            "Thanks for using! 💜"
        ),
    },
    "1.2": {
        "ru": (
            "🔥 Обновление v1.2\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "🖼 Скачивание обложек видео\n"
            "⭕ Видео-кружочки для Telegram\n"
            "📋 Очередь загрузок — никаких зависаний\n"
            "⚡ Настройка скорости 0.5x–2x\n"
            "🎬 Полноценная поддержка YouTube\n\n"
            "Стало ещё удобнее! 🚀"
        ),
        "en": (
            "🔥 Update v1.2\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "🖼 Video thumbnail extraction\n"
            "⭕ Telegram video circles\n"
            "📋 Download queue — no freezes\n"
            "⚡ Speed control 0.5x–2x\n"
            "🎬 Full YouTube support\n\n"
            "Better than ever! 🚀"
        ),
    },
    "1.3": {
        "ru": (
            "⚙️ Обновление v1.3 — Под капотом\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔗 Переход на Webhook — быстрее отклик\n"
            "🗄 Redis для хранения данных\n"
            "📦 Автосжатие файлов > 50 МБ\n"
            "🔄 Автообновление загрузчика\n\n"
            "Бот стал стабильнее и быстрее! ⚡"
        ),
        "en": (
            "⚙️ Update v1.3 — Under the Hood\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔗 Webhook mode — faster response\n"
            "🗄 Redis data storage\n"
            "📦 Auto-compress files > 50 MB\n"
            "🔄 Auto-update downloader\n\n"
            "Faster and more stable! ⚡"
        ),
    },
    "1.4": {
        "ru": (
            "✨ Обновление v1.4\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "📊 Прогресс-бар с весёлыми статусами\n"
            "👀 Превью видео перед скачиванием\n"
            "🔄 Кнопка «Скачать ещё раз»\n"
            "🔗 Объединение нескольких видео в одно\n\n"
            "Попробуй — тебе понравится! 🎯"
        ),
        "en": (
            "✨ Update v1.4\n"
            "━━━━━━━━━━━━━━━━━\n\n"
            "📊 Progress bar with fun messages\n"
            "👀 Video preview before download\n"
            "🔄 Download again button\n"
            "🔗 Merge multiple videos into one\n\n"
            "Give it a try! 🎯"
        ),
    },
    "1.5": {
        "ru": (
            "🌟 Обновление v1.5 — Мегапак!\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📌 Pinterest, 🟣 Twitch, 🎞 Vimeo, 📺 Dailymotion\n"
            "🎵 Форматы WAV и FLAC для аудиофилов\n"
            "🎭 Скачивание стикерпаков целиком\n"
            "🔍 Поиск по YouTube прямо в боте\n"
            "🎧 SoundCloud — музыка без границ\n\n"
            "Теперь поддерживается 11+ платформ! 🔥"
        ),
        "en": (
            "🌟 Update v1.5 — Megapack!\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📌 Pinterest, 🟣 Twitch, 🎞 Vimeo, 📺 Dailymotion\n"
            "🎵 WAV and FLAC for audiophiles\n"
            "🎭 Download entire sticker packs\n"
            "🔍 YouTube search right in the bot\n"
            "🎧 SoundCloud — music without borders\n\n"
            "Now supports 11+ platforms! 🔥"
        ),
    },
    "1.7": {
        "ru": (
            "🎉 Обновление v1.7 — Новые эффекты\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌀 Ускорение GIF — теперь можно менять скорость\n"
            "🔊 Басс-буст — мощный бас на видео\n"
            "🗿 Шакал — видео в максимально убитом качестве\n"
            "📸 Instagram — исправлена загрузка (куки)\n"
            "📌 Pinterest — исправлен формат скачивания\n"
            "🔧 ffmpeg через pip — работает на любом хостинге\n\n"
            "Шакалим! 🗿"
        ),
        "en": (
            "🎉 Update v1.7 — New Effects\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🌀 GIF speed — change playback speed\n"
            "🔊 Bass boost — heavy bass on videos\n"
            "🗿 Deep-fried — maximum compression meme quality\n"
            "📸 Instagram — download fixed (cookies)\n"
            "📌 Pinterest — format fix\n"
            "🔧 ffmpeg via pip — works on any hosting\n\n"
            "Deep-fry everything! 🗿"
        ),
    },
    "1.6": {
        "ru": (
            "🛠 Обновление v1.6 — Стабильность\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🐛 Исправлены все известные баги\n"
            "🧹 Полный рефакторинг кода\n"
            "🧠 Оптимизация памяти и скорости\n"
            "📌 Pinterest — теперь скачивает корректно\n"
            "🎵 Яндекс Музыка — поддержка восстановлена\n"
            "🕘 Исправлена кнопка «История»\n\n"
            "Всё летает! 🚀"
        ),
        "en": (
            "🛠 Update v1.6 — Stability\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🐛 All known bugs fixed\n"
            "🧹 Full code refactoring\n"
            "🧠 Memory and speed optimization\n"
            "📌 Pinterest — now downloads correctly\n"
            "🎵 Yandex Music — support restored\n"
            "🕘 History button fixed\n\n"
            "Everything flies! 🚀"
        ),
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════════════════════

def get_lang(context) -> str:
    return context.user_data.get("lang", "ru")


def t(context, key: str, **kw) -> str:
    lang = get_lang(context)
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, key)
    return text.format(**kw) if kw else text


def is_supported_url(url: str) -> bool:
    return any(re.search(p, url, re.IGNORECASE) for p in SUPPORTED_PATTERNS)


def get_platform(url: str) -> str:
    mapping = [
        (r"tiktok\.com", "TikTok"),
        (r"instagram\.com|instagr\.am", "Instagram"),
        (r"youtube\.com|youtu\.be", "YouTube"),
        (r"twitter\.com|x\.com", "Twitter/X"),
        (r"vk\.com", "VK"),
        (r"twitch\.tv", "Twitch"),
        (r"reddit\.com", "Reddit"),
        (r"pinterest\.com|pin\.it", "Pinterest"),
        (r"soundcloud\.com", "SoundCloud"),
        (r"vimeo\.com", "Vimeo"),
        (r"dailymotion\.com|dai\.ly", "Dailymotion"),
        (r"open\.spotify\.com", "Spotify"),
        (r"music\.yandex", "Яндекс Музыка"),
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


def time_str_valid(s: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", s.strip()))


def format_duration(seconds) -> str:
    try:
        s = int(seconds)
        h, m, sec = s // 3600, (s % 3600) // 60, s % 60
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"
    except Exception:
        return "?"


# ═══════════════════════════════════════════════════════════════════════════════
# ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ
# ═══════════════════════════════════════════════════════════════════════════════

def update_stats(user_id: int, platform: str):
    data = get_data()
    uid = str(user_id)
    stats = data.setdefault("stats", {})
    stats["total"] = stats.get("total", 0) + 1
    stats.setdefault("platforms", {})[platform] = stats.get("platforms", {}).get(platform, 0) + 1
    stats.setdefault("users", {})[uid] = stats.get("users", {}).get(uid, 0) + 1
    data.setdefault("user_platforms", {}).setdefault(uid, {})[platform] = \
        data.get("user_platforms", {}).get(uid, {}).get(platform, 0) + 1
    Storage.save(data)


def check_limit(user_id: int) -> tuple[bool, int]:
    if user_id == ADMIN_ID:
        return True, 999  # Админ — безлимит
    data = get_data()
    today_count = data.get("downloads_today", {}).get(str(user_id), 0)
    return today_count < DAILY_LIMIT, DAILY_LIMIT - today_count


def increment_limit(user_id: int):
    data = get_data()
    uid = str(user_id)
    dt = data.setdefault("downloads_today", {})
    dt[uid] = dt.get(uid, 0) + 1
    Storage.save(data)


def is_blocked(user_id: int) -> bool:
    return user_id in get_data().get("blocked", [])


def add_to_history(context, url: str, platform: str):
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
        Storage.save(data)


def load_history_from_db(user_id: int) -> list:
    return get_data().get("histories", {}).get(str(user_id), [])


def get_merged_history(user_id: int, context) -> list:
    """Объединяет историю из сессии и БД."""
    db_history = load_history_from_db(user_id)
    session_history = context.user_data.get("history", [])
    seen, merged = set(), []
    for item in list(session_history) + list(db_history):
        url = item.get("url")
        if url and url not in seen:
            seen.add(url)
            merged.append(item)
    history = merged[:HISTORY_SIZE]
    context.user_data["history"] = history
    return history


def _load_user_prefs(user_id: int, context):
    """Загружает настройки из Redis/JSON."""
    data = get_data()
    uid = str(user_id)
    saved_lang = data.get("user_langs", {}).get(uid)
    if saved_lang and "lang" not in context.user_data:
        context.user_data["lang"] = saved_lang
    prefs = data.get("user_prefs", {}).get(uid, {})
    for key, field in [("theme", "theme"), ("default_format", "format"), ("default_quality", "quality")]:
        if key not in context.user_data and field in prefs:
            context.user_data[key] = prefs[field]


def _save_user_prefs(user_id: int, context):
    """Сохраняет настройки в Redis/JSON."""
    data = get_data()
    uid = str(user_id)
    prefs = data.setdefault("user_prefs", {}).setdefault(uid, {})
    prefs["theme"] = context.user_data.get("theme", "light")
    prefs["format"] = context.user_data.get("default_format", "video")
    prefs["quality"] = context.user_data.get("default_quality", "best")
    Storage.save(data)


# ═══════════════════════════════════════════════════════════════════════════════
# FFMPEG УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════════════════════

def ffmpeg_ok() -> bool:
    if FFMPEG_LOCATION:
        return True
    return shutil.which("ffmpeg") is not None


def _ffmpeg_cmd() -> str:
    """Возвращает путь к ffmpeg бинарнику."""
    if FFMPEG_LOCATION:
        candidate = Path(FFMPEG_LOCATION) / "ffmpeg"
        if candidate.exists():
            return str(candidate)
    return "ffmpeg"


def ffmpeg_run(cmd: list) -> bool:
    if not ffmpeg_ok():
        logger.warning("ffmpeg недоступен")
        return False
    # Подставляем полный путь к ffmpeg
    if cmd and cmd[0] == "ffmpeg":
        cmd = [_ffmpeg_cmd()] + cmd[1:]
    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if result.returncode != 0:
        logger.error("ffmpeg error: %s", result.stderr.decode()[:500])
    return result.returncode == 0


def _ffprobe_cmd() -> str:
    """Возвращает путь к ffprobe бинарнику."""
    if FFMPEG_LOCATION:
        candidate = Path(FFMPEG_LOCATION) / "ffprobe"
        if candidate.exists():
            return str(candidate)
    return "ffprobe"


def get_video_duration(path: Path) -> float:
    try:
        r = subprocess.run(
            [_ffprobe_cmd(), "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=15,
        )
        return float(r.stdout.strip())
    except Exception:
        return 0.0


async def ffmpeg_with_progress(cmd: list, status_msg, label: str, duration: float = 0) -> bool:
    """Запускает ffmpeg с прогресс-баром."""
    # Подставляем полный путь к ffmpeg
    if cmd and cmd[0] == "ffmpeg":
        cmd = [_ffmpeg_cmd()] + cmd[1:]
    # Вставляем -progress перед выходным файлом
    cmd_prog = cmd[:-1] + ["-progress", "pipe:1", "-nostats", cmd[-1]]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_prog,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        last_update = 0.0
        last_pct = -1
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            decoded = line.decode().strip()
            if decoded.startswith("out_time_ms=") and duration > 0:
                try:
                    ms = int(decoded.split("=")[1])
                    pct = min(99, int(ms / 1_000_000 / duration * 100))
                    now = asyncio.get_event_loop().time()
                    if pct != last_pct and now - last_update > 2:
                        last_pct = pct
                        last_update = now
                        try:
                            await status_msg.edit_text(f"{label}\n{make_progress_bar(pct)}")
                        except Exception:
                            pass
                except Exception:
                    pass
        await proc.wait()
        return proc.returncode == 0
    except Exception as e:
        logger.warning("ffmpeg_with_progress fallback: %s", e)
        return ffmpeg_run(cmd)


def apply_audio(path: Path, volume: float) -> Path:
    if volume == 1.0:
        return path
    out = path.with_stem(path.stem + "_audio")
    if volume == 0.0:
        cmd = ["ffmpeg", "-y", "-i", str(path), "-an", "-c:v", "copy", str(out)]
    else:
        cmd = ["ffmpeg", "-y", "-i", str(path), "-filter:a", f"volume={volume}", "-c:v", "copy", str(out)]
    return out if ffmpeg_run(cmd) and out.exists() else path


def apply_orientation(path: Path, orient: str) -> Path:
    if orient == "original":
        return path
    out = path.with_stem(path.stem + "_orient")
    if orient == "square":
        # FIX: правильное экранирование запятой для ffmpeg
        vf = "crop=min(iw\\,ih):min(iw\\,ih),scale=720:720"
    else:
        vf = "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2"
    cmd = ["ffmpeg", "-y", "-i", str(path), "-vf", vf, "-c:a", "copy", str(out)]
    return out if ffmpeg_run(cmd) and out.exists() else path


def apply_trim(path: Path, start: str, end: str) -> Path:
    out = path.with_stem(path.stem + "_trim")
    cmd = ["ffmpeg", "-y", "-i", str(path), "-ss", start, "-to", end, "-c", "copy", str(out)]
    return out if ffmpeg_run(cmd) and out.exists() else path


def apply_speed(path: Path, speed: float) -> Path:
    """Ускоряет/замедляет видео. FIX: корректная цепочка atempo."""
    if speed == 1.0:
        return path
    out = path.with_stem(path.stem + "_speed")
    # atempo поддерживает только 0.5–100.0
    # Для speed=0.5 → setpts=2*PTS, atempo=0.5
    # Для speed=2.0 → setpts=0.5*PTS, atempo=2.0
    atempo_parts = []
    remaining = speed
    while remaining > 2.0:
        atempo_parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        atempo_parts.append("atempo=0.5")
        remaining /= 0.5
    atempo_parts.append(f"atempo={remaining}")
    atempo_filter = ",".join(atempo_parts)

    cmd = [
        "ffmpeg", "-y", "-i", str(path),
        "-vf", f"setpts={1/speed}*PTS",
        "-af", atempo_filter,
        "-c:v", "libx264", "-preset", "fast",
        str(out),
    ]
    return out if ffmpeg_run(cmd) and out.exists() else path


def compress_video(path: Path, target_mb: float = 45.0) -> Path:
    out = path.with_stem(path.stem + "_compressed").with_suffix(".mp4")
    dur = get_video_duration(path)
    if dur > 0:
        target_kbps = max(200, int(target_mb * 8 * 1024 / dur) - 128)
        cmd = [
            "ffmpeg", "-y", "-i", str(path),
            "-c:v", "libx264", "-b:v", f"{target_kbps}k",
            "-c:a", "aac", "-b:a", "128k",
            "-preset", "fast", "-movflags", "+faststart",
            str(out),
        ]
    else:
        cmd = [
            "ffmpeg", "-y", "-i", str(path),
            "-c:v", "libx264", "-crf", "28",
            "-c:a", "aac", "-b:a", "96k", "-preset", "fast",
            str(out),
        ]
    return out if ffmpeg_run(cmd) and out.exists() else path


def apply_bass_boost(path: Path) -> Path:
    """Басс-буст: усиливает низкие частоты."""
    out = path.with_stem(path.stem + "_bass").with_suffix(".mp4")

    # Попытка 1: полный эквалайзер
    cmd = [
        "ffmpeg", "-y", "-i", str(path),
        "-af", (
            "equalizer=f=60:t=h:w=50:g=20,"
            "equalizer=f=120:t=h:w=80:g=12,"
            "equalizer=f=200:t=h:w=100:g=5,"
            "volume=1.8,"
            "alimiter=limit=0.95:attack=5:release=50"
        ),
        "-c:v", "copy",
        str(out),
    ]
    logger.warning("Bass boost cmd1: %s", " ".join(cmd))
    if ffmpeg_run(cmd) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Басс-буст (equalizer) применён: %s", out)
        return out

    # Попытка 2: простой volume boost без alimiter
    logger.warning("Bass boost: equalizer не сработал, пробуем простой volume")
    out.unlink(missing_ok=True)
    cmd2 = [
        "ffmpeg", "-y", "-i", str(path),
        "-af", "volume=3.0",
        "-c:v", "copy",
        str(out),
    ]
    logger.warning("Bass boost cmd2: %s", " ".join(cmd2))
    if ffmpeg_run(cmd2) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Басс-буст (volume) применён: %s", out)
        return out

    # Попытка 3: re-encode всё
    logger.warning("Bass boost: copy не сработал, пробуем re-encode")
    out.unlink(missing_ok=True)
    cmd3 = [
        "ffmpeg", "-y", "-i", str(path),
        "-af", "volume=3.0",
        "-c:v", "libx264", "-preset", "fast",
        "-c:a", "aac",
        str(out),
    ]
    logger.warning("Bass boost cmd3: %s", " ".join(cmd3))
    if ffmpeg_run(cmd3) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Басс-буст (re-encode) применён: %s", out)
        return out

    logger.error("❌ Басс-буст: все попытки не удались")
    return path


def apply_shakal(path: Path, speed: float = 1.0) -> Path:
    """Шакал: максимально убитое качество (мем-эффект). speed — ускорение."""
    out = path.with_stem(path.stem + "_shakal").with_suffix(".mp4")

    # Формируем видеофильтр
    speed_vf = f"setpts={1/speed}*PTS," if speed != 1.0 else ""
    vf_full = (
        f"{speed_vf}"
        "scale=144:-2,"
        "noise=alls=40:allf=t,"
        "eq=contrast=1.8:brightness=0.05:saturation=2.5,"
        "unsharp=5:5:2.0"
    )
    vf_simple = f"{speed_vf}scale=144:-2"

    # Формируем аудиофильтр (atempo для скорости)
    af_parts = []
    if speed != 1.0:
        rem = speed
        while rem > 2.0:
            af_parts.append("atempo=2.0"); rem /= 2.0
        while rem < 0.5:
            af_parts.append("atempo=0.5"); rem /= 0.5
        af_parts.append(f"atempo={rem}")

    # Попытка 1: полный шакал + скорость + аудио
    cmd = [
        "ffmpeg", "-y", "-i", str(path),
        "-vf", vf_full,
    ]
    if af_parts:
        cmd += ["-af", ",".join(af_parts)]
    cmd += [
        "-c:v", "libx264", "-crf", "45", "-preset", "ultrafast",
        "-b:v", "50k", "-maxrate", "80k", "-bufsize", "40k",
        "-c:a", "aac", "-b:a", "24k", "-ar", "22050", "-ac", "1",
        "-movflags", "+faststart",
        str(out),
    ]
    logger.warning("Шакал cmd1: %s", " ".join(cmd))
    if ffmpeg_run(cmd) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Шакал (полный+audio) speed=%s: %s", speed, out)
        return out

    # Попытка 2: полный шакал БЕЗ аудио
    logger.warning("Шакал: попытка 1 не удалась, пробуем без аудио")
    out.unlink(missing_ok=True)
    cmd2 = [
        "ffmpeg", "-y", "-i", str(path),
        "-vf", vf_full,
        "-c:v", "libx264", "-crf", "45", "-preset", "ultrafast",
        "-b:v", "50k", "-an",
        str(out),
    ]
    logger.warning("Шакал cmd2: %s", " ".join(cmd2))
    if ffmpeg_run(cmd2) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Шакал (полный-an) speed=%s: %s", speed, out)
        return out

    # Попытка 3: простой шакал БЕЗ аудио
    logger.warning("Шакал: попытка 2 не удалась, пробуем простой")
    out.unlink(missing_ok=True)
    cmd3 = [
        "ffmpeg", "-y", "-i", str(path),
        "-vf", vf_simple,
        "-c:v", "libx264", "-crf", "51", "-preset", "ultrafast",
        "-an",
        str(out),
    ]
    logger.warning("Шакал cmd3: %s", " ".join(cmd3))
    if ffmpeg_run(cmd3) and out.exists() and out.stat().st_size > 0:
        logger.info("✅ Шакал (простой) speed=%s: %s", speed, out)
        return out

    logger.error("❌ Шакал: все попытки не удались")
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════════════════════

MENU_LABELS = {
    "ru": {
        "download": "⬇️ Скачать видео", "history": "🕘 История",
        "me": "📊 Статистика", "patchnote": "📋 Патч-ноут",
        "help": "❓ Помощь", "lang": "🌍 Язык",
        "share": "📤 Поделиться", "stats": "📊 Стат. бота",
        "blocks": "🚫 Блокировки", "sendpatch": "📢 Разослать",
        "share_text": "Скачиваю видео из TikTok, YouTube и не только! @balerndownloadsbot",
        "settings": "⚙️ Настройки", "merge": "🔗 Объединить",
        "search": "🔍 YouTube", "sticker": "🎭 Стикерпак",
    },
    "en": {
        "download": "⬇️ Download", "history": "🕘 History",
        "me": "📊 Stats", "patchnote": "📋 Patch notes",
        "help": "❓ Help", "lang": "🌍 Language",
        "share": "📤 Share", "stats": "📊 Bot stats",
        "blocks": "🚫 Blocks", "sendpatch": "📢 Send patch",
        "share_text": "Download videos from TikTok, YouTube and more! @balerndownloadsbot",
        "settings": "⚙️ Settings", "merge": "🔗 Merge",
        "search": "🔍 YouTube", "sticker": "🎭 Stickers",
    },
}


def main_menu_keyboard(is_admin: bool = False, lang: str = "ru") -> InlineKeyboardMarkup:
    L = MENU_LABELS.get(lang, MENU_LABELS["ru"])
    rows = [
        [InlineKeyboardButton(L["download"], callback_data="menu_download"),
         InlineKeyboardButton(L["history"], callback_data="menu_history")],
        [InlineKeyboardButton(L["me"], callback_data="menu_me"),
         InlineKeyboardButton(L["patchnote"], callback_data="menu_patchnote")],
        [InlineKeyboardButton(L["help"], callback_data="menu_help"),
         InlineKeyboardButton(L["lang"], callback_data="menu_lang")],
        [InlineKeyboardButton(L["settings"], callback_data="menu_settings"),
         InlineKeyboardButton(L["merge"], callback_data="menu_merge")],
        [InlineKeyboardButton(L["search"], callback_data="menu_search"),
         InlineKeyboardButton(L["sticker"], callback_data="menu_sticker")],
        [InlineKeyboardButton(L["share"], switch_inline_query=L["share_text"])],
    ]
    if is_admin:
        rows.append([
            InlineKeyboardButton(L["stats"], callback_data="menu_stats"),
            InlineKeyboardButton(L["blocks"], callback_data="menu_blocks"),
        ])
        rows.append([InlineKeyboardButton(L["sendpatch"], callback_data="menu_sendpatch")])
    return InlineKeyboardMarkup(rows)


def format_keyboard(lang="ru", default_fmt="", url="") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    def btn(key, cb):
        mark = " ✅" if cb == f"fmt_{default_fmt}" else ""
        return InlineKeyboardButton(T[key] + mark, callback_data=cb)
    rows = [
        [btn("fmt_video", "fmt_video"), btn("fmt_audio", "fmt_audio")],
        [btn("fmt_gif", "fmt_gif"), btn("fmt_circle", "fmt_circle")],
        [btn("fmt_thumb", "fmt_thumb"), btn("fmt_playlist", "fmt_playlist")],
        [btn("fmt_wav", "fmt_wav"), btn("fmt_flac", "fmt_flac")],
        [btn("fmt_shakal", "fmt_shakal")],
    ]
    if url and re.search(STICKER_PATTERN, url, re.IGNORECASE):
        rows.append([btn("fmt_sticker", "fmt_sticker")])
    return InlineKeyboardMarkup(rows)


def quality_keyboard(lang="ru", default_quality="") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    def btn(key, cb):
        qid = cb.replace("quality_", "")
        mark = " ✅" if qid == default_quality else ""
        return InlineKeyboardButton(T[key] + mark, callback_data=cb)
    return InlineKeyboardMarkup([
        [btn("q360", "quality_360"), btn("q480", "quality_480")],
        [btn("q720", "quality_720"), btn("q1080", "quality_1080")],
        [btn("qbest", "quality_best")],
    ])


def audio_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["audio_mute"], callback_data="audio_mute"),
         InlineKeyboardButton(T["audio_quiet"], callback_data="audio_quiet")],
        [InlineKeyboardButton(T["audio_normal"], callback_data="audio_normal"),
         InlineKeyboardButton(T["audio_loud"], callback_data="audio_loud")],
    ])


def speed_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["speed_half"], callback_data="speed_0.5"),
         InlineKeyboardButton(T["speed_075"], callback_data="speed_0.75")],
        [InlineKeyboardButton(T["speed_1"], callback_data="speed_1.0"),
         InlineKeyboardButton(T["speed_15"], callback_data="speed_1.5")],
        [InlineKeyboardButton(T["speed_2"], callback_data="speed_2.0")],
    ])


def orientation_keyboard(subs_on=False, speed="1.0", bass=False, lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["orient_original"], callback_data="orient_original"),
         InlineKeyboardButton(T["orient_square"], callback_data="orient_square")],
        [InlineKeyboardButton(T["orient_landscape"], callback_data="orient_landscape")],
        [InlineKeyboardButton(T["subs_on"] if subs_on else T["subs_off"], callback_data="orient_toggle_subs"),
         InlineKeyboardButton(T["bass_on"] if bass else T["bass_off"], callback_data="orient_bass")],
        [InlineKeyboardButton(T["speed_btn"].format(speed=speed), callback_data="orient_speed"),
         InlineKeyboardButton(T["trim_btn"], callback_data="orient_trim")],
        [InlineKeyboardButton(T["download_btn"], callback_data="orient_download")],
    ])


def gif_menu_keyboard(speed="1.0", lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["gif_speed"].format(speed=speed), callback_data="gif_speed")],
        [InlineKeyboardButton(T["gif_download"], callback_data="gif_download")],
    ])


def shakal_menu_keyboard(speed="1.0", bass=False, lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["shakal_speed"].format(speed=speed), callback_data="shakal_speed"),
         InlineKeyboardButton(T["shakal_bass_on"] if bass else T["shakal_bass_off"], callback_data="shakal_bass")],
        [InlineKeyboardButton(T["shakal_download"], callback_data="shakal_download")],
    ])


def circle_menu_keyboard(speed="1.0", audio="normal", lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    audio_labels = {"mute": "🔇", "quiet": "🔉", "normal": "🔊", "loud": "📢"}
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["circle_speed"].format(speed=speed), callback_data="circle_speed"),
         InlineKeyboardButton(T["circle_audio"].format(label=audio_labels.get(audio, "🔊")), callback_data="circle_audio")],
        [InlineKeyboardButton(T["circle_download"], callback_data="circle_download")],
    ])


def trim_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["trim_yes"], callback_data="trim_yes"),
         InlineKeyboardButton(T["trim_no"], callback_data="trim_no")],
        [InlineKeyboardButton("◀️ " + T["back_btn"], callback_data="trim_back")],
    ])


def cancel_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([[InlineKeyboardButton(T["cancel_btn"], callback_data="cancel_download")]])


def preview_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["preview_download"], callback_data="preview_confirm"),
         InlineKeyboardButton(T["cancel_btn"], callback_data="preview_cancel")],
    ])


def merge_keyboard(lang="ru", count=0) -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    if lang == "ru":
        label = f"🔗 Объединить ({count})" if count >= 2 else f"🔗 Нужно ещё {max(0, 2 - count)}"
    else:
        label = f"🔗 Merge ({count})" if count >= 2 else f"🔗 Need {max(0, 2 - count)} more"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data="merge_do"),
         InlineKeyboardButton(T["merge_cancel_btn"], callback_data="merge_cancel")],
    ])


def history_keyboard(history: list) -> InlineKeyboardMarkup:
    rows = []
    for i, item in enumerate(history):
        platform = item.get("platform", "Видео")
        ts = item.get("time", "")[:10]
        rows.append([InlineKeyboardButton(f"{platform} • {ts}", callback_data=f"history_{i}")])
    rows.append([InlineKeyboardButton("❌", callback_data="history_close")])
    return InlineKeyboardMarkup(rows)


def back_keyboard(lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([[InlineKeyboardButton(T["back_btn"], callback_data="menu_back")]])


def lang_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("◀️", callback_data="menu_back")],
    ])


def patchnote_keyboard(version: str) -> InlineKeyboardMarkup:
    versions = sorted(PATCH_NOTES.keys(), key=lambda v: [int(x) for x in v.split(".")], reverse=True)
    rows, row = [], []
    for v in versions:
        mark = " ✅" if v == version else ""
        row.append(InlineKeyboardButton(f"v{v}{mark}", callback_data=f"patch_nav_{v}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀️", callback_data="menu_back")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(theme, fmt, quality, lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
    fmt_labels = {"video": "🎬", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕", "shakal": "🗿"}
    q_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Max"}
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["theme_toggle"].format(theme=theme_label), callback_data="settings_theme")],
        [InlineKeyboardButton(T["default_fmt"].format(fmt=fmt_labels.get(fmt, fmt)), callback_data="settings_fmt")],
        [InlineKeyboardButton(T["default_quality"].format(q=q_labels.get(quality, quality)), callback_data="settings_quality")],
        [InlineKeyboardButton(T["back_btn"], callback_data="menu_back")],
    ])


def settings_fmt_keyboard(current, lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    fmts = [("video", T["fmt_video"]), ("audio", T["fmt_audio"]),
            ("gif", T["fmt_gif"]), ("circle", T["fmt_circle"])]
    rows = [[InlineKeyboardButton(fl + (" ✅" if fid == current else ""), callback_data=f"setfmt_{fid}")]
            for fid, fl in fmts]
    rows.append([InlineKeyboardButton(T["back_btn"], callback_data="settings_back")])
    return InlineKeyboardMarkup(rows)


def settings_quality_keyboard(current, lang="ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    qs = [("360", T["q360"]), ("480", T["q480"]), ("720", T["q720"]),
          ("1080", T["q1080"]), ("best", T["qbest"])]
    rows = [[InlineKeyboardButton(ql + (" ✅" if qid == current else ""), callback_data=f"setquality_{qid}")]
            for qid, ql in qs]
    rows.append([InlineKeyboardButton(T["back_btn"], callback_data="settings_back")])
    return InlineKeyboardMarkup(rows)


def persistent_menu_keyboard(lang="ru") -> ReplyKeyboardMarkup:
    label = "🎛 Меню" if lang == "ru" else "🎛 Menu"
    return ReplyKeyboardMarkup(
        [[KeyboardButton(label)]],
        resize_keyboard=True, is_persistent=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# СКАЧИВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def _ydl_base_opts(url: str = "") -> dict:
    """Базовые опции yt-dlp с учётом платформы."""
    url_lower = url.lower() if url else ""

    # User-Agent по платформе
    if "instagram.com" in url_lower or "instagr.am" in url_lower:
        user_agent = (
            "Instagram 317.0.0.34.109 Android (31/12; 420dpi; 1080x2280; "
            "samsung; SM-G991B; o1s; exynos2100; en_US; 556895435)"
        )
    elif "pinterest" in url_lower or "pin.it" in url_lower:
        user_agent = (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
            "Mobile/15E148 Safari/604.1"
        )
    else:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

    opts = {
        "quiet": True,
        "no_warnings": True,
        "http_headers": {"User-Agent": user_agent},
        "socket_timeout": 30,
        "extractor_args": {
            "youtube": {"player_client": ["ios", "android", "web"]},
            "instagram": {"api_endpoint": "media"},
        },
        "geo_bypass": True,
        "geo_bypass_country": "RU",
        "nocheckcertificate": True,
    }

    # Pinterest — не проверять форматы, разрешить любой
    if "pinterest" in url_lower or "pin.it" in url_lower:
        opts["no_check_formats"] = True
        opts["format_sort"] = ["res", "ext:mp4:jpg"]
        opts["format_sort_force"] = True
        opts["check_formats"] = False
        opts["ignore_no_formats_error"] = True

    # Instagram — referer, app ID, не проверять форматы
    if "instagram.com" in url_lower or "instagr.am" in url_lower:
        opts["http_headers"]["Referer"] = "https://www.instagram.com/"
        opts["http_headers"]["X-IG-App-ID"] = "936619743392459"
        opts["no_check_formats"] = True
        opts["format_sort"] = ["res", "ext:mp4"]

    if FFMPEG_LOCATION:
        opts["ffmpeg_location"] = FFMPEG_LOCATION
    cookies = Path("cookies.txt")
    if cookies.exists():
        opts["cookiefile"] = str(cookies)
    return opts


async def download_thumbnail(url: str, output_path: Path) -> Path | None:
    opts = _ydl_base_opts(url)
    opts.update({
        "skip_download": True,
        "writethumbnail": True,
        "outtmpl": str(output_path / "thumb_%(id)s"),
    })
    loop = asyncio.get_event_loop()

    def _dl():
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            thumb_id = info.get("id", "thumb")
            for ext in ("jpg", "jpeg", "png", "webp"):
                p = output_path / f"thumb_{thumb_id}.{ext}"
                if p.exists():
                    return p
            files = list(output_path.glob(f"thumb_{thumb_id}*"))
            return files[0] if files else None

    try:
        return await loop.run_in_executor(None, _dl)
    except Exception as e:
        logger.error("Thumbnail error: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# INSTAGRAM — загрузчик через Cobalt API + ddinstagram + embed (без куки)
# ═══════════════════════════════════════════════════════════════════════════════

# Публичные Cobalt API инстансы (обновлять по мере необходимости)
# Список можно обновить из https://instances.cobalt.best/api
_COBALT_INSTANCES = [
    "https://api.cobalt.tools",
    "https://cobalt-api.kwiatekmiki.com",
    "https://cobalt.api.timelessnesses.me",
    "https://api.cobalt.tskau.team",
    "https://cobalt-api.ayo.tf",
    "https://cobalt.canine.tools",
]

_COBALT_CACHE: dict = {"instances": None, "ts": 0}


def _ig_extract_shortcode(url: str) -> str | None:
    """Извлекает shortcode из Instagram URL."""
    for pat in [
        r'instagram\.com/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)',
        r'instagr\.am/(?:p|reel)/([A-Za-z0-9_-]+)',
    ]:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def _ig_http_request(url: str, headers: dict = None, data: bytes = None,
                     timeout: int = 15, method: str = None) -> bytes | None:
    """Универсальный HTTP-запрос с обработкой ошибок."""
    if headers is None:
        headers = {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
            # Иногда полезные данные приходят даже в ошибочных ответах
            if body and len(body) > 100:
                return body
        except Exception:
            pass
        logger.debug("HTTP %s → %s", url[:80], e.code)
    except Exception as e:
        logger.debug("HTTP %s → %s", url[:80], e)
    return None


def _ig_download_file(url: str, output_path: Path, shortcode: str, ext: str) -> Path | None:
    """Скачивает файл по прямой ссылке."""
    out_file = output_path / f"{shortcode}.{ext}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
    }
    raw = _ig_http_request(url, headers=headers, timeout=30)
    if raw and len(raw) > 1000:
        out_file.write_bytes(raw)
        logger.info("IG: скачан %s (%d bytes)", shortcode, len(raw))
        return out_file
    out_file.unlink(missing_ok=True)
    return None


def _cobalt_get_instances() -> list[str]:
    """Получает список Cobalt-инстансов (кэш 1 час)."""
    import time as _time
    now = _time.time()
    if _COBALT_CACHE["instances"] and (now - _COBALT_CACHE["ts"]) < 3600:
        return _COBALT_CACHE["instances"]

    # Пробуем подгрузить актуальный список
    try:
        headers = {
            "User-Agent": "tg-video-bot/1.6 (+https://github.com)",
            "Accept": "application/json",
        }
        raw = _ig_http_request(
            "https://instances.cobalt.best/api/instances.json",
            headers=headers, timeout=8,
        )
        if raw:
            instances_data = json.loads(raw.decode("utf-8", errors="replace"))
            urls = []
            for inst in instances_data:
                if not isinstance(inst, dict):
                    continue
                if not inst.get("online", False):
                    continue
                api = inst.get("api", "")
                proto = inst.get("protocol", "https")
                # Проверяем поддержку Instagram
                services = inst.get("services", {})
                if services.get("instagram") is True:
                    info = inst.get("info", {})
                    # Предпочитаем без auth
                    if not info.get("auth", False):
                        urls.append(f"{proto}://{api}")
            if urls:
                _COBALT_CACHE["instances"] = urls[:10]
                _COBALT_CACHE["ts"] = now
                logger.info("Cobalt: загружено %d инстансов", len(urls))
                return urls[:10]
    except Exception as e:
        logger.debug("Cobalt instances fetch error: %s", e)

    return _COBALT_INSTANCES  # fallback на захардкоженный список


def _cobalt_try_download(url: str, output_path: Path, shortcode: str) -> Path | None:
    """Метод 1: Cobalt API — POST {url} → получаем прямую ссылку."""
    instances = _cobalt_get_instances()

    body = json.dumps({
        "url": url,
        "videoQuality": "1080",
        "filenameStyle": "basic",
    }).encode("utf-8")

    for api_url in instances:
        endpoint = api_url.rstrip("/") + "/"
        logger.warning("IG Cobalt: пробую %s", endpoint)

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "tg-video-bot/1.6",
        }

        raw = _ig_http_request(endpoint, headers=headers, data=body, timeout=12)
        if not raw:
            logger.warning("IG Cobalt: %s — нет ответа", endpoint)
            continue

        try:
            resp = json.loads(raw.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("IG Cobalt: %s — не JSON (%d bytes)", endpoint, len(raw))
            continue

        status = resp.get("status")
        dl_url = resp.get("url")
        logger.warning("IG Cobalt: %s → status=%s, url=%s", endpoint, status, bool(dl_url))

        if status == "error":
            err = resp.get("error", {})
            err_code = err.get("code", "") if isinstance(err, dict) else str(err)
            logger.warning("IG Cobalt %s error: %s", endpoint, err_code)
            continue

        # status: tunnel / redirect / stream — есть url для скачивания
        if dl_url and status in ("tunnel", "redirect", "stream"):
            logger.warning("IG Cobalt (%s): нашёл URL (%s)", endpoint, status)
            result = _ig_download_file(dl_url, output_path, shortcode, "mp4")
            if result:
                return result

        # status: picker — несколько медиа, берём первое видео
        if status == "picker":
            picker_items = resp.get("picker", []) or resp.get("audio_items", [])
            for item in picker_items:
                item_url = item.get("url")
                item_type = item.get("type", "video")
                if item_url and "video" in item_type:
                    result = _ig_download_file(item_url, output_path, shortcode, "mp4")
                    if result:
                        return result
            # Если только фото — берём первое
            for item in picker_items:
                item_url = item.get("url")
                if item_url:
                    ext = "jpg" if "image" in item.get("type", "") else "mp4"
                    result = _ig_download_file(item_url, output_path, shortcode, ext)
                    if result:
                        return result

    logger.warning("IG Cobalt: все инстансы не сработали")
    return None


def _ddinstagram_try_download(url: str, output_path: Path, shortcode: str) -> Path | None:
    """Метод 2: ddinstagram (InstaFix) — парсим og:video из HTML."""
    # d.ddinstagram.com при запросе с бот-UA возвращает og:video
    # Пробуем и как /p/ и как /reel/
    content_type = "reel" if ("/reel/" in url or "/reels/" in url) else "p"
    test_urls = [
        f"https://d.ddinstagram.com/{content_type}/{shortcode}/",
        f"https://d.ddinstagram.com/p/{shortcode}/",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)",
        "Accept": "text/html,*/*",
    }

    for test_url in test_urls:
        logger.warning("IG ddinstagram: %s", test_url)
        raw = _ig_http_request(test_url, headers=headers, timeout=12)
        if not raw:
            logger.warning("IG ddinstagram: %s — нет ответа", test_url)
            continue

        html = raw.decode("utf-8", errors="replace")
        logger.warning("IG ddinstagram: получил %d chars", len(html))

        # Ищем og:video
        for pat in [
            r'<meta\s+(?:property|name)=["\']og:video["\'][^>]*content=["\'](https?://[^"\']+)["\']',
            r'content=["\'](https?://[^"\']+)["\'][^>]*(?:property|name)=["\']og:video["\']',
            r'"video_url"\s*:\s*"(https?://[^"]+)"',
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                video_url = m.group(1).replace("\\u0026", "&").replace("\\/", "/").replace("&amp;", "&")
                logger.info("IG ddinstagram: нашёл видео для %s", shortcode)
                result = _ig_download_file(video_url, output_path, shortcode, "mp4")
                if result:
                    return result

        # Ищем og:image (фото-пост)
        for pat in [
            r'<meta\s+(?:property|name)=["\']og:image["\'][^>]*content=["\'](https?://[^"\']+)["\']',
            r'content=["\'](https?://[^"\']+)["\'][^>]*(?:property|name)=["\']og:image["\']',
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                img_url = m.group(1).replace("\\u0026", "&").replace("\\/", "/").replace("&amp;", "&")
                if "s150x150" not in img_url:
                    result = _ig_download_file(img_url, output_path, shortcode, "jpg")
                    if result:
                        return result

    return None


def _ig_embed_try_download(url: str, output_path: Path, shortcode: str) -> Path | None:
    """Метод 3: Instagram embed page — парсим video_url из HTML/JSON."""
    embed_url = f"https://www.instagram.com/p/{shortcode}/embed/captioned/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,*/*",
        "Referer": "https://www.instagram.com/",
    }
    logger.warning("IG embed: %s", embed_url)
    raw = _ig_http_request(embed_url, headers=headers, timeout=12)
    if not raw:
        logger.warning("IG embed: нет ответа")
        return None

    html = raw.decode("utf-8", errors="replace")
    for pat in [
        r'"video_url"\s*:\s*"(https?://[^"]+)"',
        r'"contentUrl"\s*:\s*"(https?://[^"]+)"',
        r'<source[^>]+src=["\'](https?://[^"\']+\.mp4[^"\']*)["\']',
        r'data-video-url=["\'](https?://[^"\']+)["\']',
    ]:
        m = re.search(pat, html)
        if m:
            video_url = m.group(1).replace("\\u0026", "&").replace("\\/", "/")
            logger.info("IG embed: нашёл видео для %s", shortcode)
            result = _ig_download_file(video_url, output_path, shortcode, "mp4")
            if result:
                return result
    return None


async def _instagram_direct_download(url: str, output_path: Path, fmt: str = "video") -> Path | None:
    """
    Instagram загрузчик — 3 метода БЕЗ куки/логина:
      1) Cobalt API (публичные инстансы с куками на их стороне)
      2) ddinstagram/InstaFix (proxy-сервис)
      3) Instagram embed page
    Если все 3 не сработали → fallback на yt-dlp в download_video.
    """
    shortcode = _ig_extract_shortcode(url)
    if not shortcode:
        return None

    loop = asyncio.get_running_loop()

    def _try_all() -> Path | None:
        # Метод 1: Cobalt API (самый надёжный — у них свои куки)
        result = _cobalt_try_download(url, output_path, shortcode)
        if result:
            return result

        # Метод 2: ddinstagram / InstaFix
        result = _ddinstagram_try_download(url, output_path, shortcode)
        if result:
            return result

        # Метод 3: Instagram embed page
        result = _ig_embed_try_download(url, output_path, shortcode)
        if result:
            return result

        logger.warning("IG: все 3 метода не сработали для %s", shortcode)
        return None

    try:
        return await loop.run_in_executor(None, _try_all)
    except Exception as e:
        logger.error("IG direct download error: %s", e)
        return None


async def download_video(
    url, quality, output_path, status_msg, cancel_flag,
    fmt="video", lang="ru", audio_codec="mp3",
) -> Path | None:
    url_lower = str(url).lower()

    # Instagram — сначала пробуем прямой метод (без yt-dlp)
    is_instagram = any(p in url_lower for p in ["instagram.com", "instagr.am"])
    if is_instagram and fmt in ("video", "gif", "circle", "shakal"):
        logger.warning("IG: пробуем прямой загрузчик для %s", url)
        ig_result = await _instagram_direct_download(url, output_path, fmt)
        if ig_result:
            return ig_result
        logger.warning("IG: прямой метод не сработал, пробуем yt-dlp")

    format_str = QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"])
    if fmt in ("audio", "wav", "flac"):
        format_str = "bestaudio/best"
    elif fmt in ("gif", "circle"):
        format_str = "best[ext=mp4]/best[ext=webm]/best"
    elif fmt == "shakal":
        format_str = "worst[ext=mp4]/worst/best[height<=360]"

    # Pinterest/Twitch/Reddit/Instagram — ограниченные форматы, используем best
    is_simple_platform = any(p in str(url).lower() for p in [
        "pinterest.com", "pin.it", "reddit.com", "dailymotion.com", "dai.ly",
        "instagram.com", "instagr.am",
    ])
    is_pinterest = any(p in str(url).lower() for p in ["pinterest.com", "pin.it"])
    if is_simple_platform and fmt not in ("audio", "wav", "flac", "shakal"):
        format_str = "best"

    last_update = {"pct": -1}
    loop = asyncio.get_running_loop()

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
                    try:
                        asyncio.run_coroutine_threadsafe(
                            status_msg.edit_text(
                                f"⏳ {get_funny_status(pct)}",
                                reply_markup=cancel_keyboard(lang),
                            ), loop,
                        )
                    except Exception:
                        pass

    opts = _ydl_base_opts(url)
    opts.update({
        "outtmpl": str(output_path / "%(id)s.%(ext)s"),
        "format": format_str + "/best",
        "merge_output_format": None if is_simple_platform else ("mp4" if fmt not in ("audio", "wav", "flac") else None),
        "progress_hooks": [progress_hook],
    })

    # Глобально: если ffmpeg не найден — отключаем мерж для ВСЕХ платформ
    if not FFMPEG_LOCATION:
        if fmt != "shakal":
            opts["format"] = "best/bestvideo/bestaudio"
        opts["merge_output_format"] = None
        opts["postprocessors"] = []

    # Pinterest — максимально мягкие настройки формата (БЕЗ мержа!)
    if is_pinterest:
        opts["format"] = "best/bestvideo/bestaudio"
        opts["no_check_formats"] = True
        opts["ignore_no_formats_error"] = True
        opts["check_formats"] = False
        opts["compat_opts"] = {"format-spec"}
        opts.pop("merge_output_format", None)
        opts["postprocessors"] = []

    # Аудио постпроцессоры
    if fmt in ("audio", "wav", "flac"):
        codec = audio_codec if fmt == "audio" else fmt
        pp = [{"key": "FFmpegExtractAudio", "preferredcodec": codec,
               "preferredquality": "0" if codec in ("wav", "flac") else "192"}]
        if codec in ("mp3", "flac", "ogg", "opus"):
            pp.append({"key": "FFmpegMetadata"})
            pp.append({"key": "EmbedThumbnail"})
            opts["writethumbnail"] = True
        opts["postprocessors"] = pp

    def _download():
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            p = Path(filename)
            if fmt in ("audio", "wav", "flac"):
                codec = audio_codec if fmt == "audio" else fmt
                for ext in (codec, "mp3", "opus", "m4a", "webm"):
                    candidate = p.with_suffix(f".{ext}")
                    if candidate.exists():
                        return candidate
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
                try:
                    asyncio.run_coroutine_threadsafe(
                        status_msg.edit_text(f"⏳ 🔄 Попытка {attempt + 1}/3..."),
                        loop,
                    )
                except Exception:
                    pass
                await asyncio.sleep(delay)
                last_update["pct"] = -1
            return await loop.run_in_executor(None, _download)
        except Exception as e:
            err_str = str(e)
            if "CANCELLED" in err_str:
                return None
            logger.error("Попытка %d/3: %s", attempt + 1, e)
            # Instagram: «login required» — ретрай бесполезен
            if is_instagram and ("login required" in err_str.lower() or "rate-limit" in err_str.lower()):
                logger.error("IG: требуется авторизация — ретрай отменён")
                cancel_flag["_ig_login_required"] = True
                break
            # Pinterest: «format not available» — пробуем без указания формата
            if is_pinterest and "format" in err_str.lower() and "not available" in err_str.lower():
                if attempt == 0:
                    logger.warning("Pinterest: формат недоступен — пробую без формата")
                    opts["format"] = None
                    opts.pop("format_sort", None)
                    opts.pop("format_sort_force", None)
                    continue
                logger.error("Pinterest: формат недоступен — ретрай отменён")
                break
    return None


async def download_playlist(url, quality, output_path, status_msg, cancel_flag, lang="ru") -> Path | None:
    playlist_dir = output_path / "playlist_tmp"
    playlist_dir.mkdir(exist_ok=True)
    count = {"n": 0}
    loop = asyncio.get_running_loop()

    def progress_hook(d):
        if cancel_flag.get("cancelled"):
            raise Exception("CANCELLED")
        if d["status"] == "finished":
            count["n"] += 1
            try:
                asyncio.run_coroutine_threadsafe(
                    status_msg.edit_text(f"⏳ Скачано: {count['n']}",
                                         reply_markup=cancel_keyboard(lang)),
                    loop,
                )
            except Exception:
                pass

    opts = _ydl_base_opts(url)
    opts.update({
        "outtmpl": str(playlist_dir / "%(playlist_index)s_%(title)s.%(ext)s"),
        "format": QUALITY_OPTIONS.get(quality, QUALITY_OPTIONS["best"]),
        "merge_output_format": "mp4",
        "progress_hooks": [progress_hook],
        "noplaylist": False,
        "playlistend": 20,
    })

    def _download():
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        zp = output_path / "playlist.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            for f in playlist_dir.iterdir():
                zf.write(f, f.name)
        return zp

    try:
        zp = await loop.run_in_executor(None, _download)
        for f in playlist_dir.iterdir():
            f.unlink(missing_ok=True)
        playlist_dir.rmdir()
        return zp
    except Exception as e:
        logger.error("Playlist error: %s", e)
        return None


async def download_sticker_pack(pack_name: str, bot, dest_dir: Path) -> tuple[Path | None, int]:
    try:
        sticker_set = await bot.get_sticker_set(pack_name)
        zp = dest_dir / f"{pack_name}.zip"
        count = 0
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, sticker in enumerate(sticker_set.stickers):
                try:
                    file = await bot.get_file(sticker.file_id)
                    data = await file.download_as_bytearray()
                    ext = "webm" if sticker.is_video else ("tgs" if sticker.is_animated else "webp")
                    zf.writestr(f"{i + 1:03d}.{ext}", bytes(data))
                    count += 1
                except Exception as e:
                    logger.warning("sticker %d: %s", i, e)
        return zp, count
    except Exception as e:
        logger.error("download_sticker_pack: %s", e)
        return None, 0


async def youtube_search(query: str, max_results: int = 5) -> list[dict]:
    opts = {
        "quiet": True, "no_warnings": True, "skip_download": True,
        "extract_flat": True,
        "extractor_args": {"youtube": {"player_client": ["ios"]}},
        "socket_timeout": 15,
    }
    try:
        loop = asyncio.get_event_loop()
        def _search():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
                return info.get("entries", []) if info else []
        results = await asyncio.wait_for(loop.run_in_executor(None, _search), timeout=20)
        return [r for r in results if r]
    except Exception as e:
        logger.warning("youtube_search: %s", e)
        return []


async def fetch_video_info(url: str) -> dict | None:
    opts = _ydl_base_opts()
    opts["skip_download"] = True
    try:
        loop = asyncio.get_event_loop()
        def _get():
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        return await asyncio.wait_for(loop.run_in_executor(None, _get), timeout=30)
    except Exception as e:
        logger.warning("fetch_video_info: %s", e)
        return None


async def _add_subtitles(url: str, video_path: Path, platform: str) -> tuple[Path, str | None]:
    if platform == "TikTok":
        return video_path, "⚠️ TikTok не поддерживает субтитры"
    if not ffmpeg_ok():
        return video_path, "⚠️ ffmpeg недоступен"

    out = video_path.with_stem(video_path.stem + "_sub")
    opts = _ydl_base_opts()
    opts.update({
        "skip_download": True, "writesubtitles": True,
        "writeautomaticsub": True, "subtitlesformat": "srt",
        "outtmpl": str(video_path.with_suffix("")),
    })
    loop = asyncio.get_event_loop()

    def _dl():
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])

    try:
        await loop.run_in_executor(None, _dl)
        srt_files = list(video_path.parent.glob(video_path.stem + "*.srt"))
        if not srt_files:
            return video_path, "⚠️ Субтитры недоступны"
        srt_file = srt_files[0]
        srt_str = str(srt_file).replace("\\", "/").replace(":", "\\:")
        cmd = ["ffmpeg", "-y", "-i", str(video_path), "-vf", f"subtitles={srt_str}", "-c:a", "copy", str(out)]
        ok = ffmpeg_run(cmd)
        srt_file.unlink(missing_ok=True)
        return (out, None) if ok and out.exists() else (video_path, "⚠️ Не удалось вшить субтитры")
    except Exception as e:
        logger.error("Subtitles: %s", e)
        return video_path, "⚠️ Субтитры недоступны"


def merge_videos(paths: list[Path], output: Path) -> bool:
    list_file = output.with_suffix(".txt")
    with open(list_file, "w") as f:
        for p in paths:
            f.write(f"file '{p.absolute()}'\n")
    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(list_file), "-c", "copy", str(output)]
    ok = ffmpeg_run(cmd)
    list_file.unlink(missing_ok=True)
    return ok


# ═══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════════════

async def safe_edit(query, text, reply_markup=None, parse_mode=None):
    """Безопасно редактирует сообщение."""
    for method in (
        lambda: query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode),
        lambda: query.edit_message_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode),
        lambda: query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode),
    ):
        try:
            await method()
            return
        except Exception:
            continue


def get_user_theme(context) -> str:
    return context.user_data.get("theme", "light")


async def send_menu_photo(target, caption, reply_markup, context, gif=False):
    """Отправляет меню с фото/GIF из локальных файлов."""
    global _GIF_FILE_ID
    theme = get_user_theme(context)
    lang = get_lang(context)
    chat_id = target.chat_id if hasattr(target, "chat_id") else target.id

    # GIF при /start
    if gif and MENU_GIF_FILE.exists():
        try:
            if _GIF_FILE_ID:
                src = _GIF_FILE_ID
            else:
                src = open(MENU_GIF_FILE, "rb")
            if hasattr(target, "reply_animation"):
                msg = await target.reply_animation(animation=src, caption=caption, reply_markup=reply_markup)
            else:
                msg = await context.bot.send_animation(chat_id=chat_id, animation=src, caption=caption, reply_markup=reply_markup)
            if not _GIF_FILE_ID and msg.animation:
                _GIF_FILE_ID = msg.animation.file_id
            return
        except Exception as e:
            logger.warning("GIF send failed: %s", e)

    # Фото меню (тема + язык)
    cache_key = f"{theme}_{lang}"
    photo_path = _menu_photo_path(theme, lang)
    cached = _PHOTO_CACHE.get(cache_key)
    try:
        if cached:
            src = cached
        elif photo_path.exists():
            src = open(photo_path, "rb")
        else:
            if hasattr(target, "reply_text"):
                await target.reply_text(caption, reply_markup=reply_markup)
            else:
                await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=reply_markup)
            return

        if hasattr(target, "reply_photo"):
            msg = await target.reply_photo(photo=src, caption=caption, reply_markup=reply_markup)
        else:
            msg = await context.bot.send_photo(chat_id=chat_id, photo=src, caption=caption, reply_markup=reply_markup)
        if cache_key not in _PHOTO_CACHE and msg.photo:
            _PHOTO_CACHE[cache_key] = msg.photo[-1].file_id
    except Exception:
        if hasattr(target, "reply_text"):
            await target.reply_text(caption, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=reply_markup)


async def _notify_admin(user, platform, fmt, context):
    try:
        fmt_labels = {"video": "🎬", "audio": "🎵", "gif": "🌀",
                      "circle": "⭕", "thumb": "🖼", "playlist": "📋",
                      "wav": "🎵 WAV", "flac": "🎵 FLAC", "shakal": "🗿"}
        name = user.full_name or "?"
        username = f"@{user.username}" if user.username else ""
        await context.bot.send_message(
            ADMIN_ID,
            f"📥 {name} {username}\n🆔 {user.id}\n📱 {platform} • {fmt_labels.get(fmt, fmt)}",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ КОНТЕКСТА ПОЛЬЗОВАТЕЛЯ
# ═══════════════════════════════════════════════════════════════════════════════

def init_download_context(context, url: str, platform: str):
    """Сбрасывает контекст скачивания."""
    context.user_data.update({
        "pending_url": url,
        "platform": platform,
        "cancel_flag": {"cancelled": False},
        "trim_start": None,
        "trim_end": None,
        "subtitles": False,
        "bass_boost": False,
        "waiting_trim": False,
        "speed": "1.0",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    context.user_data["_uid"] = str(user.id)
    _load_user_prefs(user.id, context)
    lang = get_lang(context)
    track_user(user.id, lang)

    await update.message.reply_text("👇", reply_markup=persistent_menu_keyboard(lang))
    await send_menu_photo(
        update.message, t(context, "start_caption"),
        main_menu_keyboard(user.id == ADMIN_ID, lang), context, gif=True,
    )


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(context)
    track_user(user.id, lang)
    await send_menu_photo(
        update.message, t(context, "menu_title"),
        main_menu_keyboard(user.id == ADMIN_ID, lang), context,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(t(context, "help"))


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    history = get_merged_history(update.effective_user.id, context)
    if not history:
        await update.message.reply_text(t(context, "history_empty"))
        return
    await update.message.reply_text(t(context, "history_title"), reply_markup=history_keyboard(history))


async def cmd_me(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    data = get_data()
    uid = str(user.id)
    total = data.get("stats", {}).get("users", {}).get(uid, 0)
    if total == 0:
        await update.message.reply_text(t(context, "me_empty"))
        return
    up = data.get("user_platforms", {}).get(uid, {})
    fav = max(up.items(), key=lambda x: x[1])[0] if up else "—"
    today = data.get("downloads_today", {}).get(uid, 0)
    await update.message.reply_text(t(context, "me", total=total, fav=fav, today=today, limit=DAILY_LIMIT))


async def cmd_patchnote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = PATCH_NOTES.get(BOT_VERSION)
    if not notes:
        await update.message.reply_text(f"📋 v{BOT_VERSION} — нет патч-нотов.")
        return
    lang = get_lang(context)
    await update.message.reply_text(notes.get(lang, notes.get("ru", "")), reply_markup=patchnote_keyboard(BOT_VERSION))


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    data = get_data()
    stats = data.get("stats", {})
    total = stats.get("total", 0)
    platforms = stats.get("platforms", {})
    top = sorted(platforms.items(), key=lambda x: x[1], reverse=True)[:5]
    top_str = "\n".join(f"  {p}: {c}" for p, c in top) or "  —"
    await update.message.reply_text(
        f"📊 Статистика:\n\nВсего: {total}\n"
        f"Пользователей: {len(stats.get('users', {}))}\n"
        f"Активных: {len(ACTIVE_USERS)}\n\n"
        f"Топ:\n{top_str}",
    )


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not context.args:
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
        Storage.save(data)
        await update.message.reply_text(f"✅ {uid} заблокирован.")
    else:
        await update.message.reply_text("Уже заблокирован.")


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not context.args:
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
        Storage.save(data)
        await update.message.reply_text(f"✅ {uid} разблокирован.")


async def cmd_sendpatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    version = context.args[0] if context.args else BOT_VERSION
    notes = PATCH_NOTES.get(version)
    if not notes or not ACTIVE_USERS:
        await update.message.reply_text("❌ Нет данных для рассылки.")
        return
    sent, failed = 0, 0
    for uid, lang in list(ACTIVE_USERS.items()):
        if uid == ADMIN_ID:
            continue
        try:
            await context.bot.send_message(uid, notes.get(lang, notes.get("ru", "")))
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Отправлено: {sent}, ошибок: {failed}")


# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИК ТЕКСТА
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = (update.message.text or "").strip()
    lang = get_lang(context)

    # Режим стикерпака
    if context.user_data.get("waiting_sticker"):
        context.user_data["waiting_sticker"] = False
        m = re.search(r"(?:https?://)?t\.me/addstickers/([A-Za-z0-9_]+)", text)
        if not m:
            await update.message.reply_text("❌ Неверная ссылка.")
            return
        pack = m.group(1)
        await update.message.reply_text(t(context, "sticker_downloading"))
        zp, count = await download_sticker_pack(pack, context.bot, DOWNLOAD_DIR)
        if zp and count > 0:
            with open(zp, "rb") as f:
                await update.message.reply_document(document=f, filename=f"{pack}.zip",
                                                     caption=t(context, "sticker_done", n=count))
            zp.unlink(missing_ok=True)
        else:
            await update.message.reply_text(t(context, "sticker_not_found"))
        return

    # Режим поиска YouTube
    if context.user_data.get("waiting_search"):
        context.user_data["waiting_search"] = False
        status = await update.message.reply_text(t(context, "searching"))
        results = await youtube_search(text)
        if not results:
            await status.edit_text(t(context, "search_no_results"))
            return
        rows = []
        for i, r in enumerate(results[:5]):
            title = r.get("title", "?")[:50]
            dur = format_duration(r.get("duration", 0))
            rows.append([InlineKeyboardButton(f"{title} [{dur}]", callback_data=f"search_pick_{i}")])
        context.user_data["search_results"] = results[:5]
        await status.edit_text(t(context, "search_results"), reply_markup=InlineKeyboardMarkup(rows))
        return

    # Режим объединения
    if context.user_data.get("waiting_merge"):
        msg = "📎 Отправь видео как файл." if lang == "ru" else "📎 Send video as a file."
        await update.message.reply_text(msg)
        return

    # Кнопка Меню
    if text in ("🎛 Меню", "🎛 Menu"):
        lang = get_lang(context)
        track_user(user.id, lang)
        await send_menu_photo(
            update.message, t(context, "menu_title"),
            main_menu_keyboard(user.id == ADMIN_ID, lang), context,
        )
        return

    # Ввод времени обрезки
    if context.user_data.get("waiting_trim"):
        if context.user_data.get("trim_start") is None:
            if not time_str_valid(text):
                await update.message.reply_text(t(context, "trim_invalid"))
                return
            context.user_data["trim_start"] = text
            await update.message.reply_text(t(context, "trim_enter_end"))
        else:
            if not time_str_valid(text):
                await update.message.reply_text(t(context, "trim_invalid"))
                return
            context.user_data["trim_end"] = text
            context.user_data["waiting_trim"] = False
            fmt = context.user_data.get("format", "video")
            if fmt == "gif":
                speed = context.user_data.get("speed", "1.0")
                await update.message.reply_text(
                    t(context, "gif_menu"),
                    reply_markup=gif_menu_keyboard(speed, lang),
                )
            elif fmt == "circle":
                speed = context.user_data.get("speed", "1.0")
                audio = context.user_data.get("audio", "normal")
                await update.message.reply_text(
                    t(context, "circle_menu"),
                    reply_markup=circle_menu_keyboard(speed, audio, lang),
                )
            elif fmt == "shakal":
                speed = context.user_data.get("speed", "1.0")
                bass = context.user_data.get("bass_boost", False)
                await update.message.reply_text(
                    t(context, "shakal_menu"),
                    reply_markup=shakal_menu_keyboard(speed, bass, lang),
                )
            else:
                subs_on = context.user_data.get("subtitles", False)
                speed = context.user_data.get("speed", "1.0")
                await update.message.reply_text(
                    t(context, "step_orient"),
                    reply_markup=orientation_keyboard(subs_on, speed, context.user_data.get("bass_boost", False), lang),
                )
        return

    # Админ блокировка
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
                Storage.save(data)
            await update.message.reply_text(f"✅ {uid} заблокирован.")
        else:
            if uid in blocked:
                blocked.remove(uid)
                Storage.save(data)
            await update.message.reply_text(f"✅ {uid} разблокирован.")
        return

    # Обычный режим — ссылка
    context.user_data["_uid"] = str(user.id)
    track_user(user.id, lang)

    if is_blocked(user.id):
        await update.message.reply_text(t(context, "blocked"))
        return

    allowed, remaining = check_limit(user.id)
    if not allowed:
        await update.message.reply_text(t(context, "limit", limit=DAILY_LIMIT))
        return

    # Стикерпак
    sticker_match = re.search(r"(?:https?://)?t\.me/addstickers/([A-Za-z0-9_]+)", text)
    if sticker_match:
        pack = sticker_match.group(1)
        await update.message.reply_text(t(context, "sticker_downloading"))
        zp, count = await download_sticker_pack(pack, context.bot, DOWNLOAD_DIR)
        if zp and count > 0:
            with open(zp, "rb") as f:
                await update.message.reply_document(document=f, filename=f"{pack}.zip",
                                                     caption=t(context, "sticker_done", n=count))
            zp.unlink(missing_ok=True)
        else:
            await update.message.reply_text(t(context, "sticker_not_found"))
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

    # Спецобработка платформ
    if re.search(SPOTIFY_PATTERN, url, re.IGNORECASE):
        await update.message.reply_text(t(context, "spotify_not_supported"))
        return

    # Яндекс Музыка — пробуем скачать (может быть геоблок)
    if re.search(YANDEX_PATTERN, url, re.IGNORECASE):
        await update.message.reply_text(
            "🎵 Яндекс Музыка\n⚠️ Возможен геоблок — попробую скачать!" if lang == "ru"
            else "🎵 Yandex Music\n⚠️ Geo-restrictions possible — will try!"
        )

    init_download_context(context, url, platform)
    get_merged_history(user.id, context)

    await update.message.reply_text(
        f"🎬 {platform}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard(lang, context.user_data.get("default_format", ""), url),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CALLBACKS
# ═══════════════════════════════════════════════════════════════════════════════

async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                reply_markup=main_menu_keyboard(is_admin, lang),
            )
        except Exception:
            await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin, lang))

    elif action == "merge":
        context.user_data["merge_files"] = []
        context.user_data["waiting_merge"] = True
        await safe_edit(query, t(context, "merge_start"), reply_markup=merge_keyboard(lang))

    elif action == "search":
        context.user_data["waiting_search"] = True
        await safe_edit(query, t(context, "search_enter"))

    elif action == "sticker":
        context.user_data["waiting_sticker"] = True
        await safe_edit(query, t(context, "sticker_enter"))

    elif action == "settings":
        theme = context.user_data.get("theme", "light")
        def_fmt = context.user_data.get("default_format", "video")
        def_q = context.user_data.get("default_quality", "best")
        T = TEXTS.get(lang, TEXTS["ru"])
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        fmt_labels = {"video": "🎬", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕", "shakal": "🗿"}
        q_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Max"}
        await safe_edit(query,
                        T["settings_info"].format(theme=theme_label, fmt=fmt_labels.get(def_fmt, def_fmt), quality=q_labels.get(def_q, def_q)),
                        reply_markup=settings_keyboard(theme, def_fmt, def_q, lang))

    elif action == "download":
        await safe_edit(query, t(context, "no_url"))

    elif action == "history":
        history = get_merged_history(user.id, context)
        if not history:
            await query.answer(t(context, "history_empty"), show_alert=True)
        else:
            # FIX: отправляем НОВОЕ сообщение для истории (не ломаем фото-меню)
            rows = list(history_keyboard(history).inline_keyboard)
            rows.append([InlineKeyboardButton("◀️ Закрыть", callback_data="history_close")])
            kb = InlineKeyboardMarkup(rows)
            await query.message.reply_text(t(context, "history_title"), reply_markup=kb)

    elif action == "me":
        data = get_data()
        uid = str(user.id)
        total = data.get("stats", {}).get("users", {}).get(uid, 0)
        if total == 0:
            text = t(context, "me_empty")
        else:
            up = data.get("user_platforms", {}).get(uid, {})
            fav = max(up.items(), key=lambda x: x[1])[0] if up else "—"
            today = data.get("downloads_today", {}).get(uid, 0)
            text = t(context, "me", total=total, fav=fav, today=today, limit=DAILY_LIMIT)
        await safe_edit(query, text, reply_markup=back_keyboard(lang))

    elif action == "patchnote":
        notes = PATCH_NOTES.get(BOT_VERSION)
        text = notes.get(lang, notes.get("ru", "")) if notes else f"📋 v{BOT_VERSION}"
        await safe_edit(query, text, reply_markup=patchnote_keyboard(BOT_VERSION))

    elif action == "help":
        await safe_edit(query, t(context, "help"), reply_markup=back_keyboard(lang))

    elif action == "lang":
        await safe_edit(query, "🌍 Выбери язык / Choose language:", reply_markup=lang_menu_keyboard())

    elif action == "stats" and is_admin:
        data = get_data()
        stats = data.get("stats", {})
        total = stats.get("total", 0)
        top = sorted(stats.get("platforms", {}).items(), key=lambda x: x[1], reverse=True)[:5]
        top_str = "\n".join(f"  {p}: {c}" for p, c in top) or "  —"
        await safe_edit(query,
                        f"📊 Всего: {total}\nПользователей: {len(stats.get('users', {}))}\nАктивных: {len(ACTIVE_USERS)}\n\n{top_str}",
                        reply_markup=back_keyboard(lang))

    elif action == "blocks" and is_admin:
        blocked = get_data().get("blocked", [])
        if not blocked:
            await safe_edit(query, "✅ Нет блокировок.", reply_markup=back_keyboard(lang))
        else:
            rows = [[InlineKeyboardButton(f"🔓 {uid}", callback_data=f"adm_unblock_{uid}")] for uid in blocked[:10]]
            rows.append([InlineKeyboardButton("◀️", callback_data="menu_back")])
            await safe_edit(query, f"🚫 Заблокировано: {len(blocked)}", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "sendpatch" and is_admin:
        notes = PATCH_NOTES.get(BOT_VERSION)
        if not notes or not ACTIVE_USERS:
            await safe_edit(query, "❌ Нет данных.", reply_markup=back_keyboard(lang))
            return
        sent, failed = 0, 0
        for uid, ulang in list(ACTIVE_USERS.items()):
            if uid == ADMIN_ID:
                continue
            try:
                await context.bot.send_message(uid, notes.get(ulang, notes.get("ru", "")))
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await safe_edit(query, f"✅ Отправлено: {sent}, ошибок: {failed}", reply_markup=back_keyboard(lang))


async def cb_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = query.data.replace("lang_", "")
    context.user_data["lang"] = lang
    track_user(query.from_user.id, lang)
    data = get_data()
    data.setdefault("user_langs", {})[str(query.from_user.id)] = lang
    Storage.save(data)
    is_admin = query.from_user.id == ADMIN_ID
    try:
        await query.edit_message_caption(
            caption=t(context, "menu_title"),
            reply_markup=main_menu_keyboard(is_admin, lang),
        )
    except Exception:
        await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin, lang))


async def cb_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "history_close":
        try:
            await query.delete_message()
        except Exception:
            pass
        return
    idx = int(query.data.replace("history_", ""))
    history = context.user_data.get("history", [])
    if idx >= len(history):
        await query.answer("❌ Не найдено", show_alert=True)
        return
    item = history[idx]
    allowed, remaining = check_limit(query.from_user.id)
    if not allowed:
        await query.answer(t(context, "limit", limit=DAILY_LIMIT), show_alert=True)
        return
    init_download_context(context, item["url"], item["platform"])
    lang = get_lang(context)
    # FIX: отправляем НОВОЕ сообщение вместо редактирования фото
    await query.message.reply_text(
        f"🎬 {item['platform']}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard(lang, context.user_data.get("default_format", ""), item["url"]),
    )


async def cb_format(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fmt = query.data.replace("fmt_", "")
    context.user_data["format"] = fmt
    platform = context.user_data.get("platform", "Видео")
    lang = get_lang(context)

    if fmt in ("audio", "wav", "flac"):
        # FIX: правильная обработка WAV/FLAC
        context.user_data["quality"] = "best"
        if fmt == "audio":
            context.user_data["audio_format"] = "mp3"
        else:
            context.user_data["audio_format"] = fmt
            context.user_data["format"] = "audio"  # Унифицируем в "audio"
        await safe_edit(query, f"🎵 {platform}\n\n{t(context, 'step_audio')}", reply_markup=audio_keyboard(lang))

    elif fmt == "gif":
        context.user_data.update({"quality": "480", "audio": "mute", "orientation": "original"})
        await safe_edit(query, f"🌀 {platform} • GIF\n\n{t(context, 'step_trim')}", reply_markup=trim_keyboard(lang))

    elif fmt == "sticker":
        url = context.user_data.get("pending_url", "")
        pack = url.split("/addstickers/")[-1].split("?")[0].strip()
        await safe_edit(query, t(context, "sticker_downloading"))
        zp, count = await download_sticker_pack(pack, context.bot, DOWNLOAD_DIR)
        if zp and count > 0:
            with open(zp, "rb") as f:
                await query.message.reply_document(document=f, filename=f"{pack}.zip",
                                                    caption=t(context, "sticker_done", n=count))
            zp.unlink(missing_ok=True)
        else:
            await safe_edit(query, t(context, "sticker_not_found"))

    elif fmt == "circle":
        context.user_data.update({"quality": "480", "audio": "normal", "orientation": "original"})
        await safe_edit(query, f"⭕ {platform}\n\n{t(context, 'step_trim_gif')}", reply_markup=trim_keyboard(lang))

    elif fmt == "shakal":
        context.user_data.update({"quality": "360", "audio": "normal", "orientation": "original"})
        await safe_edit(query, f"🗿 {platform} • Шакал\n\n{t(context, 'step_trim')}", reply_markup=trim_keyboard(lang))

    elif fmt == "thumb":
        await safe_edit(query, "⏳ Скачиваю обложку...", reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)

    elif fmt == "playlist":
        context.user_data.update({"audio": "normal", "orientation": "original"})
        await safe_edit(query, f"📋 {platform}\n\n{t(context, 'step_quality')}",
                        reply_markup=quality_keyboard(lang, context.user_data.get("default_quality", "")))

    else:  # video
        await safe_edit(query, f"🎬 {platform}\n\n{t(context, 'step_quality')}",
                        reply_markup=quality_keyboard(lang, context.user_data.get("default_quality", "")))


async def cb_quality(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    quality = query.data.replace("quality_", "")
    context.user_data["quality"] = quality
    fmt = context.user_data.get("format", "video")
    platform = context.user_data.get("platform", "Видео")
    lang = get_lang(context)

    if fmt == "playlist":
        await safe_edit(query, "⏳ Скачиваю плейлист...", reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)
        return

    ql = QUALITY_LABELS.get(quality, quality)
    await safe_edit(query, f"🎬 {platform} • {ql}\n\n{t(context, 'step_audio')}", reply_markup=audio_keyboard(lang))


async def cb_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    audio = query.data.replace("audio_", "")
    context.user_data["audio"] = audio
    fmt = context.user_data.get("format", "video")
    lang = get_lang(context)

    if fmt == "audio":
        context.user_data["orientation"] = "original"
        await show_preview_or_download(query, context)
        return

    # Возврат из меню кружочка
    if context.user_data.pop("circle_audio_return", False):
        speed = context.user_data.get("speed", "1.0")
        await safe_edit(query, t(context, "circle_menu"),
                        reply_markup=circle_menu_keyboard(speed, audio, lang))
        return

    subs_on = context.user_data.get("subtitles", False)
    speed = context.user_data.get("speed", "1.0")
    await safe_edit(query, t(context, "step_orient"),
                    reply_markup=orientation_keyboard(subs_on, speed, context.user_data.get("bass_boost", False), lang))


async def cb_speed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    speed = query.data.replace("speed_", "")
    context.user_data["speed"] = speed
    lang = get_lang(context)

    if context.user_data.pop("circle_speed_return", False):
        audio = context.user_data.get("audio", "normal")
        await safe_edit(query, t(context, "circle_menu"),
                        reply_markup=circle_menu_keyboard(speed, audio, lang))
        return

    if context.user_data.pop("gif_speed_return", False):
        await safe_edit(query, t(context, "gif_menu"),
                        reply_markup=gif_menu_keyboard(speed, lang))
        return

    if context.user_data.pop("shakal_speed_return", False):
        bass = context.user_data.get("bass_boost", False)
        await safe_edit(query, t(context, "shakal_menu"),
                        reply_markup=shakal_menu_keyboard(speed, bass, lang))
        return

    subs_on = context.user_data.get("subtitles", False)
    await safe_edit(query, t(context, "step_orient"),
                    reply_markup=orientation_keyboard(subs_on, speed, context.user_data.get("bass_boost", False), lang))


async def cb_orientation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    lang = get_lang(context)
    subs_on = context.user_data.get("subtitles", False)
    speed = context.user_data.get("speed", "1.0")

    if data == "orient_toggle_subs":
        context.user_data["subtitles"] = not subs_on
        bass = context.user_data.get("bass_boost", False)
        await safe_edit(query, t(context, "step_orient"),
                        reply_markup=orientation_keyboard(not subs_on, speed, bass, lang))
    elif data == "orient_bass":
        context.user_data["bass_boost"] = not context.user_data.get("bass_boost", False)
        bass = context.user_data["bass_boost"]
        await safe_edit(query, t(context, "step_orient"),
                        reply_markup=orientation_keyboard(subs_on, speed, bass, lang))
    elif data == "orient_speed":
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(lang))
    elif data == "orient_trim":
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, t(context, "trim_enter_start"))
    elif data == "orient_download":
        context.user_data.setdefault("orientation", "original")
        await show_preview_or_download(query, context)
    else:
        orient = data.replace("orient_", "")
        context.user_data["orientation"] = orient
        await safe_edit(query, t(context, "step_orient"),
                        reply_markup=orientation_keyboard(subs_on, speed, context.user_data.get("bass_boost", False), lang))


async def cb_circle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "circle_speed":
        context.user_data["circle_speed_return"] = True
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(lang))
    elif query.data == "circle_audio":
        context.user_data["circle_audio_return"] = True
        await safe_edit(query, t(context, "step_audio"), reply_markup=audio_keyboard(lang))
    elif query.data == "circle_download":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)


async def cb_gif_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "gif_speed":
        context.user_data["gif_speed_return"] = True
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(lang))
    elif query.data == "gif_download":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)


async def cb_shakal_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "shakal_speed":
        context.user_data["shakal_speed_return"] = True
        await safe_edit(query, t(context, "step_speed"), reply_markup=speed_keyboard(lang))
    elif query.data == "shakal_bass":
        context.user_data["bass_boost"] = not context.user_data.get("bass_boost", False)
        speed = context.user_data.get("speed", "1.0")
        bass = context.user_data["bass_boost"]
        await safe_edit(query, t(context, "shakal_menu"),
                        reply_markup=shakal_menu_keyboard(speed, bass, lang))
    elif query.data == "shakal_download":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)


async def cb_trim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fmt = context.user_data.get("format", "video")
    lang = get_lang(context)

    if query.data == "trim_back":
        # Возвращаемся к выбору формата
        platform = context.user_data.get("platform", "Видео")
        url = context.user_data.get("pending_url", "")
        _, remaining = check_limit(query.from_user.id)
        await safe_edit(
            query,
            f"🎬 {platform}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
            reply_markup=format_keyboard(lang, context.user_data.get("default_format", ""), url),
        )
    elif query.data == "trim_no":
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        if fmt == "circle":
            speed = context.user_data.get("speed", "1.0")
            audio = context.user_data.get("audio", "normal")
            await safe_edit(query, t(context, "circle_menu"),
                            reply_markup=circle_menu_keyboard(speed, audio, lang))
        elif fmt == "gif":
            speed = context.user_data.get("speed", "1.0")
            await safe_edit(query, t(context, "gif_menu"),
                            reply_markup=gif_menu_keyboard(speed, lang))
        elif fmt == "shakal":
            speed = context.user_data.get("speed", "1.0")
            bass = context.user_data.get("bass_boost", False)
            await safe_edit(query, t(context, "shakal_menu"),
                            reply_markup=shakal_menu_keyboard(speed, bass, lang))
        else:
            await show_preview_or_download(query, context)
    else:
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, t(context, "trim_enter_start"))


async def cb_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "preview_confirm":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, query.message, context)
    elif query.data == "preview_cancel":
        await safe_edit(query, t(context, "preview_cancelled"))


async def cb_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Отменяю...")
    flag = context.user_data.get("cancel_flag", {})
    flag["cancelled"] = True
    await safe_edit(query, "❌ Отменено.")


async def cb_download_again(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)
    url = context.user_data.get("pending_url")
    if not url:
        await query.answer("❌ Ссылка устарела", show_alert=True)
        return
    status_msg = await query.message.reply_text(
        f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
        reply_markup=cancel_keyboard(lang),
    )
    # Сбрасываем cancel_flag
    context.user_data["cancel_flag"] = {"cancelled": False}
    await _run_download(query.from_user, status_msg, context)


async def cb_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.replace("search_pick_", ""))
    results = context.user_data.get("search_results", [])
    if idx >= len(results):
        await safe_edit(query, "❌")
        return
    item = results[idx]
    url = item.get("url") or item.get("webpage_url") or f"https://www.youtube.com/watch?v={item.get('id', '')}"
    if not url.startswith("http"):
        url = f"https://www.youtube.com/watch?v={item.get('id', '')}"

    init_download_context(context, url, "YouTube")
    lang = get_lang(context)
    allowed, remaining = check_limit(query.from_user.id)
    if not allowed:
        await safe_edit(query, t(context, "limit", limit=DAILY_LIMIT))
        return
    await safe_edit(
        query,
        f"🎬 YouTube\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard(lang, context.user_data.get("default_format", ""), url),
    )


async def cb_merge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "merge_do":
        files = context.user_data.get("merge_files", [])
        if len(files) < 2:
            await query.answer(t(context, "merge_need_two"), show_alert=True)
            return
        context.user_data["waiting_merge"] = False
        await safe_edit(query, t(context, "merge_processing"))
        paths = []
        for i, entry in enumerate(files):
            try:
                fname = DOWNLOAD_DIR / f"merge_{query.from_user.id}_{i}.mp4"
                tg_file = await context.bot.get_file(entry["file_id"])
                await tg_file.download_to_drive(fname)
                paths.append(fname)
            except Exception as e:
                logger.error("merge download: %s", e)
                await safe_edit(query, f"❌ Ошибка файла {i + 1}")
                return
        output = DOWNLOAD_DIR / f"merged_{query.from_user.id}.mp4"
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(None, merge_videos, paths, output)
        if ok and output.exists():
            with open(output, "rb") as f:
                await query.message.reply_video(video=f, caption=t(context, "merge_done"))
            output.unlink(missing_ok=True)
        else:
            await safe_edit(query, "❌ Ошибка объединения.")
        for p in paths:
            p.unlink(missing_ok=True)
        context.user_data["merge_files"] = []

    elif query.data == "merge_cancel":
        context.user_data["waiting_merge"] = False
        context.user_data["merge_files"] = []
        await safe_edit(query, t(context, "preview_cancelled"))


async def cb_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    lang = get_lang(context)
    T = TEXTS.get(lang, TEXTS["ru"])

    if data == "settings_theme":
        current = context.user_data.get("theme", "light")
        new = "dark" if current == "light" else "light"
        context.user_data["theme"] = new
        _save_user_prefs(query.from_user.id, context)
        # Пересоздаём сообщение с новым фото
        try:
            await query.message.delete()
        except Exception:
            pass
        photo_path = _menu_photo_path(new, lang)
        cache_key = f"{new}_{lang}"
        cached = _PHOTO_CACHE.get(cache_key)
        theme_label = T["theme_dark"] if new == "dark" else T["theme_light"]
        def_fmt = context.user_data.get("default_format", "video")
        def_q = context.user_data.get("default_quality", "best")
        fmt_labels = {"video": "🎬", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕", "shakal": "🗿"}
        q_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Max"}
        text = T["settings_info"].format(theme=theme_label, fmt=fmt_labels.get(def_fmt, def_fmt), quality=q_labels.get(def_q, def_q))
        try:
            if cached:
                src = cached
            elif photo_path.exists():
                src = open(photo_path, "rb")
            else:
                await context.bot.send_message(query.message.chat_id, text=text,
                                               reply_markup=settings_keyboard(new, def_fmt, def_q, lang))
                return
            msg = await context.bot.send_photo(
                query.message.chat_id, photo=src, caption=text,
                reply_markup=settings_keyboard(new, def_fmt, def_q, lang),
            )
            if cache_key not in _PHOTO_CACHE and msg.photo:
                _PHOTO_CACHE[cache_key] = msg.photo[-1].file_id
        except Exception:
            await context.bot.send_message(query.message.chat_id, text=text,
                                           reply_markup=settings_keyboard(new, def_fmt, def_q, lang))

    elif data == "settings_fmt":
        await safe_edit(query, T["default_fmt_title"],
                        reply_markup=settings_fmt_keyboard(context.user_data.get("default_format", "video"), lang))

    elif data.startswith("setfmt_"):
        fmt = data.replace("setfmt_", "")
        context.user_data["default_format"] = fmt
        _save_user_prefs(query.from_user.id, context)
        # Возвращаемся в настройки
        theme = context.user_data.get("theme", "light")
        def_q = context.user_data.get("default_quality", "best")
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        fmt_labels = {"video": "🎬", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕", "shakal": "🗿"}
        q_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Max"}
        await safe_edit(query,
                        T["settings_info"].format(theme=theme_label, fmt=fmt_labels.get(fmt, fmt), quality=q_labels.get(def_q, def_q)),
                        reply_markup=settings_keyboard(theme, fmt, def_q, lang))

    elif data == "settings_quality":
        await safe_edit(query, T["default_fmt_title"],
                        reply_markup=settings_quality_keyboard(context.user_data.get("default_quality", "best"), lang))

    elif data.startswith("setquality_"):
        quality = data.replace("setquality_", "")
        context.user_data["default_quality"] = quality
        _save_user_prefs(query.from_user.id, context)
        theme = context.user_data.get("theme", "light")
        def_fmt = context.user_data.get("default_format", "video")
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        fmt_labels = {"video": "🎬", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕", "shakal": "🗿"}
        q_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Max"}
        await safe_edit(query,
                        T["settings_info"].format(theme=theme_label, fmt=fmt_labels.get(def_fmt, def_fmt), quality=q_labels.get(quality, quality)),
                        reply_markup=settings_keyboard(theme, def_fmt, quality, lang))

    elif data == "settings_back":
        is_admin = query.from_user.id == ADMIN_ID
        try:
            await query.edit_message_caption(
                caption=t(context, "menu_title"),
                reply_markup=main_menu_keyboard(is_admin, lang),
            )
        except Exception:
            await safe_edit(query, t(context, "menu_title"), reply_markup=main_menu_keyboard(is_admin, lang))


async def cb_patch_nav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    version = query.data.replace("patch_nav_", "")
    lang = get_lang(context)
    notes = PATCH_NOTES.get(version)
    text = notes.get(lang, notes.get("ru", "")) if notes else f"📋 v{version}"
    await safe_edit(query, text, reply_markup=patchnote_keyboard(version))


async def cb_adm_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        return
    uid = int(query.data.replace("adm_unblock_", ""))
    data = get_data()
    blocked = data.get("blocked", [])
    if uid in blocked:
        blocked.remove(uid)
        Storage.save(data)
    await safe_edit(query, f"✅ {uid} разблокирован.", reply_markup=back_keyboard(get_lang(context)))


# ═══════════════════════════════════════════════════════════════════════════════
# ПРЕВЬЮ И СКАЧИВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

async def show_preview_or_download(query, context):
    """Показывает превью или сразу скачивает."""
    lang = get_lang(context)
    url = context.user_data.get("pending_url", "")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    status = await query.message.reply_text(t(context, "preview_loading"))
    info = await fetch_video_info(url)
    if info:
        title = info.get("title", "?")[:60]
        dur = format_duration(info.get("duration", 0))
        uploader = info.get("uploader") or info.get("channel") or ""
        views = info.get("view_count")
        lines = [f"🎬 {title}", f"⏱ {dur}"]
        if uploader:
            lines.append(f"👤 {uploader}")
        if views:
            lines.append(f"👁 {views:,}".replace(",", " "))
        lines.append("\nСкачать?" if lang == "ru" else "\nDownload?")
        await status.edit_text("\n".join(lines), reply_markup=preview_keyboard(lang))
    else:
        await status.edit_text(f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                               reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, status, context)


async def _run_download(user, status_msg, context):
    """Запускает скачивание с очередью."""
    user_id = user.id
    if user_id not in DOWNLOAD_LOCKS:
        DOWNLOAD_LOCKS[user_id] = asyncio.Lock()

    lock = DOWNLOAD_LOCKS[user_id]
    if lock.locked():
        pos = DOWNLOAD_QUEUE.qsize() + 1
        try:
            await status_msg.edit_text(t(context, "queued", pos=pos))
        except Exception:
            pass
        # FIX: копируем user_data чтобы избежать race condition
        user_data_copy = {
            k: v for k, v in context.user_data.items()
            if isinstance(v, (str, int, float, bool, list, dict, type(None)))
        }
        await DOWNLOAD_QUEUE.put((user, status_msg, user_data_copy))
        return

    async with lock:
        await _do_download(user, status_msg, context)

    # Обработка очереди
    if not DOWNLOAD_QUEUE.empty():
        try:
            q_user, q_msg, q_data = await asyncio.wait_for(DOWNLOAD_QUEUE.get(), timeout=1)
            # FIX: используем копию данных напрямую, не обновляя context
            class FakeContext:
                def __init__(self, data):
                    self.user_data = data
            fake_ctx = FakeContext(q_data)
            await _do_download(q_user, q_msg, fake_ctx)
        except asyncio.TimeoutError:
            pass


async def _do_download(user, status_msg, context):
    """Основная логика скачивания."""
    url       = context.user_data.get("pending_url")
    quality   = context.user_data.get("quality", "best")
    fmt       = context.user_data.get("format", "video")
    audio     = context.user_data.get("audio", "normal")
    orient    = context.user_data.get("orientation", "original")
    trim_s    = context.user_data.get("trim_start")
    trim_e    = context.user_data.get("trim_end")
    subtitles = context.user_data.get("subtitles", False)
    platform  = context.user_data.get("platform", "Видео")
    speed_str = context.user_data.get("speed", "1.0")
    cancel_flag = context.user_data.get("cancel_flag", {"cancelled": False})
    audio_codec = context.user_data.get("audio_format", "mp3")
    volume, _ = AUDIO_OPTIONS.get(audio, (1.0, "🔊"))
    speed = float(speed_str)
    ql = QUALITY_LABELS.get(quality, quality)
    lang = context.user_data.get("lang", "ru")
    files_to_clean = []

    if not url:
        await status_msg.edit_text("❌ Ссылка устарела.")
        return

    try:
        # Обложка
        if fmt == "thumb":
            await status_msg.edit_text("🖼 Скачиваю обложку...")
            thumb = await download_thumbnail(url, DOWNLOAD_DIR)
            if not thumb or not thumb.exists():
                await status_msg.edit_text("❌ Не удалось.")
                return
            files_to_clean.append(thumb)
            with open(thumb, "rb") as f:
                await status_msg.reply_photo(photo=f, caption=f"🖼 {platform}")
            await status_msg.delete()
            update_stats(user.id, platform)
            increment_limit(user.id)
            add_to_history(context, url, platform)
            return

        # Плейлист
        if fmt == "playlist":
            zp = await download_playlist(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag, lang)
            if cancel_flag.get("cancelled") or not zp or not zp.exists():
                await status_msg.edit_text("❌ Не удалось.")
                return
            await status_msg.edit_text("📤 Отправляю...")
            with open(zp, "rb") as f:
                await status_msg.reply_document(document=f, caption=f"✅ Плейлист • {ql}")
            await status_msg.delete()
            zp.unlink(missing_ok=True)
            update_stats(user.id, platform)
            increment_limit(user.id)
            add_to_history(context, url, platform)
            await _notify_admin(user, platform, fmt, context)
            return

        # Видео/аудио/GIF/кружочек
        file_path = await download_video(
            url, quality, DOWNLOAD_DIR, status_msg, cancel_flag,
            fmt, lang, audio_codec,
        )
        if cancel_flag.get("cancelled"):
            await status_msg.edit_text("❌ Отменено.")
            return
        if not file_path or not file_path.exists():
            # Instagram — специальное сообщение
            if cancel_flag.get("_ig_login_required"):
                ig_msg = (
                    "❌ Instagram требует авторизацию.\n\n"
                    "Instagram заблокировал анонимный доступ.\n"
                    "Для работы нужен файл cookies.txt:\n\n"
                    "1️⃣ Открой Instagram в Chrome\n"
                    "2️⃣ Установи расширение «Get cookies.txt LOCALLY»\n"
                    "3️⃣ Экспортируй куки для instagram.com\n"
                    "4️⃣ Положи файл cookies.txt в папку бота\n"
                    "5️⃣ Перезапусти бота"
                )
                await status_msg.edit_text(ig_msg)
            else:
                await status_msg.edit_text("❌ Не удалось скачать.")
            return

        files_to_clean.append(file_path)
        current = file_path

        # Обрезка
        if trim_s and trim_e and fmt != "audio":
            current = await asyncio.get_event_loop().run_in_executor(None, apply_trim, current, trim_s, trim_e)
            if current != file_path:
                files_to_clean.append(current)

        # GIF
        if fmt == "gif":
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            out_gif = current.with_stem(current.stem + "_gif").with_suffix(".gif")

            if speed != 1.0 and ffmpeg_ok():
                # Одна команда: ускорение + конвертация в GIF через filter_complex
                vf = f"setpts={1/speed}*PTS,fps=15,scale=480:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse"
                cmd = [
                    "ffmpeg", "-y", "-i", str(current),
                    "-filter_complex", vf,
                    "-loop", "0", str(out_gif),
                ]
                logger.warning("GIF speed+convert cmd: %s", " ".join(cmd))
                est_dur = dur / speed
                ok = await asyncio.get_event_loop().run_in_executor(None, ffmpeg_run, cmd)
                if not ok or not out_gif.exists() or out_gif.stat().st_size == 0:
                    # Фоллбэк: простая конвертация без палитры
                    logger.warning("GIF filter_complex failed, trying simple")
                    out_gif.unlink(missing_ok=True)
                    cmd_simple = [
                        "ffmpeg", "-y", "-i", str(current),
                        "-vf", f"setpts={1/speed}*PTS,fps=12,scale=320:-1",
                        "-loop", "0", str(out_gif),
                    ]
                    await asyncio.get_event_loop().run_in_executor(None, ffmpeg_run, cmd_simple)
            else:
                # Без ускорения — обычная конвертация
                cmd = [
                    "ffmpeg", "-y", "-i", str(current),
                    "-vf", "fps=15,scale=480:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
                    "-loop", "0", str(out_gif),
                ]
                logger.warning("GIF convert cmd: %s", " ".join(cmd))
                ok = await asyncio.get_event_loop().run_in_executor(None, ffmpeg_run, cmd)
                if not ok or not out_gif.exists() or out_gif.stat().st_size == 0:
                    # Фоллбэк: простая конвертация без палитры
                    logger.warning("GIF palettegen failed, trying simple")
                    out_gif.unlink(missing_ok=True)
                    cmd_simple = [
                        "ffmpeg", "-y", "-i", str(current),
                        "-vf", "fps=12,scale=320:-1",
                        "-loop", "0", str(out_gif),
                    ]
                    await asyncio.get_event_loop().run_in_executor(None, ffmpeg_run, cmd_simple)

            if out_gif.exists() and out_gif.stat().st_size > 0:
                current = out_gif
                files_to_clean.append(current)
            else:
                logger.error("GIF конвертация полностью провалена, отправляем как видео")
                fmt = "video"

        # Кружочек
        elif fmt == "circle":
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            out_circle = current.with_stem(current.stem + "_circle").with_suffix(".mp4")
            # FIX: правильное экранирование для ffmpeg
            cmd = [
                "ffmpeg", "-y", "-i", str(current),
                "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=384:384",
                "-t", "60", "-c:v", "libx264", "-preset", "fast",
                "-c:a", "aac", "-b:a", "128k", str(out_circle),
            ]
            await ffmpeg_with_progress(cmd, status_msg, "⭕ Кружочек...", dur)
            if out_circle.exists():
                current = out_circle
                files_to_clean.append(current)

        # Шакал
        elif fmt == "shakal":
            logger.warning("Шакал: speed=%s, bass=%s, ffmpeg_ok=%s", speed, context.user_data.get("bass_boost", False), ffmpeg_ok())
            if ffmpeg_ok():
                label = f"🗿 Шакалим{' ⚡' + str(speed) + 'x' if speed != 1.0 else ''}..."
                await status_msg.edit_text(label)
                new = await asyncio.get_event_loop().run_in_executor(None, apply_shakal, current, speed)
                if new != current:
                    files_to_clean.append(new)
                    current = new
                else:
                    logger.error("Шакал: ffmpeg вернул оригинал — обработка не удалась")
            else:
                logger.error("Шакал: ffmpeg не найден!")

        # Ориентация
        if fmt == "video" and orient != "original":
            current = await asyncio.get_event_loop().run_in_executor(None, apply_orientation, current, orient)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Субтитры
        subs_warning = None
        if subtitles and fmt == "video":
            await status_msg.edit_text("📝 Субтитры...")
            new, subs_warning = await _add_subtitles(url, current, platform)
            if new != current:
                files_to_clean.append(new)
                current = new

        # Скорость
        if speed != 1.0 and fmt in ("video", "circle"):
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            out_speed = current.with_stem(current.stem + "_speed")
            # FIX: корректная цепочка atempo
            atempo_parts = []
            remaining_speed = speed
            while remaining_speed > 2.0:
                atempo_parts.append("atempo=2.0")
                remaining_speed /= 2.0
            while remaining_speed < 0.5:
                atempo_parts.append("atempo=0.5")
                remaining_speed /= 0.5
            atempo_parts.append(f"atempo={remaining_speed}")
            atempo_filter = ",".join(atempo_parts)
            cmd = [
                "ffmpeg", "-y", "-i", str(current),
                "-vf", f"setpts={1/speed}*PTS",
                "-af", atempo_filter,
                "-c:v", "libx264", "-preset", "fast",
                str(out_speed),
            ]
            await ffmpeg_with_progress(cmd, status_msg, f"⚡ {speed}x...", dur / speed)
            if out_speed.exists():
                current = out_speed
                files_to_clean.append(current)

        # Громкость
        if fmt == "video" and volume != 1.0:
            current = await asyncio.get_event_loop().run_in_executor(None, apply_audio, current, volume)
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Басс-буст
        bass_boost = context.user_data.get("bass_boost", False)
        logger.warning("Bass boost: %s, fmt: %s, ffmpeg: %s", bass_boost, fmt, ffmpeg_ok())
        if bass_boost and fmt in ("video", "shakal") and ffmpeg_ok():
            await status_msg.edit_text("🔊 Басс-буст...")
            new = await asyncio.get_event_loop().run_in_executor(None, apply_bass_boost, current)
            if new != current:
                files_to_clean.append(new)
                current = new

        # Проверка размера
        file_size = current.stat().st_size
        if file_size > MAX_FILE_MB * 1024 * 1024:
            size_mb = file_size / 1024 / 1024
            if fmt in ("video", "circle") and ffmpeg_ok():
                await status_msg.edit_text(f"📦 {size_mb:.0f} МБ → сжимаю...")
                compressed = await asyncio.get_event_loop().run_in_executor(
                    None, compress_video, current, float(MAX_FILE_MB - 3),
                )
                if compressed != current:
                    files_to_clean.append(compressed)
                    current = compressed
                if current.stat().st_size > MAX_FILE_MB * 1024 * 1024:
                    await status_msg.edit_text(f"❌ Не удалось сжать до {MAX_FILE_MB} МБ.")
                    return
            else:
                await status_msg.edit_text(f"❌ Файл {size_mb:.0f} МБ > {MAX_FILE_MB} МБ.")
                return

        await status_msg.edit_text("📤 Отправляю...")

        # Caption
        codec_label = context.user_data.get("audio_format", "mp3").upper()
        if fmt == "audio":
            caption = f"🎵 {platform} • {codec_label}"
        elif fmt == "gif":
            caption = f"🌀 {platform} • GIF"
        elif fmt == "circle":
            caption = f"⭕ {platform}"
        elif fmt == "shakal":
            caption = f"🗿 {platform} • Шакал"
        else:
            caption = f"✅ {platform} • {ql}"
        if speed != 1.0:
            caption += f" • {speed}x"
        if bass_boost:
            caption += " • 🔊 Bass"
        if subs_warning:
            caption += f"\n{subs_warning}"

        # Кнопка «Ещё раз»
        again_label = "🔄 Ещё раз" if lang == "ru" else "🔄 Again"
        again_kb = InlineKeyboardMarkup([[InlineKeyboardButton(again_label, callback_data="download_again")]])

        with open(current, "rb") as f:
            if fmt == "audio":
                await status_msg.reply_audio(audio=f, caption=caption, reply_markup=again_kb)
            elif fmt == "gif":
                await status_msg.reply_animation(animation=f, caption=caption, reply_markup=again_kb)
            elif fmt == "circle":
                await status_msg.reply_video_note(video_note=f)
            elif fmt == "shakal":
                await status_msg.reply_video(video=f, caption=caption,
                                             supports_streaming=True, reply_markup=again_kb)
            else:
                await status_msg.reply_video(video=f, caption=caption,
                                             supports_streaming=True, reply_markup=again_kb)

        await status_msg.delete()
        update_stats(user.id, platform)
        increment_limit(user.id)
        add_to_history(context, url, platform)
        await _notify_admin(user, platform, fmt, context)

    except Exception as e:
        logger.error("Download error: %s", e)
        try:
            await status_msg.edit_text("❌ Ошибка обработки.")
        except Exception:
            pass
    finally:
        for f in files_to_clean:
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИК ВИДЕО-ФАЙЛОВ (merge)
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    if not context.user_data.get("waiting_merge"):
        hint = "🔗 Нажми «Объединить» в меню." if lang == "ru" else "🔗 Press «Merge» in menu."
        await update.message.reply_text(hint)
        return
    video = update.message.video or update.message.document
    if not video:
        await update.message.reply_text("❌ Не распознал файл.")
        return
    size_mb = (video.file_size or 0) / 1024 / 1024
    if size_mb > 20:
        await update.message.reply_text(f"❌ {size_mb:.0f} МБ — макс. 20 МБ.")
        return
    files = context.user_data.setdefault("merge_files", [])
    files.append({"file_id": video.file_id, "size": size_mb})
    n = len(files)
    await update.message.reply_text(
        t(context, "merge_received", n=n),
        reply_markup=merge_keyboard(lang, count=n),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ФОНОВЫЕ ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════════════════════

async def task_limit_reset(context):
    """Сброс лимитов в полночь."""
    data = get_data()
    data["downloads_today"] = {}
    data["last_reset"] = str(date.today())
    Storage.save(data)
    for uid, lang in list(ACTIVE_USERS.items()):
        try:
            T = TEXTS.get(lang, TEXTS["ru"])
            await context.bot.send_message(uid, T["limit_reset"].format(limit=DAILY_LIMIT))
            await asyncio.sleep(0.05)
        except Exception:
            pass


async def task_ytdlp_update(context=None):
    """Обновление yt-dlp."""
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp",
                 "--break-system-packages"],
                capture_output=True, text=True, timeout=120,
            ),
        )
        if result.returncode == 0:
            logger.info("✅ yt-dlp обновлён")
    except Exception as e:
        logger.error("yt-dlp update: %s", e)


async def task_cleanup_downloads(context=None):
    """Очистка старых файлов в DOWNLOAD_DIR."""
    try:
        for f in DOWNLOAD_DIR.iterdir():
            if f.is_file():
                age = datetime.now().timestamp() - f.stat().st_mtime
                if age > 3600:  # старше 1 часа
                    f.unlink(missing_ok=True)
    except Exception as e:
        logger.warning("cleanup: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИК ОШИБОК
# ═══════════════════════════════════════════════════════════════════════════════

async def error_handler(update, context):
    import time as time_mod
    err = context.error
    if "Conflict" in str(err):
        logger.warning("Конфликт polling — второй экземпляр?")
        await asyncio.sleep(10)
        return
    logger.error("Ошибка: %s", err)
    # FIX: корректный счётчик ошибок
    _error_state["count"] += 1
    now = time_mod.time()
    if _error_state["count"] >= 5 and now - _error_state["last_alert"] > 300:
        count = _error_state["count"]
        _error_state["count"] = 0
        _error_state["last_alert"] = now
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"⚠️ {count} ошибок за 5 мин!\nПоследняя: {str(err)[:200]}",
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    global app_ref

    Storage.init()

    app = Application.builder().token(BOT_TOKEN).build()
    app_ref = app

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("me", cmd_me))
    app.add_handler(CommandHandler("patchnote", cmd_patchnote))
    app.add_handler(CommandHandler("sendpatch", cmd_sendpatch))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("unblock", cmd_unblock))

    # Callbacks
    app.add_handler(CallbackQueryHandler(cb_menu, pattern="^menu_"))
    app.add_handler(CallbackQueryHandler(cb_adm_unblock, pattern="^adm_unblock_"))
    app.add_handler(CallbackQueryHandler(cb_lang, pattern="^lang_"))
    app.add_handler(CallbackQueryHandler(cb_history, pattern="^history_"))
    app.add_handler(CallbackQueryHandler(cb_format, pattern="^fmt_"))
    app.add_handler(CallbackQueryHandler(cb_quality, pattern="^quality_"))
    app.add_handler(CallbackQueryHandler(cb_audio, pattern="^audio_"))
    app.add_handler(CallbackQueryHandler(cb_speed, pattern="^speed_"))
    app.add_handler(CallbackQueryHandler(cb_orientation, pattern="^orient_"))
    app.add_handler(CallbackQueryHandler(cb_circle, pattern="^circle_"))
    app.add_handler(CallbackQueryHandler(cb_gif_menu, pattern="^gif_"))
    app.add_handler(CallbackQueryHandler(cb_shakal_menu, pattern="^shakal_"))
    app.add_handler(CallbackQueryHandler(cb_trim, pattern="^trim_"))
    app.add_handler(CallbackQueryHandler(cb_settings, pattern="^settings_|^setfmt_|^setquality_"))
    app.add_handler(CallbackQueryHandler(cb_patch_nav, pattern="^patch_nav_"))
    app.add_handler(CallbackQueryHandler(cb_cancel, pattern="^cancel_download"))
    app.add_handler(CallbackQueryHandler(cb_search, pattern="^search_pick_"))
    app.add_handler(CallbackQueryHandler(cb_preview, pattern="^preview_"))
    app.add_handler(CallbackQueryHandler(cb_download_again, pattern="^download_again"))
    app.add_handler(CallbackQueryHandler(cb_merge, pattern="^merge_"))

    # Видео файлы (merge)
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.MimeType("video/mp4")
        | filters.Document.MimeType("video/quicktime")
        | filters.Document.MimeType("video/x-matroska")
        | filters.Document.MimeType("video/webm"),
        handle_video_file,
    ))
    # Текст
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Ошибки
    app.add_error_handler(error_handler)

    # Фоновые задачи
    async def _post_init(application):
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await asyncio.sleep(2)

        # Сброс лимитов в полночь
        application.job_queue.run_daily(
            callback=task_limit_reset,
            time=dtime(hour=0, minute=0, second=0),
            name="limit_reset",
        )
        # Обновление yt-dlp раз в неделю
        application.job_queue.run_repeating(
            callback=lambda ctx: asyncio.ensure_future(task_ytdlp_update(ctx)),
            interval=7 * 24 * 3600,
            first=7 * 24 * 3600,
            name="ytdlp_update",
        )
        # Очистка файлов каждый час
        application.job_queue.run_repeating(
            callback=lambda ctx: asyncio.ensure_future(task_cleanup_downloads(ctx)),
            interval=3600,
            first=60,
            name="cleanup",
        )

    app.post_init = _post_init

    logger.info("🚀 Бот v%s запущен (polling)...", BOT_VERSION)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
