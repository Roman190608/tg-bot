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
BOT_VERSION  = os.environ.get("BOT_VERSION", "1.5")
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
    "1.4": {
        "ru": (
            "🆕 Обновление v1.4\n\n"
            "• 📊 Прогресс-бар при конвертации GIF, кружочка и скорости\n"
            "• 🔍 Превью видео перед скачкой — название, длина, автор\n"
            "• 🔄 Кнопка «Скачать ещё раз» под каждым файлом\n"
            "• 🔔 Уведомление о сбросе лимита в полночь\n"
            "• 🔗 Объединение нескольких видео в одно\n"
            "• ⚠️ Алерты админу при накоплении ошибок"
        ),
        "en": (
            "🆕 Update v1.4\n\n"
            "• 📊 Progress bar during GIF, circle and speed conversion\n"
            "• 🔍 Video preview before download — title, duration, author\n"
            "• 🔄 «Download again» button under every file\n"
            "• 🔔 Limit reset notification at midnight\n"
            "• 🔗 Merge multiple videos into one\n"
            "• ⚠️ Admin alerts on error accumulation"
        ),
    },
    "1.5": {
        "ru": (
            "🆕 Обновление v1.5\n\n"
            "• 🌍 Новые платформы: Pinterest, Twitch VOD, Vimeo, Dailymotion\n"
            "• 🎵 WAV и FLAC — извлечение аудио без потерь\n"
            "• 🏷 MP3 теги и обложка трека автоматически\n"
            "• 🎭 Стикерпаки Telegram → ZIP\n"
            "• 🔍 Поиск видео по YouTube прямо в боте\n"
            "• 🎶 SoundCloud — скачивание треков и плейлистов"
        ),
        "en": (
            "🆕 Update v1.5\n\n"
            "• 🌍 New platforms: Pinterest, Twitch VOD, Vimeo, Dailymotion\n"
            "• 🎵 WAV and FLAC lossless audio extraction\n"
            "• 🏷 MP3 tags and cover art automatically\n"
            "• 🎭 Telegram stickerpacks → ZIP\n"
            "• 🔍 YouTube search right inside the bot\n"
            "• 🎶 SoundCloud — tracks and playlists"
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
            "1. Отправь ссылку на видео (YouTube, TikTok, Instagram, VK, Twitter, Reddit, Pinterest, Vimeo, Dailymotion, Twitch, SoundCloud)\n"
            "2. Выбери формат: видео / MP3 / WAV / FLAC / GIF / кружочек / обложка / плейлист\n"
            "3. Выбери качество и уровень звука\n"
            "4. Ориентация, субтитры, обрезка, скорость — или сразу «Скачать»\n\n"
            "🎭 Стикерпаки: отправь ссылку t.me/addstickers/ИмяПака\n"
            "🔍 Поиск: нажми «Поиск YouTube» в меню\n"
            "🔗 Объединить видео: кнопка в меню → отправляй файлы\n\n"
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
        "unsupported": "❌ Платформа не поддерживается.\nПоддерживаются: TikTok, YouTube, Instagram, Twitter, VK, Reddit, Pinterest, Vimeo, Dailymotion, Twitch, SoundCloud\n💡 Стикерпаки: t.me/addstickers/ИмяПака",
        "step1": "📦 Шаг 1 — выбери формат:",
        "remaining": "Осталось сегодня: {remaining}",
        "fmt_video": "🎬 Видео (MP4)",
        "fmt_audio": "🎵 Аудио (MP3)",
        "fmt_wav":   "🎵 Аудио (WAV)",
        "fmt_flac":  "🎵 Аудио (FLAC)",
        "fmt_sticker": "🎭 Стикерпак (ZIP)",
        "audio_fmt_title": "🎵 Выбери формат аудио:",
        "sticker_downloading": "🎭 Скачиваю стикерпак...",
        "sticker_done": "🎭 Стикерпак готов! {n} стикеров",
        "sticker_not_found": "❌ Стикерпак не найден.",
        "search_youtube": "🔍 Поиск по YouTube:",
        "search_enter": "🔍 Введи название трека или видео для поиска по YouTube:",
        "searching": "🔍 Ищу...",
        "search_no_results": "❌ Ничего не найдено.",
        "search_results": "🔍 Результаты поиска. Выбери:",
        "spotify_not_supported": "⚠️ Spotify не поддерживается напрямую — вставь ссылку на трек и получишь MP3 через yt-dlp (YouTube Music).",
        "pinterest_hint": "📌 Pinterest: работает только для видео-пинов. Если это картинка — скачать не получится.",
        "yandex_geo_error": "❌ Яндекс Музыка недоступна с серверов бота (ошибка 451 — геоблок). Бот работает вне России и не может скачивать с Яндекс Музыки.",
        "menu_sticker": "🎭 Стикерпак",
        "sticker_enter": "🎭 Отправь ссылку на стикерпак:\nt.me/addstickers/ИмяПака",
        "yandex_not_supported": "⚠️ Яндекс Музыка: вставь прямую ссылку на трек (music.yandex.ru/album/xxx/track/xxx).",
        "search_results": "🔍 Результаты поиска:",
        "search_placeholder": "Введи запрос для поиска на YouTube:",
        "spotify_hint": "⚠️ Spotify требует spotdl. Устанавливаю...",
        "yandex_hint": "⚠️ Яндекс Музыка — скачивание треков",
        "platform_unsupported": "❌ Платформа не поддерживается для этого формата.",
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
        "preview_loading": "🔍 Получаю информацию о видео...",
        "preview_download": "⬇️ Скачать",
        "cancel_btn": "❌ Отмена",
        "preview_cancelled": "❌ Отменено.",
        "merge_start": "🎬 Режим объединения видео\n\nОтправляй видео как файл (📎 → Файл → mp4), по одному.\nКогда добавишь все — нажми «Объединить».",
        "merge_btn": "🔗 Объединить",
        "merge_cancel_btn": "❌ Отмена",
        "merge_received": "✅ Видео {n} получено. Отправь ещё или нажми «Объединить».",
        "merge_processing": "🔗 Объединяю видео...",
        "merge_done": "✅ Видео объединены!",
        "merge_need_two": "❌ Нужно минимум 2 видео.",
        "limit_reset": "🔄 Лимит скачиваний сброшен! Снова доступно {limit} скачиваний.",
        "settings": "⚙️ Настройки",
        "merge": "🔗 Объединить видео",
        "search": "🔍 Поиск YouTube",
        "sticker": "🎭 Стикерпак",
        "settings_title": "⚙️ Настройки профиля",
        "theme_toggle": "🎨 Тема: {theme}",
        "theme_light": "☀️ Светлая",
        "theme_dark":  "🌙 Тёмная",
        "theme_changed": "🎨 Тема изменена на {theme}",
        "default_fmt": "📦 Формат по умолчанию: {fmt}",
        "default_fmt_title": "📦 Выбери формат по умолчанию:",
        "default_quality": "📐 Качество по умолч.: {q}",
        "pref_saved": "✅ Настройки сохранены!",
        "settings_info": (
            "⚙️ Настройки профиля\n\n"
            "🎨 Тема: {theme}\n"
            "📦 Формат: {fmt}\n"
            "📐 Качество: {quality}"
        ),
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
        "fmt_wav":   "🎵 Audio (WAV)",
        "fmt_flac":  "🎵 Audio (FLAC)",
        "fmt_sticker": "🎭 Stickerpack (ZIP)",
        "audio_fmt_title": "🎵 Choose audio format:",
        "sticker_downloading": "🎭 Downloading stickerpack...",
        "sticker_done": "🎭 Stickerpack ready! {n} stickers",
        "sticker_not_found": "❌ Stickerpack not found.",
        "search_youtube": "🔍 YouTube Search:",
        "search_enter": "🔍 Enter a track or video title to search on YouTube:",
        "searching": "🔍 Searching...",
        "search_no_results": "❌ No results found.",
        "search_results": "🔍 Search results. Choose:",
        "spotify_not_supported": "⚠️ Spotify is not directly supported — paste a track link and get MP3 via yt-dlp (YouTube Music).",
        "pinterest_hint": "📌 Pinterest: only video pins are supported. Images cannot be downloaded.",
        "yandex_geo_error": "❌ Yandex Music is unavailable from the bot's servers (error 451 — geo-block). The bot runs outside Russia and cannot download from Yandex Music.",
        "menu_sticker": "🎭 Stickerpack",
        "sticker_enter": "🎭 Send a stickerpack link:\nt.me/addstickers/PackName",
        "yandex_not_supported": "⚠️ Yandex Music: paste a direct track link (music.yandex.ru/album/xxx/track/xxx).",
        "search_results": "🔍 Search results:",
        "search_placeholder": "Enter a YouTube search query:",
        "spotify_hint": "⚠️ Spotify requires spotdl. Installing...",
        "yandex_hint": "⚠️ Yandex Music — track download",
        "platform_unsupported": "❌ Platform not supported for this format.",
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
        "preview_loading": "🔍 Getting video info...",
        "preview_download": "⬇️ Download",
        "cancel_btn": "❌ Cancel",
        "preview_cancelled": "❌ Cancelled.",
        "merge_start": "🎬 Video merge mode\n\nSend videos as files (📎 → File → mp4), one by one.\nWhen all added — press «Merge».",
        "merge_btn": "🔗 Merge",
        "merge_cancel_btn": "❌ Cancel",
        "merge_received": "✅ Video {n} received. Send more or press «Merge».",
        "merge_processing": "🔗 Merging videos...",
        "merge_done": "✅ Videos merged!",
        "merge_need_two": "❌ Need at least 2 videos.",
        "limit_reset": "🔄 Daily limit reset! {limit} downloads available again.",
        "settings": "⚙️ Settings",
        "merge": "🔗 Merge videos",
        "search": "🔍 YouTube Search",
        "sticker": "🎭 Stickerpack",
        "settings_title": "⚙️ Profile Settings",
        "theme_toggle": "🎨 Theme: {theme}",
        "theme_light": "☀️ Light",
        "theme_dark":  "🌙 Dark",
        "theme_changed": "🎨 Theme changed to {theme}",
        "default_fmt": "📦 Default format: {fmt}",
        "default_fmt_title": "📦 Choose default format:",
        "default_quality": "📐 Default quality: {q}",
        "pref_saved": "✅ Settings saved!",
        "settings_info": (
            "⚙️ Profile Settings\n\n"
            "🎨 Theme: {theme}\n"
            "📦 Format: {fmt}\n"
            "📐 Quality: {quality}"
        ),
        "me": (
            "👤 Your statistics:\n\n"
            "📥 Total downloads: {total}\n"
            "❤️ Favourite platform: {fav}\n"
            "📅 Today: {today} of {limit}"
        ),
        "me_empty": "📭 You haven't downloaded anything yet!",
        "menu_title": "🎛 Main menu:",
        "queued": "⏳ You are in queue ({pos}). Please wait...",
        "preview_loading": "🔍 Getting video info...",
        "preview_download": "⬇️ Download",
        "cancel_btn": "❌ Cancel",
        "preview_cancelled": "❌ Cancelled.",
        "merge_start": "🎬 Video merge mode\n\nSend videos as files (📎 → File → mp4), one by one.\nWhen all added — press «Merge».",
        "merge_btn": "🔗 Merge",
        "merge_cancel_btn": "❌ Cancel",
        "merge_received": "✅ Video {n} received. Send more or press «Merge».",
        "merge_processing": "🔗 Merging videos...",
        "merge_done": "✅ Videos merged!",
        "merge_need_two": "❌ Need at least 2 videos.",
        "limit_reset": "🔄 Daily limit reset! {limit} downloads available again.",
        "settings": "⚙️ Settings",
        "merge": "🔗 Merge videos",
        "settings_title": "⚙️ Profile Settings",
        "theme_toggle": "🎨 Theme: {theme}",
        "theme_light": "☀️ Light",
        "theme_dark":  "🌙 Dark",
        "theme_changed": "🎨 Theme changed to {theme}",
        "default_fmt": "📦 Default format: {fmt}",
        "default_fmt_title": "📦 Choose default format:",
        "default_quality": "📐 Default quality: {q}",
        "pref_saved": "✅ Settings saved!",
        "settings_info": (
            "⚙️ Profile Settings\n\n"
            "🎨 Theme: {theme}\n"
            "📦 Format: {fmt}\n"
            "📐 Quality: {quality}"
        ),
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
    # Существующие
    r"tiktok\.com", r"vm\.tiktok\.com",
    r"instagram\.com", r"instagr\.am",
    r"youtube\.com/shorts", r"youtube\.com/watch", r"youtu\.be",
    r"twitter\.com", r"x\.com",
    r"vk\.com", r"clips\.twitch\.tv",
    r"reddit\.com",
    # Новые v1.5
    r"pinterest\.com", r"pin\.it",
    r"twitch\.tv/videos", r"twitch\.tv/.+/clip",
    r"soundcloud\.com",
    r"vimeo\.com",
    r"dailymotion\.com", r"dai\.ly",
    r"music\.yandex\.(ru|com)", r"music\.yandex\.kz",
    r"open\.spotify\.com",
    r"t\.me/addstickers/",
    # YouTube поиск и каналы для плейлиста
    r"youtube\.com/channel", r"youtube\.com/@", r"youtube\.com/c/",
    r"youtube\.com/playlist",
    # Стикерпаки Telegram
    r"t\.me/addstickers/",
]

# Паттерны которые требуют особой обработки
SOUNDCLOUD_PATTERN = r"soundcloud\.com"
SPOTIFY_PATTERN    = r"open\.spotify\.com"
YANDEX_PATTERN     = r"music\.yandex"
STICKER_PATTERN    = r"t\.me/addstickers/"

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
        (r"tiktok\.com",                    "TikTok"),
        (r"instagram\.com|instagr\.am",     "Instagram"),
        (r"youtube\.com|youtu\.be",         "YouTube"),
        (r"twitter\.com|x\.com",            "Twitter/X"),
        (r"vk\.com",                         "VK"),
        (r"twitch\.tv",                      "Twitch"),
        (r"reddit\.com",                     "Reddit"),
        (r"pinterest\.com|pin\.it",         "Pinterest"),
        (r"soundcloud\.com",                 "SoundCloud"),
        (r"vimeo\.com",                      "Vimeo"),
        (r"dailymotion\.com|dai\.ly",       "Dailymotion"),
        (r"open\.spotify\.com",             "Spotify"),
        (r"music\.yandex",                   "Яндекс Музыка"),
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
        "settings": "⚙️ Настройки",
        "merge": "🔗 Объединить видео",
        "search": "🔍 Поиск YouTube",
        "sticker": "🎭 Стикерпак",
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
        "settings": "⚙️ Settings",
        "merge": "🔗 Merge videos",
        "search": "🔍 YouTube Search",
        "sticker": "🎭 Stickerpack",
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
        [InlineKeyboardButton(L["settings"],  callback_data="menu_settings"),
         InlineKeyboardButton(L["merge"],     callback_data="menu_merge")],
        [InlineKeyboardButton(L["search"],    callback_data="menu_search"),
         InlineKeyboardButton(L["sticker"],   callback_data="menu_sticker")],
        [InlineKeyboardButton(L["share"],     switch_inline_query=L["share_text"])],
    ]
    if is_admin:
        rows.append([
            InlineKeyboardButton(L["stats"],     callback_data="menu_stats"),
            InlineKeyboardButton(L["blocks"],    callback_data="menu_blocks"),
        ])
        rows.append([InlineKeyboardButton(L["sendpatch"], callback_data="menu_sendpatch")])
    return InlineKeyboardMarkup(rows)

def settings_keyboard(theme: str, fmt: str, quality: str, lang: str = "ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
    fmt_labels = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF",
                  "circle": "⭕ Кружочек", "playlist": "📋 Плейлист"}
    if lang == "en":
        fmt_labels = {"video": "🎬 Video", "audio": "🎵 MP3", "gif": "🌀 GIF",
                      "circle": "⭕ Circle", "playlist": "📋 Playlist"}
    quality_labels = {"360": "360p", "480": "480p", "720": "720p", "1080": "1080p", "best": "Макс." if lang == "ru" else "Max"}
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["theme_toggle"].format(theme=theme_label),
                              callback_data="settings_theme")],
        [InlineKeyboardButton(T["default_fmt"].format(fmt=fmt_labels.get(fmt, fmt)),
                              callback_data="settings_fmt")],
        [InlineKeyboardButton(T["default_quality"].format(q=quality_labels.get(quality, quality)),
                              callback_data="settings_quality")],
        [InlineKeyboardButton(T["back_btn"], callback_data="menu_back")],
    ])

def settings_fmt_keyboard(current: str, lang: str = "ru") -> InlineKeyboardMarkup:
    """Клавиатура выбора формата по умолчанию."""
    T = TEXTS.get(lang, TEXTS["ru"])
    fmts = [
        ("video",    T["fmt_video"]),
        ("audio",    T["fmt_audio"]),
        ("gif",      T["fmt_gif"]),
        ("circle",   T["fmt_circle"]),
    ]
    rows = []
    for fid, flabel in fmts:
        mark = " ✅" if fid == current else ""
        rows.append([InlineKeyboardButton(flabel + mark, callback_data=f"setfmt_{fid}")])
    rows.append([InlineKeyboardButton(T["back_btn"], callback_data="settings_back")])
    return InlineKeyboardMarkup(rows)

def settings_quality_keyboard(current: str, lang: str = "ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    qs = [("360", T["q360"]), ("480", T["q480"]), ("720", T["q720"]),
          ("1080", T["q1080"]), ("best", T["qbest"])]
    rows = []
    for qid, qlabel in qs:
        mark = " ✅" if qid == current else ""
        rows.append([InlineKeyboardButton(qlabel + mark, callback_data=f"setquality_{qid}")])
    rows.append([InlineKeyboardButton(T["back_btn"], callback_data="settings_back")])
    return InlineKeyboardMarkup(rows)

def patchnote_keyboard(version: str) -> InlineKeyboardMarkup:
    versions = sorted(PATCH_NOTES.keys(), key=lambda v: [int(x) for x in v.split(".")], reverse=True)
    rows = []
    # Кнопки всех версий — выбранная отмечена ✅, сначала новые
    row = []
    for v in versions:
        mark = " ✅" if v == version else ""
        row.append(InlineKeyboardButton(f"v{v}{mark}", callback_data=f"patch_nav_{v}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
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

def format_keyboard(lang="ru", default_fmt: str = "", url: str = "") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    def btn(key, cb):
        mark = " ✅" if cb == f"fmt_{default_fmt}" else ""
        return InlineKeyboardButton(T[key] + mark, callback_data=cb)

    # Базовые строки
    rows = [
        [btn("fmt_video",    "fmt_video"),   btn("fmt_audio",    "fmt_audio")],
        [btn("fmt_gif",      "fmt_gif"),     btn("fmt_circle",   "fmt_circle")],
        [btn("fmt_thumb",    "fmt_thumb"),   btn("fmt_playlist", "fmt_playlist")],
        [btn("fmt_wav",      "fmt_wav"),     btn("fmt_flac",     "fmt_flac")],
    ]
    # Стикерпак — только для t.me/addstickers
    if url and re.search(STICKER_PATTERN, url, re.IGNORECASE):
        rows.append([btn("fmt_sticker", "fmt_sticker")])

    return InlineKeyboardMarkup(rows)


def audio_format_keyboard(lang="ru") -> InlineKeyboardMarkup:
    """Клавиатура выбора формата аудио: MP3 / WAV / FLAC."""
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T["fmt_audio"], callback_data="afmt_mp3"),
         InlineKeyboardButton(T["fmt_wav"],   callback_data="afmt_wav"),
         InlineKeyboardButton(T["fmt_flac"],  callback_data="afmt_flac")],
    ])

def quality_keyboard(lang="ru", default_quality: str = "") -> InlineKeyboardMarkup:
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

async def ffmpeg_with_progress(cmd: list, status_msg, label: str, duration: float = 0) -> bool:
    """Запускает ffmpeg и обновляет прогресс-бар в статус-сообщении."""
    cmd_with_progress = cmd.copy()
    # Вставляем -progress pipe:1 перед выходным файлом
    cmd_with_progress = cmd_with_progress[:-1] + ["-progress", "pipe:1", "-nostats", cmd_with_progress[-1]]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_with_progress,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        last_update = 0
        last_pct = -1
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line = line.decode().strip()
            if line.startswith("out_time_ms=") and duration > 0:
                try:
                    ms = int(line.split("=")[1])
                    pct = min(99, int(ms / 1000000 / duration * 100))
                    now = asyncio.get_event_loop().time()
                    if pct != last_pct and now - last_update > 2:
                        last_pct = pct
                        last_update = now
                        bar = make_progress_bar(pct)
                        try:
                            await status_msg.edit_text(f"{label}\n{bar}")
                        except Exception:
                            pass
                except Exception:
                    pass
        await proc.wait()
        return proc.returncode == 0
    except Exception:
        # Fallback — обычный запуск без прогресса
        return ffmpeg_run(cmd)


def get_video_duration(path: Path) -> float:
    """Возвращает длительность видео в секундах."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=15
        )
        return float(r.stdout.strip())
    except Exception:
        return 0.0


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

async def download_video(url, quality, output_path, status_msg, cancel_flag, fmt="video", lang="ru", audio_codec="mp3") -> Path | None:
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

    # audio_codec передаётся как параметр

    if fmt in ("audio", "wav", "flac"):
        codec = audio_codec if fmt == "audio" else fmt
        postprocessors = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": codec,
                "preferredquality": "0" if codec in ("wav", "flac") else "192",
            },
        ]
        # FFmpegMetadata (теги) — работает для mp3, flac, ogg (требует mutagen)
        # EmbedThumbnail — работает для mp3, flac, m4a, mkv, ogg
        # WAV не поддерживает ни теги ни обложку
        if codec in ("mp3", "flac", "ogg", "opus"):
            postprocessors.append({"key": "FFmpegMetadata"})
        if codec in ("mp3", "flac", "ogg", "opus"):
            postprocessors.append({"key": "EmbedThumbnail"})
            ydl_opts["writethumbnail"] = True
        ydl_opts["postprocessors"] = postprocessors

    def _download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            p = Path(filename)
            if fmt in ("audio", "wav", "flac"):
                codec = audio_codec if fmt == "audio" else fmt
                if fmt == "audio": ext = codec
                else: ext = fmt
                converted = p.with_suffix(f".{ext}")
                if converted.exists():
                    return converted
                # fallback поиск любого аудио
                for try_ext in (ext, "mp3", "opus", "m4a", "webm"):
                    candidate = p.with_suffix(f".{try_ext}")
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

async def safe_edit(query, text, reply_markup=None, parse_mode=None):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        try:
            await query.edit_message_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            try:
                await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception:
                pass

# ─── Команды ──────────────────────────────────────────────────────────────────

# URL картинки меню — можно заменить на свою ссылку или оставить imgur
# URL картинок меню — светлая и тёмная темы (можно задать через env)
MENU_PHOTO_LIGHT = os.environ.get("MENU_PHOTO_LIGHT", "https://i.imgur.com/4M34hi2.png")
MENU_PHOTO_DARK  = os.environ.get("MENU_PHOTO_DARK",  "https://i.imgur.com/4M34hi2.png")
# GIF приветствия — показывается только при /start
MENU_GIF_URL     = os.environ.get("MENU_GIF_URL", "")  # пусто = не показывать

# Кэш file_id по теме — Telegram хранит файлы на своих серверах
_PHOTO_CACHE: dict[str, str] = {}  # "light"/"dark" -> file_id
_GIF_FILE_ID: str | None = None

def get_user_theme(context) -> str:
    """Возвращает тему пользователя: 'light' или 'dark'."""
    return context.user_data.get("theme", "light")

def get_menu_photo_url(theme: str) -> str:
    """Возвращает URL картинки меню по теме."""
    return MENU_PHOTO_DARK if theme == "dark" else MENU_PHOTO_LIGHT

async def send_menu_photo(target, caption: str, reply_markup, context,
                          gif: bool = False) -> None:
    """Отправляет меню с картинкой или GIF. Кэширует file_id."""
    global _GIF_FILE_ID
    theme = get_user_theme(context)

    # GIF приветствия — только если задан и явно запрошен
    if gif and MENU_GIF_URL:
        gif_src = _GIF_FILE_ID or MENU_GIF_URL
        try:
            if hasattr(target, 'reply_animation'):
                msg = await target.reply_animation(
                    animation=gif_src, caption=caption, reply_markup=reply_markup
                )
            else:
                msg = await context.bot.send_animation(
                    chat_id=target.id, animation=gif_src,
                    caption=caption, reply_markup=reply_markup
                )
            if not _GIF_FILE_ID and msg.animation:
                _GIF_FILE_ID = msg.animation.file_id
                logger.info(f"GIF закэширован: {_GIF_FILE_ID}")
            return
        except Exception as e:
            logger.warning(f"GIF не отправился: {e} — fallback на фото")

    # Статичное фото по теме
    photo = _PHOTO_CACHE.get(theme) or get_menu_photo_url(theme)
    try:
        if hasattr(target, 'reply_photo'):
            msg = await target.reply_photo(
                photo=photo, caption=caption, reply_markup=reply_markup
            )
        else:
            msg = await context.bot.send_photo(
                chat_id=target.id, photo=photo,
                caption=caption, reply_markup=reply_markup
            )
        if theme not in _PHOTO_CACHE and msg.photo:
            _PHOTO_CACHE[theme] = msg.photo[-1].file_id
            logger.info(f"Фото [{theme}] закэшировано: {_PHOTO_CACHE[theme]}")
    except Exception as e:
        logger.warning(f"Фото меню не отправилось: {e}")
        text = caption
        if hasattr(target, 'reply_text'):
            await target.reply_text(text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(
                chat_id=target.id, text=text, reply_markup=reply_markup
            )

def _load_user_prefs(user_id: int, context) -> None:
    """Загружает настройки пользователя из Redis/JSON в context.user_data."""
    data = get_data()
    uid_str = str(user_id)
    saved_lang = data.get("user_langs", {}).get(uid_str)
    if saved_lang and "lang" not in context.user_data:
        context.user_data["lang"] = saved_lang
    prefs = data.get("user_prefs", {}).get(uid_str, {})
    if "theme" not in context.user_data and "theme" in prefs:
        context.user_data["theme"] = prefs["theme"]
    if "default_format" not in context.user_data and "format" in prefs:
        context.user_data["default_format"] = prefs["format"]
    if "default_quality" not in context.user_data and "quality" in prefs:
        context.user_data["default_quality"] = prefs["quality"]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    ACTIVE_USERS[user.id] = get_lang(context)
    context.user_data["_uid"] = str(user.id)

    # Загружаем сохранённые настройки
    _load_user_prefs(user.id, context)

    is_admin = user.id == ADMIN_ID
    lang = get_lang(context)

    await update.message.reply_text("👇", reply_markup=persistent_menu_keyboard())
    await send_menu_photo(update.message, t(context, "start_caption"), main_menu_keyboard(is_admin, lang), context, gif=True)

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
    text = update.message.text.strip() if update.message.text else ""

    # Режим ожидания стикерпака
    if context.user_data.get("waiting_sticker"):
        context.user_data["waiting_sticker"] = False
        sticker_match = re.search(r"(?:https?://)?t\.me/addstickers/([A-Za-z0-9_]+)", text)
        if not sticker_match:
            await update.message.reply_text("❌ Неверная ссылка. Формат: t.me/addstickers/ИмяПака")
            return
        pack_name = sticker_match.group(1)
        await update.message.reply_text(t(context, "sticker_downloading"))
        zip_path, count = await download_sticker_pack(pack_name, context.bot, DOWNLOAD_DIR)
        if zip_path and count > 0:
            with open(zip_path, "rb") as f:
                await update.message.reply_document(document=f, filename=f"{pack_name}.zip",
                                                     caption=t(context, "sticker_done", n=count))
            zip_path.unlink(missing_ok=True)
        else:
            await update.message.reply_text(t(context, "sticker_not_found"))
        return

    # Режим поиска YouTube
    if context.user_data.get("waiting_search"):
        context.user_data["waiting_search"] = False
        query_text = text
        lang = context.user_data.get("lang", "ru")
        status = await update.message.reply_text(t(context, "searching"))
        results = await youtube_search(query_text)
        if not results:
            await status.edit_text(t(context, "search_no_results"))
            return
        kb_rows = []
        for i, r in enumerate(results[:5]):
            title = r.get("title", "?")[:50]
            dur = format_duration(r.get("duration", 0))
            kb_rows.append([InlineKeyboardButton(
                f"{title} [{dur}]",
                callback_data=f"search_pick_{i}"
            )])
        context.user_data["search_results"] = results[:5]
        await status.edit_text(
            t(context, "search_results"),
            reply_markup=InlineKeyboardMarkup(kb_rows)
        )
        return

    # Режим объединения — ссылки и текст игнорируем
    if context.user_data.get("waiting_merge"):
        lang = context.user_data.get("lang", "ru")
        msg = ("📎 Отправь видео как файл (не ссылку). Нажми 📎 → Файл → выбери mp4"
               if lang == "ru" else
               "📎 Send video as a file (not a link). Tap 📎 → File → choose mp4")
        await update.message.reply_text(msg)
        return

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

    # Стикерпак — ловим и без https:// (t.me/addstickers/...)
    sticker_match = re.search(r"(?:https?://)?t\.me/addstickers/([A-Za-z0-9_]+)", text)
    if sticker_match:
        pack_name = sticker_match.group(1)
        await update.message.reply_text(t(context, "sticker_downloading"))
        zip_path, count = await download_sticker_pack(pack_name, context.bot, DOWNLOAD_DIR)
        if zip_path and count > 0:
            with open(zip_path, "rb") as f:
                await update.message.reply_document(document=f, filename=f"{pack_name}.zip",
                                                     caption=t(context, "sticker_done", n=count))
            zip_path.unlink(missing_ok=True)
        else:
            await update.message.reply_text(t(context, "sticker_not_found"))
        return

    urls = re.findall(r"https?://[^\s]+", text)
    if not urls:
        await update.message.reply_text(t(context, "no_url"))
        return

    url = urls[0]
    # Старая обработка стикерпаков через https (на случай если выше не поймало)
    if re.search(STICKER_PATTERN, url, re.IGNORECASE):
        pack_name = url.split("/addstickers/")[-1].split("?")[0].strip()
        await update.message.reply_text(t(context, "sticker_downloading"))
        zip_path, count = await download_sticker_pack(pack_name, context.bot, DOWNLOAD_DIR)
        if zip_path and count > 0:
            with open(zip_path, "rb") as f:
                await update.message.reply_document(document=f, filename=f"{pack_name}.zip",
                                                     caption=t(context, "sticker_done", n=count))
            zip_path.unlink(missing_ok=True)
        else:
            await update.message.reply_text(t(context, "sticker_not_found"))
        return

    if not is_supported_url(url):
        lang = get_lang(context)
        hint = ("💡 Для стикерпаков отправь ссылку вида: t.me/addstickers/ИмяПака" 
                if lang == "ru" else 
                "💡 For stickerpacks send a link like: t.me/addstickers/PackName")
        await update.message.reply_text(t(context, "unsupported") + "\n\n" + hint)
        return

    platform = get_platform(url)

    # Spotify — не поддерживается yt-dlp напрямую
    if re.search(SPOTIFY_PATTERN, url, re.IGNORECASE):
        await update.message.reply_text(t(context, "spotify_not_supported"))
        return

    # Яндекс Музыка — геоблок вне России
    if re.search(YANDEX_PATTERN, url, re.IGNORECASE):
        await update.message.reply_text(t(context, "yandex_geo_error"))
        return

    # Pinterest — предупреждение что работает только для видео
    if re.search(r"pinterest\.com|pin\.it", url, re.IGNORECASE):
        await update.message.reply_text(t(context, "pinterest_hint"))

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
        reply_markup=format_keyboard(get_lang(context), context.user_data.get("default_format", ""), context.user_data.get("pending_url", ""))
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
        def_quality = context.user_data.get("default_quality", "best")
        T = TEXTS.get(lang, TEXTS["ru"])
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        fmt_labels_ru = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Кружочек"}
        fmt_labels_en = {"video": "🎬 Video", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Circle"}
        fmt_labels = fmt_labels_en if lang == "en" else fmt_labels_ru
        quality_label = {"360": "360p", "480": "480p", "720": "720p",
                         "1080": "1080p", "best": "Макс." if lang == "ru" else "Max"}.get(def_quality, def_quality)
        text = T["settings_info"].format(
            theme=theme_label,
            fmt=fmt_labels.get(def_fmt, def_fmt),
            quality=quality_label
        )
        await safe_edit(query, text,
                        reply_markup=settings_keyboard(theme, def_fmt, def_quality, lang))

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

# ─── Callback: объединение видео ─────────────────────────────────────────────

async def handle_merge_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "merge_do":
        file_entries = context.user_data.get("merge_files", [])
        if len(file_entries) < 2:
            await query.answer(
                f"❌ Нужно минимум 2 видео, сейчас: {len(file_entries)}" if get_lang(context) == "ru"
                else f"❌ Need at least 2 videos, got: {len(file_entries)}",
                show_alert=True
            )
            return
        context.user_data["waiting_merge"] = False
        await safe_edit(query, t(context, "merge_processing"))
        # Скачиваем файлы по file_id
        paths = []
        for i, entry in enumerate(file_entries):
            try:
                fname = DOWNLOAD_DIR / f"merge_{query.from_user.id}_{i}.mp4"
                tg_file = await context.bot.get_file(entry["file_id"])
                await tg_file.download_to_drive(fname)
                paths.append(fname)
                logger.info(f"merge: скачан файл {i+1}/{len(file_entries)}")
            except Exception as e:
                logger.error(f"merge download error: {e}")
                await safe_edit(query, f"❌ Ошибка загрузки файла {i+1}: {e}")
                return
        output = DOWNLOAD_DIR / f"merged_{query.from_user.id}.mp4"
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(None, merge_videos, paths, output)
        if ok and output.exists():
            with open(output, "rb") as f:
                await query.message.reply_video(video=f, caption=t(context, "merge_done"))
            output.unlink(missing_ok=True)
        else:
            await safe_edit(query, "❌ Не удалось объединить видео.")
        for p in paths:
            try: p.unlink(missing_ok=True)
            except: pass
        context.user_data["merge_files"] = []

    elif query.data == "merge_cancel":
        context.user_data["waiting_merge"] = False
        context.user_data["merge_files"] = []
        await safe_edit(query, t(context, "preview_cancelled"))

# ─── Callback: настройки профиля ─────────────────────────────────────────────

async def handle_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    lang = get_lang(context)
    T = TEXTS.get(lang, TEXTS["ru"])

    def _save_prefs():
        """Сохраняем настройки пользователя в Redis/JSON."""
        uid = str(query.from_user.id)
        db = get_data()
        prefs = db.setdefault("user_prefs", {}).setdefault(uid, {})
        prefs["theme"]   = context.user_data.get("theme", "light")
        prefs["format"]  = context.user_data.get("default_format", "video")
        prefs["quality"] = context.user_data.get("default_quality", "best")
        save_data(db)

    if data == "settings_theme":
        # Переключаем тему
        current = context.user_data.get("theme", "light")
        new_theme = "dark" if current == "light" else "light"
        context.user_data["theme"] = new_theme
        _save_prefs()
        theme_label = T["theme_dark"] if new_theme == "dark" else T["theme_light"]
        def_fmt = context.user_data.get("default_format", "video")
        def_quality = context.user_data.get("default_quality", "best")
        fmt_labels = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Кружочек"}
        if lang == "en":
            fmt_labels = {"video": "🎬 Video", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Circle"}
        quality_label = {"360": "360p", "480": "480p", "720": "720p",
                         "1080": "1080p", "best": "Макс." if lang == "ru" else "Max"}.get(def_quality, def_quality)
        text = T["settings_info"].format(
            theme=theme_label,
            fmt=fmt_labels.get(def_fmt, def_fmt),
            quality=quality_label
        )
        # Удаляем старое сообщение — нельзя поменять фото через edit
        try:
            await query.message.delete()
        except Exception:
            pass
        # Отправляем новое с другой картинкой
        photo_url = MENU_PHOTO_DARK if new_theme == "dark" else MENU_PHOTO_LIGHT
        cached = _PHOTO_CACHE.get(new_theme)
        try:
            msg = await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=cached or photo_url,
                caption=text,
                reply_markup=settings_keyboard(new_theme, def_fmt, def_quality, lang)
            )
            if new_theme not in _PHOTO_CACHE and msg.photo:
                _PHOTO_CACHE[new_theme] = msg.photo[-1].file_id
        except Exception as e:
            logger.warning(f"Не удалось отправить фото темы: {e}")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=settings_keyboard(new_theme, def_fmt, def_quality, lang)
            )

    elif data == "settings_fmt":
        current_fmt = context.user_data.get("default_format", "video")
        await safe_edit(query, T["default_fmt_title"],
                        reply_markup=settings_fmt_keyboard(current_fmt, lang))

    elif data.startswith("setfmt_"):
        fmt = data.replace("setfmt_", "")
        context.user_data["default_format"] = fmt
        _save_prefs()
        theme = context.user_data.get("theme", "light")
        def_quality = context.user_data.get("default_quality", "best")
        fmt_labels = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Кружочек"}
        if lang == "en":
            fmt_labels = {"video": "🎬 Video", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Circle"}
        quality_label = {"360": "360p", "480": "480p", "720": "720p",
                         "1080": "1080p", "best": "Макс." if lang == "ru" else "Max"}.get(def_quality, def_quality)
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        text = T["settings_info"].format(
            theme=theme_label, fmt=fmt_labels.get(fmt, fmt), quality=quality_label
        )
        await safe_edit(query, text,
                        reply_markup=settings_keyboard(theme, fmt, def_quality, lang))

    elif data == "settings_quality":
        current_q = context.user_data.get("default_quality", "best")
        await safe_edit(query, T["default_fmt_title"],
                        reply_markup=settings_quality_keyboard(current_q, lang))

    elif data.startswith("setquality_"):
        quality = data.replace("setquality_", "")
        context.user_data["default_quality"] = quality
        _save_prefs()
        theme = context.user_data.get("theme", "light")
        def_fmt = context.user_data.get("default_format", "video")
        fmt_labels = {"video": "🎬 Видео", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Кружочек"}
        if lang == "en":
            fmt_labels = {"video": "🎬 Video", "audio": "🎵 MP3", "gif": "🌀 GIF", "circle": "⭕ Circle"}
        quality_label = {"360": "360p", "480": "480p", "720": "720p",
                         "1080": "1080p", "best": "Макс." if lang == "ru" else "Max"}.get(quality, quality)
        theme_label = T["theme_dark"] if theme == "dark" else T["theme_light"]
        text = T["settings_info"].format(
            theme=theme_label, fmt=fmt_labels.get(def_fmt, def_fmt), quality=quality_label
        )
        await safe_edit(query, text,
                        reply_markup=settings_keyboard(theme, def_fmt, quality, lang))

    elif data == "settings_back":
        is_admin = query.from_user.id == ADMIN_ID
        try:
            await query.edit_message_caption(
                caption=t(context, "menu_title"),
                reply_markup=main_menu_keyboard(is_admin, lang)
            )
        except Exception:
            await safe_edit(query, t(context, "menu_title"),
                            reply_markup=main_menu_keyboard(is_admin, lang))

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
        reply_markup=format_keyboard(get_lang(context), context.user_data.get("default_format", ""), context.user_data.get("pending_url", ""))
    )

# ─── Callback: формат ─────────────────────────────────────────────────────────

async def handle_format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    fmt = query.data.replace("fmt_", "")
    context.user_data["format"] = fmt
    platform = context.user_data.get("platform", "Видео")

    if fmt in ("audio", "wav", "flac"):
        context.user_data["quality"] = "best"
        context.user_data["audio_format"] = fmt if fmt != "audio" else "mp3"
        fmt_label = {"audio": "MP3", "wav": "WAV", "flac": "FLAC"}[fmt]
        await safe_edit(query, f"🎵 {platform} • {fmt_label}\n\n{t(context, 'step_audio')}", reply_markup=audio_keyboard(get_lang(context)))

    elif fmt == "gif":
        context.user_data["quality"] = "480"
        context.user_data["audio"] = "mute"
        context.user_data["orientation"] = "original"
        await safe_edit(query, f"🌀 {platform} • GIF\n\n{t(context, 'step_trim')}", reply_markup=trim_keyboard(get_lang(context)))

    elif fmt == "sticker":
        # Стикерпак — сразу скачиваем
        url = context.user_data.get("pending_url", "")
        pack_name = url.split("/addstickers/")[-1].split("?")[0].strip()
        await safe_edit(query, t(context, "sticker_downloading"))
        zip_path, count = await download_sticker_pack(pack_name, context.bot, DOWNLOAD_DIR)
        if zip_path and count > 0:
            with open(zip_path, "rb") as f:
                await query.message.reply_document(
                    document=f,
                    filename=f"{pack_name}.zip",
                    caption=t(context, "sticker_done", n=count)
                )
            zip_path.unlink(missing_ok=True)
        else:
            await safe_edit(query, t(context, "sticker_not_found"))
        return

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
        await safe_edit(query, f"📋 {platform} • {t(context, 'fmt_playlist')}\n\n{t(context, 'step_quality')}", reply_markup=quality_keyboard(get_lang(context), context.user_data.get("default_quality", "")))

    else:  # video
        await safe_edit(query, f"🎬 {platform} • {t(context, 'fmt_video')}\n\n{t(context, 'step_quality')}", reply_markup=quality_keyboard(get_lang(context), context.user_data.get("default_quality", "")))

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
        await show_preview_or_download(query, context)
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
        await show_preview_or_download(query, context)

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

# ─── Callback: результаты поиска ────────────────────────────────────────────

async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.replace("search_pick_", ""))
    results = context.user_data.get("search_results", [])
    if idx >= len(results):
        await safe_edit(query, "❌ Результат недоступен.")
        return
    item = results[idx]
    url = item.get("url") or item.get("webpage_url", "")
    if not url.startswith("http"):
        url = f"https://www.youtube.com/watch?v={item.get('id','')}"
    platform = "YouTube"
    context.user_data["pending_url"]   = url
    context.user_data["platform"]      = platform
    context.user_data["cancel_flag"]   = {"cancelled": False}
    context.user_data["trim_start"]    = None
    context.user_data["trim_end"]      = None
    context.user_data["subtitles"]     = False
    context.user_data["waiting_trim"]  = False
    context.user_data["speed"]         = "1.0"
    lang = get_lang(context)
    allowed, remaining = check_limit(query.from_user.id)
    if not allowed:
        await safe_edit(query, t(context, "limit", limit=DAILY_LIMIT))
        return
    await safe_edit(
        query,
        f"🎬 {platform}\n{t(context, 'remaining', remaining=remaining)}\n\n{t(context, 'step1')}",
        reply_markup=format_keyboard(lang, context.user_data.get("default_format", ""), url)
    )

# ─── Callback: превью ────────────────────────────────────────────────────────

async def handle_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)

    if query.data == "preview_confirm":
        await safe_edit(query, f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                        reply_markup=cancel_keyboard(lang))
        # Используем текущее сообщение как статус-сообщение для download
        await _run_download(query.from_user, query.message, context)

    elif query.data == "preview_cancel":
        await safe_edit(query, t(context, "preview_cancelled"))

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
            await show_preview_or_download(query, context)
    else:
        context.user_data["waiting_trim"] = True
        context.user_data["trim_start"] = None
        context.user_data["trim_end"] = None
        await safe_edit(query, t(context, "trim_enter_start"))

# ─── Функция объединения видео ───────────────────────────────────────────────

def merge_videos(paths: list[Path], output: Path) -> bool:
    """Объединяет список видео в одно через ffmpeg concat demuxer."""
    list_file = output.with_suffix(".txt")
    with open(list_file, "w") as f:
        for p in paths:
            f.write(f"file '{p.absolute()}'\n")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(list_file),
        "-c", "copy", str(output)
    ]
    ok = ffmpeg_run(cmd)
    list_file.unlink(missing_ok=True)
    return ok


def merge_keyboard(lang: str = "ru", count: int = 0) -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    merge_label = f"🔗 Объединить ({count})" if count >= 2 else f"🔗 Объединить (нужно ещё {2-count})"
    if lang == "en":
        merge_label = f"🔗 Merge ({count})" if count >= 2 else f"🔗 Merge (need {2-count} more)"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(merge_label, callback_data="merge_do"),
         InlineKeyboardButton(T.get("merge_cancel_btn", "❌ Отмена"), callback_data="merge_cancel")],
    ])

# ─── Callback: скачать ещё раз ───────────────────────────────────────────────

async def handle_download_again_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    lang = get_lang(context)
    url = context.user_data.get("pending_url")
    if not url:
        await query.answer("❌ Ссылка устарела, отправь заново" if lang == "ru" else "❌ Link expired, send again", show_alert=True)
        return
    # Запускаем заново с теми же настройками
    status_msg = await query.message.reply_text(
        f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
        reply_markup=cancel_keyboard(lang)
    )
    await _run_download(query.from_user, status_msg, context)

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

async def download_sticker_pack(pack_name: str, bot, dest_dir: Path) -> tuple[Path | None, int]:
    """Скачивает стикерпак Telegram в ZIP. Возвращает (zip_path, count)."""
    try:
        sticker_set = await bot.get_sticker_set(pack_name)
        stickers = sticker_set.stickers
        zip_path = dest_dir / f"{pack_name}.zip"
        count = 0
        import zipfile, io
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, sticker in enumerate(stickers):
                try:
                    file = await bot.get_file(sticker.file_id)
                    data = await file.download_as_bytearray()
                    ext = "webm" if sticker.is_video else ("tgs" if sticker.is_animated else "webp")
                    zf.writestr(f"{i+1:03d}.{ext}", bytes(data))
                    count += 1
                except Exception as e:
                    logger.warning(f"sticker {i}: {e}")
        return zip_path, count
    except Exception as e:
        logger.error(f"download_sticker_pack: {e}")
        return None, 0


async def youtube_search(query: str, max_results: int = 5) -> list[dict]:
    """Ищет видео на YouTube через yt-dlp."""
    ydl_opts = {
        "quiet": True, "no_warnings": True, "skip_download": True,
        "extract_flat": True,
        "extractor_args": {"youtube": {"player_client": ["ios"]}},
        "socket_timeout": 15,
    }
    try:
        loop = asyncio.get_event_loop()
        def _search():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
                return info.get("entries", []) if info else []
        results = await asyncio.wait_for(loop.run_in_executor(None, _search), timeout=20)
        return [r for r in results if r]
    except Exception as e:
        logger.warning(f"youtube_search error: {e}")
        return []


async def fetch_video_info(url: str) -> dict | None:
    """Получает метаданные видео без скачивания."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extractor_args": {"youtube": {"player_client": ["ios", "android", "web"]}},
        "http_headers": {"User-Agent": "com.google.ios.youtube/19.29.1 CFNetwork/1568.100.1 Darwin/24.0.0"},
        "socket_timeout": 15,
    }
    try:
        loop = asyncio.get_event_loop()
        def _get():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await asyncio.wait_for(loop.run_in_executor(None, _get), timeout=30)
        if info:
            logger.info(f"preview OK: {info.get('title','?')[:40]}")
        return info
    except asyncio.TimeoutError:
        logger.warning(f"fetch_video_info timeout: {url[:50]}")
        return None
    except Exception as e:
        logger.warning(f"fetch_video_info error: {e}")
        return None


def format_duration(seconds) -> str:
    """Форматирует секунды в MM:SS или HH:MM:SS."""
    try:
        s = int(seconds)
        h, m, sec = s // 3600, (s % 3600) // 60, s % 60
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"
    except Exception:
        return "?"


def preview_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    T = TEXTS.get(lang, TEXTS["ru"])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(T.get("preview_download", "⬇️ Скачать"), callback_data="preview_confirm"),
         InlineKeyboardButton(T.get("cancel_btn", "❌ Отмена"), callback_data="preview_cancel")],
    ])


async def show_preview_or_download(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает превью видео или сразу скачивает если превью недоступно."""
    lang = get_lang(context)
    url = context.user_data.get("pending_url", "")
    # Скрываем старое меню — убираем кнопки
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    # Отправляем новое сообщение с лоадером
    status = await query.message.reply_text(t(context, "preview_loading"))
    info = await fetch_video_info(url)
    if info:
        title    = info.get("title", "?")[:60]
        dur      = format_duration(info.get("duration", 0))
        uploader = info.get("uploader") or info.get("channel") or ""
        views    = info.get("view_count")
        views_str = f"👁 {views:,}".replace(",", " ") if views else ""
        lines = [f"🎬 {title}", f"⏱ {dur}"]
        if uploader: lines.append(f"👤 {uploader}")
        if views_str: lines.append(views_str)
        lines.append("\nСкачать?" if lang == "ru" else "\nDownload?")
        await status.edit_text("\n".join(lines), reply_markup=preview_keyboard(lang))
        # Сохраняем статус-сообщение для preview_confirm
        context.user_data["preview_status_id"] = status.message_id
    else:
        logger.warning(f"preview failed for url: {url[:60]}, скачиваем без превью")
        await status.edit_text(f"{t(context, 'downloading')}\n{make_progress_bar(0)}",
                               reply_markup=cancel_keyboard(lang))
        await _run_download(query.from_user, status, context)


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
        file_path = await download_video(url, quality, DOWNLOAD_DIR, status_msg, cancel_flag, fmt, lang=context.user_data.get("lang", "ru"), audio_codec=context.user_data.get("audio_format", "mp3"))

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
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            output_gif = current.with_suffix(".gif")
            cmd_gif = [
                "ffmpeg", "-y", "-i", str(current),
                "-vf", "fps=15,scale=480:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
                "-loop", "0", str(output_gif)
            ]
            await ffmpeg_with_progress(cmd_gif, status_msg, "🌀 Конвертирую в GIF...", dur)
            current = output_gif if output_gif.exists() else current
            if current not in files_to_clean:
                files_to_clean.append(current)

        # Кружочек
        elif fmt == "circle":
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            output_circle = current.with_stem(current.stem + "_circle").with_suffix(".mp4")
            cmd_circle = [
                "ffmpeg", "-y", "-i", str(current),
                "-vf", "crop=min(iw\,ih):min(iw\,ih),scale=384:384",
                "-t", "60", "-c:v", "libx264", "-preset", "fast",
                "-c:a", "aac", "-b:a", "128k", str(output_circle)
            ]
            await ffmpeg_with_progress(cmd_circle, status_msg, "⭕ Конвертирую в кружочек...", dur)
            current = output_circle if output_circle.exists() else current
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
            dur = await asyncio.get_event_loop().run_in_executor(None, get_video_duration, current)
            output_speed = current.with_stem(current.stem + "_speed")
            if speed >= 0.5:
                atempo = f"atempo={speed}"
            else:
                atempo = f"atempo=0.5,atempo={speed/0.5}"
            cmd_speed = [
                "ffmpeg", "-y", "-i", str(current),
                "-vf", f"setpts={1/speed}*PTS",
                "-af", atempo,
                "-c:v", "libx264", "-preset", "fast",
                str(output_speed)
            ]
            await ffmpeg_with_progress(cmd_speed, status_msg, f"⚡ Применяю скорость {speed}x...", dur / speed)
            current = output_speed if output_speed.exists() else current
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

        # Кнопка "Скачать ещё раз"
        lang = context.user_data.get("lang", "ru")
        again_label = "🔄 Скачать ещё раз" if lang == "ru" else "🔄 Download again"
        again_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(again_label, callback_data="download_again")
        ]])

        # Отправка
        with open(current, "rb") as f:
            if fmt == "audio":
                await status_msg.reply_audio(audio=f, caption=caption, reply_markup=again_kb)
            elif fmt == "gif":
                await status_msg.reply_animation(animation=f, caption=caption, reply_markup=again_kb)
            elif fmt == "circle":
                await status_msg.reply_video_note(video_note=f)
            else:
                await status_msg.reply_video(video=f, caption=caption,
                                             supports_streaming=True, reply_markup=again_kb)

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

# ─── Обработчик видео-файлов (для merge) ────────────────────────────────────

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Принимает видео-файлы для объединения."""
    lang = get_lang(context)
    if not context.user_data.get("waiting_merge"):
        hint = ("🔗 Хочешь объединить видео? Нажми «Объединить видео» в меню и потом отправляй файлы."
                if lang == "ru" else
                "🔗 Want to merge videos? Press «Merge videos» in the menu, then send files.")
        await update.message.reply_text(hint)
        return
    video = update.message.video or update.message.document
    if not video:
        await update.message.reply_text("❌ Не распознал видео-файл. Отправь как файл (документ).")
        return
    # Сохраняем file_id — скачаем при объединении
    file_size_mb = (video.file_size or 0) / 1024 / 1024
    if file_size_mb > 20:
        await update.message.reply_text(
            f"❌ Файл {file_size_mb:.0f} МБ — слишком большой (макс 20 МБ через Telegram)."
            if lang == "ru" else
            f"❌ File {file_size_mb:.0f} MB — too large (max 20 MB via Telegram)."
        )
        return
    files = context.user_data.setdefault("merge_files", [])
    files.append({"file_id": video.file_id, "size": file_size_mb})
    n = len(files)
    logger.info(f"merge: добавлен файл {n}, file_id={video.file_id[:20]}...")
    await update.message.reply_text(
        t(context, "merge_received", n=n),
        reply_markup=merge_keyboard(lang, count=n)
    )

# ─── Запуск ───────────────────────────────────────────────────────────────────

# ─── Фоновые задачи ──────────────────────────────────────────────────────────

async def task_limit_reset_notify(context):
    """Уведомляет активных пользователей о сбросе лимита в полночь."""
    logger.info("Сбрасываю лимиты и рассылаю уведомления...")
    data = get_data()
    # Сбрасываем лимиты
    data["downloads_today"] = {}
    data["last_reset"] = str(date.today())
    save_data(data)
    # Уведомляем активных пользователей
    notified = 0
    for uid, lang in list(ACTIVE_USERS.items()):
        try:
            T = TEXTS.get(lang, TEXTS["ru"])
            msg = T.get("limit_reset", "🔄 Лимит сброшен! Снова доступно {limit} скачиваний.").format(limit=DAILY_LIMIT)
            await context.bot.send_message(chat_id=uid, text=msg)
            notified += 1
            await asyncio.sleep(0.05)  # не флудим
        except Exception:
            pass
    logger.info(f"Уведомлено {notified} пользователей о сбросе лимита")


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
    app.add_handler(CallbackQueryHandler(handle_settings_callback,    pattern="^settings_|^setfmt_|^setquality_"))
    app.add_handler(CallbackQueryHandler(handle_patch_nav_callback,   pattern="^patch_"))
    app.add_handler(CallbackQueryHandler(handle_cancel_callback,      pattern="^cancel_download"))

    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.MimeType("video/mp4") | 
        filters.Document.MimeType("video/quicktime") |
        filters.Document.MimeType("video/x-matroska") |
        filters.Document.MimeType("video/webm"),
        handle_video_file
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_search_callback,        pattern="^search_pick_"))
    app.add_handler(CallbackQueryHandler(handle_preview_callback,       pattern="^preview_"))
    app.add_handler(CallbackQueryHandler(handle_download_again_callback, pattern="^download_again"))
    app.add_handler(CallbackQueryHandler(handle_merge_callback,         pattern="^merge_"))

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

    logger.info(f"Бот v{BOT_VERSION} запущен...")

    # Удаляем вебхук и ждём — на случай если старый инстанс ещё не умер
    import time
    async def _pre_init(app):
        # 1. Удаляем вебхук и ждём — на случай если старый инстанс ещё не умер
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook удалён, жду 3 сек перед polling...")
        except Exception as e:
            logger.warning(f"delete_webhook: {e}")
        await asyncio.sleep(3)
        # 2. Запускаем фоновые задачи
        app.job_queue.run_repeating(
            callback=lambda ctx: asyncio.ensure_future(task_ytdlp_update_once(ctx)),
            interval=7 * 24 * 3600,
            first=7 * 24 * 3600,
            name="ytdlp_update"
        )
        # Сброс лимитов в полночь каждый день
        from datetime import time as dtime
        app.job_queue.run_daily(
            callback=task_limit_reset_notify,
            time=dtime(hour=0, minute=0, second=0),
            name="limit_reset"
        )
        await task_redis_queue()

    app.post_init = _pre_init

    # Счётчик ошибок для алертов
    _error_count = {"count": 0, "last_alert": 0}

    # Обработчик ошибок — логируем + алерты при накоплении
    async def error_handler(update, context):
        import time
        err = context.error
        if "Conflict" in str(err):
            logger.warning("Конфликт polling — возможно запущен второй экземпляр бота. Жду 10 сек...")
            await asyncio.sleep(10)
            return
        logger.error(f"Ошибка: {err}")
        _error_count["count"] += 1
        now = time.time()
        # Алерт если 5+ ошибок за 5 минут
        if _error_count["count"] >= 5 and now - _error_count["last_alert"] > 300:
            _error_count["count"] = 0
            _error_count["last_alert"] = now
            try:
                await app.bot.send_message(
                    ADMIN_ID,
                    f"⚠️ Бот: {_error_count['count']+5} ошибок за 5 минут!\n"
                    f"Последняя: <code>{str(err)[:200]}</code>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

    app.add_error_handler(error_handler)

    logger.info("Запуск в Polling режиме...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
