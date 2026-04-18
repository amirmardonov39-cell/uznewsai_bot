
import os
import json
import sqlite3
import logging
import requests
import feedparser
import random
import time
import asyncio
import re
import urllib.parse
import httpx
import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_PATH = os.getenv("DB_PATH", "bot_database.sqlite")
# These can be pre-set via Railway environment variables so the bot works
# immediately after deploy without needing /set_admin and /set_channel
ENV_ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
ENV_PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "").strip()

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.error("Missing TELEGRAM_TOKEN or GEMINI_API_KEY.")
    exit(1)

# Initialize Gemini Client
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = "gemini-2.5-flash-lite"

class TranslatedArticle(BaseModel):
    emoji: str = Field(
        description="One highly relevant emoji for the news topic. ⚖️ law/court, 🔐 cybersecurity/hacking, 💰 fintech/crypto, 🤖 AI/LegalTech, 📜 legislation, 🛡️ data privacy",
        default="⚖️"
    )
    headline_ru: str = Field(
        description="""
ПРОФЕССИОНАЛЬНЫЙ заголовок на русском с emoji в начале. Требования:
- Максимум 12 слов
- Должен ИНТРИГОВАТЬ: задавать вопрос, называть конкретную цифру, раскрывать конфликт или неожиданный поворот
- Конкретный факт или имя, никакой воды
- НЕ начинать с 'Как', 'Почему' — только утверждение или интригующий вопрос
Пример хорошего: '🔐 США арестовали хакера за кражу $90M у 300 компаний'
Пример плохого: '🤖 Новые технологии меняют правовую сферу'""",
        default=""
    )
    headline_uz: str = Field(
        description="""
Тот же заголовок на НАСТОЯЩЕМ литературном узбекском языке. Правила:
- НЕ калька с русского, а смысловой перевод
- Говори как образованный журналист из Ташкента
- Используй живые узбекские идиомы и конструкции
- Максимум 12 слов""",
        default=""
    )
    analysis_ru: str = Field(
        description="""
АНАЛИТИЧЕСКИЙ ТЕКСТ на русском — ровно 3 предложения:
ПРЕДЛОЖЕНИЕ 1 (ФАКТ): Кто, что, где, когда — конкретно и ёмко. Цифры, имена, даты.
ПРЕДЛОЖЕНИЕ 2 (КОНТЕКСТ): Почему это важно, какая backstory, что предшествовало.
ПРЕДЛОЖЕНИЕ 3 (ВЫВОД/УДАР): Что это меняет для читателя, бизнеса, рынка или права — должно ЗАЦЕПИТЬ.
Стиль: умный редактор Forbes/РБК — без воды, без клише, без роботизированных фраз.
Лимит: 420 символов.""",
        default=""
    )
    analysis_uz: str = Field(
        description="""
Тот же аналитический текст на НАСТОЯЩЕМ литературном узбекском — ровно 3 предложения.
ТРЕБОВАНИЯ К ПЕРЕВОДУ (КРИТИЧНО!):
1. ЗАПРЕЩЁН дословный перевод — это калька и читается ужасно
2. Переводи СМЫСЛ, а не слова. Используй узбекские обороты и конструкции
3. Проверяй: звучало бы это естественно из уст ташкентского журналиста?
4. Термины (AI, blockchain, GDPR) оставляй как есть, но вокруг них строй живые узбекские предложения
5. НЕ используй: 'muhim', 'dolzarb', 'shubhasiz' как вводные — это штампы
Лимит: 420 символов.""",
        default=""
    )
    hashtags: str = Field(
        description="2-3 тега. Только из: #CyberLaw #LegalTech #FinTech #AILaw #Kiberjinoyat #Huquq #Kriptovalyuta #DataPrivacy #Regulation #DigitalLaw",
        default="#CyberLaw #LegalTech"
    )
    image_prompt: str = Field(
        description="Short English prompt for a relevant thematic image (10-15 words). Be specific: 'judge using AI courtroom digital gavel', 'hacker arrested handcuffs laptop cybercrime'",
        default="cybersecurity law digital justice courtroom"
    )
    reject: bool = Field(
        description="Set to true ONLY IF the news is completely irrelevant: general crimes without tech angle, sports, entertainment, health, weather, fires, university grades. If in doubt about relevance — set false.",
        default=False
    )

def strip_artificial_words(text: str) -> str:
    """Remove robotic filler phrases from AI output."""
    patterns = [
        r'\bВажно:\s*', r'\bMuhim:\s*', r'\bВАЖНО:\s*', r'\bMUHIM:\s*',
        r'\bОтметим,?\s+что\s*', r'\bСледует отметить,?\s*',
        r'\bСтоит отметить,?\s*', r'\bПо мнению экспертов,?\s*',
        r'\bЭксперты считают,?\s*', r'\bПо данным\s+\w+,?\s*',
        r'\bShuni ta.lab qilish kerakki,?\s*', r'\bQo.shimcha qilib aytish kerak,?\s*',
        r'\bAlbatta,?\s*', r'\bShubhasiz,?\s*', r'\bDolzarb\s+', r'\bMuhim\s+',
    ]
    for pattern in patterns:
        text = re.sub(pattern, '', text)
    return text.strip()

_HTML_TAG_RE = re.compile(r'<[^>]+>')

def _visible_len(html_text: str) -> int:
    """Returns the number of visible characters (strips HTML tags)."""
    return len(_HTML_TAG_RE.sub('', html_text))

def safe_caption(text: str, limit: int = 1024) -> str:
    """
    Ensures the caption never exceeds Telegram's visible-text limit.
    The footer (🔗 link + 📢 branding) is ALWAYS kept intact.
    Only the body text is truncated when needed.
    """
    if _visible_len(text) <= limit:
        return text

    # Detect and protect the footer block (🔗…\n📢… at the end)
    footer_match = re.search(r'(\n{1,2}🔗[^\n]*\n📢[^\n]*)\s*$', text)
    if not footer_match:
        footer_match = re.search(r'(\n{1,2}📢[^\n]*)\s*$', text)

    if footer_match:
        footer = footer_match.group(1)
        body = text[:footer_match.start()]
    else:
        footer = ''
        body = text

    footer_vis = _visible_len(footer)
    body_limit = limit - footer_vis

    # Strip HTML from body and truncate at last sentence boundary
    body_plain = _HTML_TAG_RE.sub('', body)
    if len(body_plain) > body_limit:
        return body_plain[:body_limit] + "..." + footer

    return body_plain + footer

async def send_article_media(context, chat_id, final_photo, media_type, caption_combined, keyboard=None):
    """Sends photo/video with caption. Falls back to text-only if any photo step fails.
    Raises ValueError if caption has no meaningful visible content (last line of defense).
    """
    # HARD GUARD: never send a post with empty/near-empty visible text
    visible = _visible_len(caption_combined or "")
    if visible < 80:
        raise ValueError(f"send_article_media: caption too short ({visible} visible chars) — refusing to send empty post. Caption repr: {repr(caption_combined[:120])}")


    caption_safe = safe_caption(caption_combined)

    async def _send_photo(photo):
        if not photo:  # None or empty bytes — don't even try
            return None
        if media_type == "video" and isinstance(photo, str) and not photo.startswith("http"):
            return await context.bot.send_video(
                chat_id=chat_id, video=photo,
                caption=caption_safe, reply_markup=keyboard, parse_mode="HTML"
            )
        return await context.bot.send_photo(
            chat_id=chat_id, photo=photo,
            caption=caption_safe, reply_markup=keyboard, parse_mode="HTML"
        )

    async def _send_text_only():
        """Always works — disable_web_page_preview prevents Telegram from fetching URLs in caption."""
        return await context.bot.send_message(
            chat_id=chat_id,
            text=caption_safe,
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    # If no photo provided, go straight to text-only
    if not final_photo:
        return await _send_text_only()

    # Step 1: Try primary photo
    try:
        result = await _send_photo(final_photo)
        if result:
            return result
    except Exception as e1:
        logger.warning(f"Primary photo failed ({e1}), sending text-only...")

    # Step 2: Text-only fallback
    try:
        return await _send_text_only()
    except Exception as e2:
        logger.error(f"Even text-only message failed: {e2}")
        raise

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT UNIQUE,
            text_uz TEXT,
            text_ru TEXT,
            photo_url TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Safely attach new columns (backwards compatibility)
    for col_def in [
        "ALTER TABLE articles ADD COLUMN media_type TEXT DEFAULT 'photo'",
        "ALTER TABLE articles ADD COLUMN title_hash TEXT DEFAULT ''",
    ]:
        try:
            cursor.execute(col_def)
        except sqlite3.OperationalError:
            pass
    # Index for fast title dedup lookup
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_title_hash ON articles(title_hash)")
    except sqlite3.OperationalError:
        pass

    # Seed config from environment variables (Railway ephemeral filesystem fix)
    if ENV_ADMIN_CHAT_ID:
        cursor.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('admin_chat', ?)",
            (ENV_ADMIN_CHAT_ID,)
        )
        logger.info(f"DB seeded admin_chat from env: {ENV_ADMIN_CHAT_ID}")
    if ENV_PUBLISH_CHANNEL:
        cursor.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('publish_channel', ?)",
            (ENV_PUBLISH_CHANNEL,)
        )
        logger.info(f"DB seeded publish_channel from env: {ENV_PUBLISH_CHANNEL}")

    conn.commit()
    conn.close()

def get_publish_channel():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key = 'publish_channel'")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def set_publish_channel(channel_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('publish_channel', ?)", (channel_id,))
    conn.commit()
    conn.close()

def get_admin_chat():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key = 'admin_chat'")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def set_admin_chat(chat_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('admin_chat', ?)", (chat_id,))
    conn.commit()
    conn.close()

def is_link_processed(link: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM articles WHERE link = ?", (link,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def normalize_title(title: str) -> str:
    """Normalize a title/headline for deduplication: lowercase, remove punctuation, short words."""
    if not title:
        return ""
    t = title.lower().strip()
    # Remove punctuation
    t = re.sub(r'[^\w\s]', '', t)
    # Remove very short words (articles, prepositions)
    words = [w for w in t.split() if len(w) > 2]
    # Take first 6 significant words to form a stable fingerprint
    return ' '.join(words[:6])

def is_title_processed(title: str) -> bool:
    """Check if a news item with a very similar title was already processed (dedup by content)."""
    fingerprint = normalize_title(title)
    if not fingerprint or len(fingerprint) < 10:
        return False
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM articles WHERE title_hash = ?", (fingerprint,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def save_article(link: str, text_uz: str, text_ru: str, photo_url: str, media_type: str = "photo", title_hash: str = "") -> int:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO articles (link, text_uz, text_ru, photo_url, media_type, title_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (link, text_uz, text_ru, photo_url, media_type, title_hash))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return -1
    finally:
        conn.close()

SYSTEM_PROMPT = """
ТЫ — ведущий аналитик и редактор Telegram-канала @aileaderuz.
Твоя аудитория: юристы, IT-предприниматели, финтех-специалисты и студенты права из Узбекистана.
Твои образцы: редакторы Forbes, РБК, The Economist — но адаптированные под Telegram.

═══════════════════════════════════
ТЕМЫ КАНАЛА (только эти, строго):
═══════════════════════════════════
1. Cyber Law & Crime: атаки, утечки данных, судебные прецеденты, приговоры хакерам
2. LegalTech & AI: ИИ-инструменты для юристов, автоматизация договоров, AI в судах
3. FinTech & Crypto Law: регулирование крипты, CBDC, SEC/ЦБ против DeFi, стейблкоины
4. Законодательство: новые законы об ИИ, кибербезопасности, персданных (UZ/ЕС/США)

═══════════════════════════════════
СТРУКТУРА ТЕКСТА (строго 3 предложения):
═══════════════════════════════════
📌 ПРЕДЛОЖЕНИЕ 1 — ФАКТ:
Кто + что + когда + где + конкретная цифра/имя/название.
✅ «7 апреля Министерство юстиции США арестовало трёх граждан России по обвинению в краже $230M через ransomware-атаки на 400 компаний.»
❌ «Произошло важное событие в сфере кибербезопасности.»

📌 ПРЕДЛОЖЕНИЕ 2 — КОНТЕКСТ:
Почему это произошло? Что предшествовало? Какова суть конфликта?
✅ «Это первое дело, где ФБР использовало блокчейн-аналитику в качестве главного доказательства — суд полностью принял цифровой след транзакций.»
❌ «Это свидетельствует о росте внимания властей к данной теме.»

📌 ПРЕДЛОЖЕНИЕ 3 — УДАР/ВЫВОД:
Что это означает для читателя? Как изменится РЫНОК, БИЗНЕС или ПРАВО?
Должно заставить читателя задуматься или предпринять действие.
✅ «Если ваша компания хранит данные клиентов без шифрования — теперь это не просто риск репутации, а прямая уголовная ответственность.»
❌ «Эксперты советуют следить за развитием событий.»

═══════════════════════════════════
ПРАВИЛА ПЕРЕВОДА НА УЗБЕКСКИЙ — КРИТИЧЕСКИ ВАЖНО:
═══════════════════════════════════
ПЕРЕВОД ДОЛЖЕН ЗВУЧАТЬ КАК РЕЧЬ ОБРАЗОВАННОГО ЖУРНАЛИСТА ИЗ ТАШКЕНТА.

❌ ЗАПРЕЩЕНО — дословная калька:
«global kiberxavfsizlik landshaftini o'zgartiradi» → неестественно
«muhim qadam qo'yildi» → штамп
«bu soha uchun jiddiy oqibatlarga olib kelishi mumkin» → канцелярит

✅ ПРАВИЛЬНО — живой узбекский:
«global kiberxavfsizlik tizimiga katta zarba berdi» → естественно
«bu qaror butun sohani o'zgartirib yuborishi aniq» → живо
«endi kompaniyalar bu masalani e'tiborsiz qoldirolmaydi» → цепляет

ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА:
- Термины (AI, blockchain, GDPR, ransomware, DeFi) НЕ переводи — оставляй как есть
- Имена людей и организаций транслитерируй: «Yustitsiya vazirligi», «Federal qidiruv byurosi (FBI)"
- Числа и даты — на узбекском: «7-aprel», «230 million dollar»
- Избегай слов-паразитов: muhim, dolzarb, shubhasiz, albatta (как вводных)

═══════════════════════════════════
СТИЛЬ И ЗАПРЕТЫ:
═══════════════════════════════════
ЗАПРЕЩЕНО в тексте:
✗ HTML-теги (<b>, <i>)
✗ Вводные слова: «Важно:», «Muhim:», «Отметим, что», «Следует отметить»
✗ Клише: «эксперты считают», «по мнению аналитиков», «в условиях глобализации»
✗ Общие фразы без конкретики
✗ Пересказ без анализа

ОБЯЗАТЕЛЬНО:
✓ Конкретные цифры, имена, даты
✓ Живой авторский голос — как будто пишешь другу-юристу
✓ Каждое предложение несёт новую информацию

ОТВЕЧАЙ СТРОГО JSON.
"""

# Keywords for pre-filter — STRICTLY niche: CyberLaw, LegalTech, FinTech, AI Legislation only
TECH_KEYWORDS = [
    # --- Cyber Law & Cybercrime ---
    "cybercrime", "cyber crime", "cyberattack", "cyber attack", "ransomware",
    "phishing", "malware", "data breach", "hacker", "hacking", "exploit",
    "cybersecurity law", "cyber law", "киберпреступ", "кибератак", "кибербезопасност",
    "утечка данных", "хакер", "ransomware", "фишинг", "вредоносн",
    "cyberjinoyat", "kiberhujum", "kiberxavfsizlik",
    # --- AI Regulation & Law ---
    "ai regulation", "ai law", "ai act", "ai policy", "artificial intelligence law",
    "ai governance", "ai ethics", "ai liability", "ai court",
    "regulation of ai", "eu ai act", "ai legislation",
    "регулирование ии", "закон об ии", "ии регулиров", "искусственный интеллект закон",
    "si qonun", "sun'iy intellekt qonun",
    # --- LegalTech ---
    "legaltech", "legal tech", "legal ai", "ai lawyer", "ai legal",
    "contract automation", "legal automation", "law firm ai", "legal chatbot",
    "юридическ", "юрист", "legaltech", "правовой ии", "суд", "судебн",
    "yurist", "huquqiy", "sud",
    # --- Data Privacy & GDPR ---
    "gdpr", "data privacy", "data protection", "privacy law", "personal data",
    "персональные данные", "защита данных", "конфиденциальность",
    "shaxsiy ma'lumot", "maxfiylik",
    # --- FinTech & Crypto Regulation ---
    "fintech", "crypto regulation", "cryptocurrency law", "blockchain law",
    "bitcoin regulation", "stablecoin", "cbdc", "digital currency law",
    "defi regulation", "sec crypto", "cftc", "crypto lawsuit",
    "цифровой сум", "цифровой рубль", "криптовалют", "крипто регулиров",
    "блокчейн закон", "финтех", "цифровая валюта",
    "raqamli so'm", "kriptovalyuta", "blokcheyn",
    # --- Legislation: Uzbekistan / EU / USA ---
    "cybersecurity strategy", "national ai policy", "ai framework",
    "cybersecurity act", "cyber resilience act",
    "стратегия кибербезопасности", "цифровизац", "цифровой узбекистан",
    "digital uzbekistan", "it-park uzbekistan", "o'zbekiston raqamli",
    "kiberhavfsizlik strategiyasi",
]

# Keywords that signal PURE political/military/geopolitical news (no tech angle)
POLITICAL_NOISE_KEYWORDS = [
    # Military & war
    "fighter jet", "warplane", "airstrike", "missile strike", "bomb", "troops",
    "military escort", "armed forces", "air force", "navy", "battalion",
    "истребител", "ракетн", "бомбардир", "военн", "армия", "войска",
    "авиаудар", "воздушный удар", "обстрел", "артиллер",
    # Pure geopolitics (people/countries clashing, no tech)
    "delegation escort", "protect delegation", "shoot down", "ceasefire",
    "peace talks", "sanctions against", "expelled diplomat", "ambassador",
    "мирные переговоры", "посол", "дипломат", "делегацию от",
    "прикрыл", "перехватил самолёт", "сопроводил самолёт",
    # Elections & domestic politics
    "election", "vote", "ballot", "parliament", "senator", "congress",
    "president signed", "prime minister met", "summit meeting",
    "выборы", "голосование", "парламент", "сенат", "президент встретил",
    # Conflicts & terrorism (without cyber angle)
    "terrorist attack", "explosion", "bombing", "hostage", "siege",
    "теракт", "взрыв", "захват заложников", "осада",
]

# Tech/law/finance keywords that RESCUE a political-looking article
# e.g. "AI regulation", "crypto ban", "cybersecurity law"
TECH_RESCUE_KEYWORDS = [
    "ai regulation", "tech regulation", "digital law", "cyber", "ai law",
    "data protection", "digital currency", "fintech", "blockchain",
    "startup", "silicon", "software", "hardware", "artificial intelligence",
    "machine learning", "data breach", "encryption", "quantum",
    "regulation of ai", "ai policy", "tech policy", "digital economy",
    "цифров", "искусственный интеллект", "кибер", "регулирование ии",
    "финтех", "блокчейн", "шифрован", "утечка данных",
]

def is_political_noise(text: str) -> bool:
    """
    Returns True if the text is pure political/military noise with no tech angle.
    Logic: has political keywords AND lacks any tech-rescue terms.
    """
    text_lower = text.lower()
    has_political = any(kw in text_lower for kw in POLITICAL_NOISE_KEYWORDS)
    if not has_political:
        return False
    # Even if political, allow through if there's a tech/law/finance angle
    has_tech_rescue = any(kw in text_lower for kw in TECH_RESCUE_KEYWORDS)
    return not has_tech_rescue

def is_tech_relevant(text: str) -> bool:
    """
    Two-stage filter:
    1. Reject pure political/military noise (even if it contains generic words like 'law', 'policy')
    2. Accept only content with tech/law/finance keywords
    """
    if is_political_noise(text):
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in TECH_KEYWORDS)

# Telegram accepts only these image formats
_ACCEPTED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


async def download_image(url: str, timeout: int = 20, min_size: int = 5000) -> bytes:
    """Downloads an image, validates it's a raster format Telegram accepts. Returns b'' on failure."""
    if not url or not url.startswith("http"):
        return b""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "image/jpeg,image/png,image/webp,image/gif,image/*",
        }
        async with httpx.AsyncClient(follow_redirects=True) as c:
            resp = await c.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()

            # Validate content type — reject SVG, HTML, PDF, etc.
            content_type = resp.headers.get("content-type", "").lower().split(";")[0].strip()
            if content_type and content_type not in _ACCEPTED_IMAGE_TYPES:
                logger.warning(f"Rejected image (bad Content-Type '{content_type}'): {url}")
                return b""

            content = resp.content
            # Too small = error page, icon, or tracker pixel (min_size is configurable)
            if len(content) < min_size:
                logger.warning(f"Rejected image (too small {len(content)} bytes, min={min_size}): {url}")
                return b""

            # Quick magic-byte check: reject SVG/HTML that bypass Content-Type
            head = content[:16].lstrip()
            if head.startswith(b"<"):
                logger.warning(f"Rejected image (looks like HTML/SVG): {url}")
                return b""

            return content
    except Exception as e:
        logger.error(f"Failed to download image {url}: {e}")
        return b""

async def resolve_photo(photo_url: str, fallback_prompt: str = "cybersecurity law digital") -> object:
    """
    ALWAYS returns an image — never None.
    Priority: og:image bytes -> Pollinations AI -> DEFAULT_IMAGE bytes
    """
    # Telegram file_id (not a URL) — use directly
    if photo_url and not photo_url.startswith("http"):
        return photo_url

    # Step 1: Try the real article og:image
    if photo_url and photo_url.startswith("http"):
        img_bytes = await download_image(photo_url, timeout=15, min_size=3000)
        if img_bytes:
            logger.info(f"Photo OK from og:image ({len(img_bytes)} bytes)")
            return img_bytes
        logger.warning(f"og:image failed: {photo_url[:60]} — trying AI fallback")

    # Step 2: Pollinations AI image (free, no API key, topical)
    try:
        ai_url = get_thematic_image(fallback_prompt)
        logger.info(f"Trying Pollinations AI image: {ai_url[:80]}")
        ai_bytes = await download_image(ai_url, timeout=30, min_size=500)
        if ai_bytes:
            logger.info(f"Pollinations AI photo OK ({len(ai_bytes)} bytes)")
            return ai_bytes
        logger.warning("Pollinations returned empty/invalid image")
    except Exception as e:
        logger.error(f"Pollinations error: {e}")

    # Step 3: Absolute last resort — static image
    logger.warning("Using DEFAULT_IMAGE as last resort")
    default_bytes = await download_image(DEFAULT_IMAGE, timeout=10, min_size=100)
    return default_bytes if default_bytes else None

async def process_and_translate(text_content: str) -> dict:
    # Limit input to avoid huge prompts
    input_text = text_content[:2500] if len(text_content) > 2500 else text_content

    def parse_gemini_json(response_text: str) -> dict:
        try:
            # Strip markdown code blocks that Gemini sometimes wraps around JSON
            text = response_text.strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
                text = re.sub(r'\s*```\s*$', '', text, flags=re.MULTILINE)
                text = text.strip()
            data = json.loads(text)
        except Exception:
            return {"error": f"JSON Decode Error: {response_text}"}
            
        emoji = data.get("emoji") or "⚡️"
        if emoji.strip() in ("🚫", "\U0001F6AB"):
            emoji = "⚡️"

        ru_header_ru = strip_artificial_words((data.get('headline_ru') or '').strip()).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        ru_header_uz = strip_artificial_words((data.get('headline_uz') or '').strip()).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        analysis_ru_raw = strip_artificial_words((data.get('analysis_ru') or '').strip())
        analysis_ru_raw = analysis_ru_raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        analysis_uz_raw = strip_artificial_words((data.get('analysis_uz') or '').strip())
        analysis_uz_raw = analysis_uz_raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        hashtags = (data.get('hashtags') or '#TechNews').strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        # --- Strict validation: reject if Gemini flagged it OR content is too thin ---
        # Now expecting 3 sentences: headline >10 chars, analysis >80 chars each
        headline_too_short = len(ru_header_ru) < 10 or len(ru_header_uz) < 10
        analysis_too_short = len(analysis_ru_raw) < 80 or len(analysis_uz_raw) < 80
        gemini_rejected = bool(data.get('reject'))

        if gemini_rejected or headline_too_short or analysis_too_short:
            reason = "Gemini reject=True" if gemini_rejected else (
                f"content too short (headline_ru={len(ru_header_ru)}, headline_uz={len(ru_header_uz)}, "
                f"analysis_ru={len(analysis_ru_raw)}, analysis_uz={len(analysis_uz_raw)})"
            )
            logger.warning(f"parse_gemini_json: auto-rejecting — {reason}")
            return {"reject": True, "ru": "", "uz": "", "title_ru": "", "image_prompt": ""}

        # RU block: emoji + bold headline + 3-sentence analysis
        ru_text = f"{emoji} <b>{ru_header_ru}</b>\n\n{analysis_ru_raw}"
        # UZ block: emoji + bold headline + 3-sentence analysis + hashtags
        uz_text = f"{emoji} <b>{ru_header_uz}</b>\n\n{analysis_uz_raw}\n\n🏷 {hashtags}"

        # Final sanity check: visible text must be substantial (raised threshold for 3-sentence format)
        ru_visible = _visible_len(ru_text)
        uz_visible = _visible_len(uz_text)
        if ru_visible < 80 or uz_visible < 80:
            logger.warning(f"parse_gemini_json: visible text too short (ru={ru_visible}, uz={uz_visible}) — auto-rejecting.")
            return {"reject": True, "ru": "", "uz": "", "title_ru": "", "image_prompt": ""}

        return {
            "ru": ru_text,
            "uz": uz_text,
            "title_ru": ru_header_ru,
            "image_prompt": (data.get('image_prompt') or 'cybersecurity law courtroom digital').replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"),
            "reject": False  # explicitly False since we passed all checks
        }

    generator_config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        response_mime_type="application/json",
        response_schema=TranslatedArticle,
        temperature=0.75,       # higher creativity = punchier, more engaging copy
        max_output_tokens=1500  # enough for 3 full sentences in both languages
    )

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await asyncio.to_thread(
                client.models.generate_content,
                model=MODEL_ID,
                contents=input_text,   # just the raw article text, no prompt mixed in
                config=generator_config
            )
            return parse_gemini_json(response.text)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "503" in err_str:
                if attempt < max_retries - 1:
                    sleep_time = 15 * (attempt + 1)
                    logger.warning(f"Gemini API limit/demand (503/429). Retry {attempt + 1}/{max_retries} in {sleep_time}s...")
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"Fallback failed after {max_retries} retries. API Error: {err_str}")
                    return {"error": f"API Error (Retries exhausted): {err_str}"}
            else:
                logger.error(f"Gemini API Error: {err_str}")
                return {"error": f"API Error: {err_str}"}

SOURCES = {
    "telegram": [
        # --- Uzbekistan & CIS tech ---
        "https://t.me/s/uzbbenelux",
        "https://t.me/s/xor_journal",
        "https://t.me/s/droidergram",
        "https://t.me/s/digest_uz",
        "https://t.me/s/exploitex",
        "https://t.me/s/pulatov_kh",
        "https://t.me/s/itpark_uz",
        # --- AI / ML ---
        "https://t.me/s/ai_machinelearning_big_data",
        "https://t.me/s/deeplearning_ru",
        "https://t.me/s/openai_ru",
        "https://t.me/s/artificial_intelligence_vc",
        # --- Fintech / Crypto ---
        "https://t.me/s/forklog",
        "https://t.me/s/fintech_ru",
        # --- Law / Legal tech ---
        "https://t.me/s/legaltech_news",
        # --- Grants ---
        "https://t.me/s/grants_and_scholarships",
    ],
    "rss": [
        # ===== AI & Machine Learning =====
        "https://techcrunch.com/feed/",
        "https://venturebeat.com/feed/",
        "https://www.artificialintelligence-news.com/feed/",
        "https://www.marktechpost.com/feed/",
        "https://www.technologyreview.com/feed/",
        "https://huggingface.co/blog/feed.xml",
        "https://hnrss.org/newest?q=AI+OR+LLM+OR+ChatGPT",
        "https://www.unite.ai/feed/",                         # Unite AI
        "https://syncedreview.com/feed/",                    # Synced AI Review
        "https://aiweekly.co/issues.rss",                    # AI Weekly digest
        # ===== Tech General =====
        "https://www.theverge.com/rss/index.xml",
        "https://feeds.arstechnica.com/arstechnica/technology-lab",
        "https://www.wired.com/feed/rss",
        "https://www.zdnet.com/topic/artificial-intelligence/rss.xml",
        "https://cnet.com/rss/news/",
        "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "https://feeds.bloomberg.com/technology/news.rss",   # Bloomberg Tech
        "https://www.reuters.com/technology/rss",            # Reuters Tech
        "https://www.businessinsider.com/tech.rss",         # Business Insider Tech
        # ===== Cybersecurity =====
        "https://exploit.media/feed/",
        "https://feeds.feedburner.com/TheHackersNews",        # The Hacker News
        "https://krebsonsecurity.com/feed/",                  # Krebs on Security
        "https://www.darkreading.com/rss.xml",               # Dark Reading
        # ===== Fintech & Finance =====
        "https://forklog.com/feed/",
        "https://www.fintechfutures.com/feed/",
        "https://www.pymnts.com/feed/",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",   # CoinDesk crypto
        "https://cointelegraph.com/rss",                     # CoinTelegraph
        "https://feeds.bloomberg.com/markets/news.rss",     # Bloomberg Markets
        "https://www.finextra.com/rss/headlines.aspx",       # FinExtra
        "https://www.bankingtech.com/feed/",                 # Banking Tech
        "https://hnrss.org/newest?q=fintech+OR+neobank+OR+crypto+funding",
        # ===== Legal & Regulation =====
        "https://iapp.org/feed/",                            # IAPP Privacy law
        "https://www.legaltech.news/feed",                   # Legaltech
        "https://www.lawfaremedia.org/feed",                 # Tech policy & law
        "https://hnrss.org/newest?q=AI+law+OR+AI+regulation+OR+data+privacy",
        "https://feeds.feedburner.com/typepad/alleywatch",   # tech law commentary
        # ===== Grants, Funding & Startup =====
        "https://hnrss.org/newest?q=startup+grant+OR+AI+grant+OR+innovation+fund",
        "https://hnrss.org/newest?q=seed+funding+OR+series+a+OR+pre-seed",
        "https://www.eu-startups.com/feed/",                 # EU Startups funding
        "https://techfundingnews.com/feed/",                 # Tech Funding News
        "https://news.crunchbase.com/feed/",                 # Crunchbase News
        # ===== Events & Conferences =====
        "https://hnrss.org/newest?q=AI+conference+OR+AI+summit+OR+tech+event",
        # ===== Russian & CIS =====
        "https://habr.com/ru/rss/all/all/",
        "https://vc.ru/rss/all",                             # vc.ru all
        "https://tjournal.ru/rss",                           # TJournal
    ]
}

DEFAULT_IMAGE = "https://telegra.ph/file/55de2abdf5e6e3d7c56dc.jpg"

def get_thematic_image(prompt: str) -> str:
    """Fallback: generates a themed AI image via Pollinations.ai (free, no API key)."""
    safe_prompt = urllib.parse.quote(prompt.strip())
    seed = random.randint(1, 99999)
    return f"https://image.pollinations.ai/prompt/{safe_prompt}?width=1280&height=720&nologo=true&seed={seed}"

async def extract_og_image(url: str) -> str:
    """Scrapes the original source URL for an OpenGraph or Twitter image."""
    if not url or not url.startswith("http"):
        return None
    try:
        # Avoid blocking by using a standard user agent
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            # Look for og:image
            og_img = soup.find("meta", property="og:image")
            if og_img and og_img.get("content"):
                return og_img["content"]
            
            # Look for twitter:image
            tw_img = soup.find("meta", property="twitter:image")
            if tw_img and tw_img.get("content"):
                return tw_img["content"]
    except Exception as e:
        logger.error(f"Failed to extract og:image from {url}: {e}")
    return None

def extract_youtube_video_id(url: str) -> str:
    """Extracts the video ID from standard YouTube URLs"""
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"youtu\.be\/([0-9A-Za-z_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

async def fetch_article_text(url: str, timeout: int = 10) -> str:
    """
    Fetches a URL and extracts readable article text:
    og:title + og:description + visible body paragraphs.
    Returns empty string on failure.
    """
    if not url or not url.startswith("http"):
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with httpx.AsyncClient(follow_redirects=True) as c:
            resp = await c.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, 'html.parser')

        parts = []

        # og:title / page title
        og_title = soup.find("meta", property="og:title")
        title_text = (og_title["content"] if og_title and og_title.get("content")
                      else (soup.title.string if soup.title else ""))
        if title_text:
            parts.append(title_text.strip())

        # og:description
        og_desc = soup.find("meta", property="og:description")
        if not og_desc:
            og_desc = soup.find("meta", attrs={"name": "description"})
        if og_desc and og_desc.get("content"):
            parts.append(og_desc["content"].strip())

        # Article body: prefer <article>, then common content selectors
        article = soup.find("article")
        if not article:
            article = soup.find(attrs={"class": re.compile(r'(article|content|post|entry|story|text)', re.I)})
        if article:
            paragraphs = article.find_all(["p", "h2", "h3"])
            body = " ".join(p.get_text(separator=" ", strip=True) for p in paragraphs)
            if body:
                parts.append(body[:2000])  # cap at 2000 chars to avoid huge prompts
        elif not parts:
            # Last resort: all visible paragraph text
            paragraphs = soup.find_all("p")
            body = " ".join(p.get_text(separator=" ", strip=True) for p in paragraphs[:20])
            if body:
                parts.append(body[:2000])

        result = "\n".join(parts).strip()
        logger.info(f"fetch_article_text extracted {len(result)} chars from {url}")
        return result
    except Exception as e:
        logger.error(f"fetch_article_text error for {url}: {e}")
        return ""

async def handle_chat_message(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str):
    await update.message.reply_chat_action(action="typing")
    chat_prompt = f"""You are a helpful, professional AI Assistant running inside the @aileaderuz Telegram news bot. Your developer is Amir.
You help the user (who is the admin) manage the tech news bot, answer their tech questions, or chat casually.
Reply in Russian. Keep your answer brief, friendly, and well formatted without using any HTML tags.

User says: {payload}"""

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await asyncio.to_thread(
                client.models.generate_content,
                model=MODEL_ID,
                contents=chat_prompt,
            )
            answer = response.text
            answer = answer.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            await update.message.reply_text(answer, parse_mode="HTML")
            return
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "503" in err_str:
                if attempt < max_retries - 1:
                    sleep_time = 15 * (attempt + 1)
                    logger.warning(f"Chat retry {attempt + 1}/{max_retries} in {sleep_time}s...")
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"Chat failed after retries: {err_str}")
                    await update.message.reply_text("Извини, нейросеть перегружена. Попробуй позже.")
                    return
            else:
                logger.error(f"Chat failed: {err_str}")
                await update.message.reply_text("Извини, я сейчас не могу ответить из-за проблем с сетью.")
                return

def fetch_latest_news():
    """
    Collect news from ALL sources and interleave them for maximum diversity.
    Sources: Telegram channels + RSS feeds (sorted by date).
    Returns a mixed list so consecutive items come from different sources.
    """
    # --- 1. Telegram Web Previews ---
    tg_buckets = []   # list of lists, one list per channel
    for tg_url in SOURCES["telegram"]:
        try:
            resp = requests.get(tg_url, timeout=10)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            messages = soup.find_all('div', class_='tgme_widget_message')
            bucket = []
            for msg in messages[-5:]:
                text_div = msg.find('div', class_='tgme_widget_message_text')
                date_a = msg.find('a', class_='tgme_widget_message_date')
                if not text_div or not date_a:
                    continue
                text = text_div.get_text(separator='\n', strip=True)
                link = date_a.get('href')
                if text and link:
                    # DON'T use Telegram channel photos: they're often unrelated
                    # (e.g. Navruz flowers photo attached to a tech article).
                    # We'll scrape og:image from the real article URL instead.
                    bucket.append({"text": text, "link": link, "photo_url": None, "published": 0, "source": "telegram"})
            if bucket:
                tg_buckets.append(bucket)
        except Exception as e:
            logger.error(f"Error scraping Telegram channel {tg_url}: {e}")

    # --- 2. RSS Feeds ---
    rss_buckets = []  # one list per feed
    for rss_url in SOURCES["rss"]:
        try:
            feed = feedparser.parse(rss_url)
            bucket = []
            for entry in feed.entries[:5]:
                link = getattr(entry, 'link', '')
                if not link:
                    continue
                title = getattr(entry, 'title', '')
                summary_html = getattr(entry, 'summary', '') if hasattr(entry, 'summary') else ''
                text_content = BeautifulSoup(summary_html, "html.parser").get_text(separator=' ', strip=True)
                if title and title not in text_content:
                    text_content = f"{title}. {text_content}".strip()
                if not text_content:
                    text_content = title

                # Publish time for date-sorting
                pub_ts = 0
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        import calendar
                        pub_ts = int(calendar.timegm(entry.published_parsed))
                    except Exception:
                        pass

                # Try to get an image hint from RSS metadata
                photo_url = None
                if hasattr(entry, 'media_content') and len(entry.media_content) > 0:
                    photo_url = entry.media_content[0].get('url') or None
                if not photo_url and summary_html:
                    img = BeautifulSoup(summary_html, "html.parser").find('img')
                    if img and img.get('src'):
                        photo_url = img['src']
                if not photo_url and hasattr(entry, 'content'):
                    for c in entry.content:
                        if c.value:
                            img = BeautifulSoup(c.value, "html.parser").find('img')
                            if img and img.get('src'):
                                photo_url = img['src']
                                break

                if link and text_content:
                    bucket.append({"text": text_content, "link": link, "photo_url": photo_url, "published": pub_ts, "source": "rss"})
            if bucket:
                # Sort bucket by freshness
                bucket.sort(key=lambda x: x["published"], reverse=True)
                rss_buckets.append(bucket)
        except Exception as e:
            logger.error(f"Error scraping RSS {rss_url}: {e}")

    # --- 3. Interleave all buckets (round-robin) for maximum source diversity ---
    # Each round picks 1 item from the next available bucket.
    # This ensures we never get 5 items from @uzbbenelux before seeing TechCrunch.
    all_buckets = tg_buckets + rss_buckets
    random.shuffle(all_buckets)   # randomize which source comes first each run
    news_items = []
    while any(all_buckets):
        for bucket in list(all_buckets):
            if bucket:
                news_items.append(bucket.pop(0))
            else:
                all_buckets.remove(bucket)

    logger.info(f"Total raw items: {len(news_items)} from {len(tg_buckets)} TG channels + {len(rss_buckets)} RSS feeds")
    return news_items

def is_daytime_tashkent() -> bool:
    """
    Returns True if current time is within working hours in Tashkent (UTC+5).
    Working hours: 09:00 – 23:00. No posts at night.
    """
    tashkent_tz = datetime.timezone(datetime.timedelta(hours=5))
    now = datetime.datetime.now(tz=tashkent_tz)
    return 9 <= now.hour < 23

async def run_aggregator_job(context: ContextTypes.DEFAULT_TYPE):
    # Night-time guard: 09:00–23:00 Tashkent (UTC+5) only
    if not is_daytime_tashkent():
        tashkent_tz = datetime.timezone(datetime.timedelta(hours=5))
        now = datetime.datetime.now(tz=tashkent_tz)
        logger.info(f"Night-time in Tashkent ({now.strftime('%H:%M')}) — aggregator skipped.")
        return

    channel_id = get_publish_channel()
    admin_id = get_admin_chat()
    
    if not channel_id:
        logger.warning("Aggregator: publish_channel not set — skipping run.")
        return
    if not admin_id:
        logger.warning("Aggregator: admin_chat not set — skipping run.")
        return

    logger.info("Running aggregator job...")

    
    # Check daily limit (max 5 per day)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articles WHERE date(timestamp) = date('now') AND text_ru IS NOT NULL AND text_ru != '' AND link NOT LIKE 'manual_%'")
    count_today = cursor.fetchone()[0]
    conn.close()
    
    if count_today >= 5:
        logger.info("Daily limit of 5 articles reached. Skipping fetch.")
        return

    news_items = await asyncio.to_thread(fetch_latest_news)
    logger.info(f"Fetched {len(news_items)} raw news items from all sources.")
    processed_count: int = 0
    MAX_PER_RUN = 1  # exactly 1 new article per hour

    for item in news_items:
        if processed_count >= MAX_PER_RUN:
            break
            
        url = item['link']
        if is_link_processed(url):
            continue

        # Dedup by title fingerprint (same story, different URL)
        raw_title = item['text'].split('\n')[0][:100]  # first line as rough title
        if is_title_processed(raw_title):
            logger.info(f"Skipping duplicate story (title match): {url}")
            save_article(url, "", "", "", title_hash=normalize_title(raw_title))  # mark URL too
            continue

        # Pre-filter: skip obviously off-topic content before even calling Gemini
        if not is_tech_relevant(item['text']):
            logger.info(f"Skipping off-topic item (pre-filter): {url}")
            save_article(url, "", "", "", title_hash=normalize_title(raw_title))
            continue
            
        logger.info(f"Processing new item: {url}")
        
        # Enrich short RSS summaries by scraping the full article
        article_text = item['text']
        if len(article_text) < 300 and url.startswith("http"):
            scraped = await fetch_article_text(url)
            if scraped and len(scraped) > 100:
                article_text = scraped
                logger.info(f"Enriched RSS item with scraped text ({len(scraped)} chars): {url}")
        
        # 1. Translate via Gemini
        translated = await process_and_translate(article_text)
        
        if not translated or "error" in translated:
            logger.warning(f"Translation failed for {url}: {translated}")
            continue

        # Skip if Gemini flagged it as off-topic OR visible text is too thin
        ru_agg_visible = _visible_len(translated.get('ru', ''))
        uz_agg_visible = _visible_len(translated.get('uz', ''))
        if translated.get('reject') or ru_agg_visible < 40 or uz_agg_visible < 40:
            logger.info(f"Aggregator: blocking post (reject={translated.get('reject')}, ru_vis={ru_agg_visible}, uz_vis={uz_agg_visible}): {url}")
            save_article(url, "", "", "")
            continue
            
        # --- Photo: only use the real og:image from the article source ---
        # If no photo found → send without image (never use AI-generated art).
        photo_url = None

        if url.startswith("http"):
            scraped_img = await extract_og_image(url)
            if scraped_img:
                photo_url = scraped_img
                logger.info(f"Using og:image: {scraped_img[:60]}")
            else:
                logger.info(f"No og:image found for {url} — will post text-only")

        # 2. Save and get article ID
        title_fingerprint = normalize_title(translated.get('title_ru', '') or raw_title)
        article_id = save_article(url, translated['uz'], translated['ru'], photo_url, title_hash=title_fingerprint)
        if article_id == -1:
            logger.warning(f"Duplicate or save error for {url}")
            continue
            
        # Format: RU block → divider → UZ block → one source line → channel
        ru_block = translated['ru']
        uz_block = translated['uz']
        source_line = f"\n\n🔗 Подробно / Batafsil: {url}" if not url.startswith("manual_") else ""
        channel_line = "\n📢 @aileaderuz"
        body = f"{ru_block}\n\n➖➖➖\n\n{uz_block}"
        footer = source_line + channel_line

        combined_caption = safe_caption(body + footer)

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
            [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
            [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
        ])

        media_type = "photo"  # aggregator always fetches photos, not videos
        final_photo = await resolve_photo(photo_url, fallback_prompt=translated.get('image_prompt', 'technology news digital'))

        try:
            await send_article_media(context, admin_id, final_photo, media_type, combined_caption, keyboard)
            processed_count += 1
            logger.info(f"Sent article {article_id} to admin for review. ({processed_count}/{MAX_PER_RUN})")
            # Small delay between posts to avoid Telegram flood
            if processed_count < MAX_PER_RUN:
                await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Failed to send to admin {admin_id}: {e}")
    
    logger.info(f"Aggregator run complete. Processed {processed_count} new articles.")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    welcome_text = (
        "👋 Welcome to the V3 Automated Uzbek News Aggregator!\n\n"
        "I will fetch news, format them via an AI Copywriter, attach photos, and generate bilingual inline buttons for your readers!\n\n"
        "To set the publication channel:\n"
        "`/set_channel @YourChannelName`\n"
        "To set yourself as the Admin (receive automated news for review):\n"
        "`/set_admin`"
    )
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def set_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = str(update.message.chat_id)
    set_admin_chat(chat_id)
    await update.message.reply_text(f"✅ Вы назначены администратором ({chat_id}). Письма на модерацию будут приходить сюда.")

async def set_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Please provide the channel ID or @username. Example: /set_channel @my_news_channel")
        return
        
    channel = context.args[0]
    set_publish_channel(channel)
    context.job_queue.run_once(run_aggregator_job, 5)
    await update.message.reply_text(f"✅ Channel set to {channel}. First run test begins in 5 seconds.")

async def manual_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"manual_post_handler triggered! chat.type={update.message.chat.type}")
    
    if update.message.chat.type != "private":
        return
    
    msg = update.message
    
    # --- Robust text extraction from all message types ---
    # 1. Plain text message
    text = getattr(msg, 'text', None) or None

    # 2. Caption (photo/video/document posts)
    if not text:
        text = getattr(msg, 'caption', None) or None

    # 3. Forwarded channel post — try forward_origin.chat title + entities
    if not text and getattr(msg, 'forward_origin', None):
        origin = msg.forward_origin
        # Some PTB versions put the post text inside a nested message object
        nested_msg = getattr(origin, 'message', None)
        if nested_msg:
            text = getattr(nested_msg, 'text', None) or getattr(nested_msg, 'caption', None) or None
        # Fallback: use channel name so Gemini at least knows where it's from
        if not text:
            chat = getattr(origin, 'chat', None)
            if chat:
                title = getattr(chat, 'title', '') or getattr(chat, 'username', '')
                text = f"Пост из канала \"{title}\" (без текста)"

    # 4. Older PTB: forward_from_chat
    if not text and getattr(msg, 'forward_from_chat', None):
        channel_title = getattr(msg.forward_from_chat, 'title', '') or getattr(msg.forward_from_chat, 'username', '')
        text = f"Пост из канала \"{channel_title}\" (без текста)"

    # 5. Any URLs in entities as last resort
    if not text and getattr(msg, 'entities', None):
        for entity in msg.entities:
            if entity.type == 'url':
                url_text = msg.text[entity.offset: entity.offset + entity.length] if msg.text else ''
                if url_text:
                    text = url_text
                    break

    logger.info(f"Extracted text: {str(text)[:60] if text else 'None'}")
    
    if not text or text.strip() == '':
        logger.info("Text still empty after all extraction attempts.")
        await msg.reply_text(
            "❗️ Не могу прочитать это сообщение — текст не найден.\n\n"
            "Попробуйте:\n"
            "• Переслать пост с текстом\n"
            "• Скопировать и вставить текст новости вручную\n"
            "• Отправить ссылку на статью"
        )
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key = 'admin_state'")
    state_row = cursor.fetchone()
    conn.close()
    
    admin_state = state_row[0] if state_row else ""
    
    if admin_state.startswith("edit_"):
        article_id = int(admin_state.split("_")[1])
        # Clear state
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM config WHERE key = 'admin_state'")
        conn.commit()
        
        # Get old article
        cursor.execute("SELECT text_ru FROM articles WHERE id = ?", (article_id,))
        article_row = cursor.fetchone()
        conn.close()
        
        if not article_row:
             await update.message.reply_text("❌ Статья не найдена.")
             return
             
        old_text = article_row[0]
        await update.message.reply_text("⏳ Переписываю новость по вашим инструкциям...")
        
        REVISION_PROMPT = f"""ТВОЯ РОЛЬ:
Ты — высококвалифицированный эксперт-аналитик, юрист в сфере IT.
Пользователь хочет изменить сгенерированную новость.
Текущий текст новости:
{old_text}

Инструкция от пользователя:
{text}

Перепиши новость, учитывая замечания пользователя.
ТВОИ ПРАВИЛА:
1. Вы должны вернуть JSON строго формата TranslatedArticle.
2. НЕ используйте HTML теги (никаких <b> или <br>). Сплошной текст, абзацы разделяйте переносами строк.
3. Полностью раскрой суть, не делай коротких "выжимок". Отрази технические/юридические аспекты.
4. Объем текста на узбекском (🇺🇿) должен быть абсолютно равен тексту на русском (🇷🇺)."""


        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await asyncio.to_thread(
                    client.models.generate_content,
                    model=MODEL_ID,
                    contents="Revise the text.",
                    config=types.GenerateContentConfig(
                        system_instruction=REVISION_PROMPT,
                        response_mime_type="application/json",
                        response_schema=TranslatedArticle,
                    )
                )
                data = json.loads(response.text)
                
                if data.get("reject"):
                    await update.message.reply_text("❌ Нейросеть отклонила текст.")
                    return
                    
                emoji = data.get("emoji", "⚡️")
                ru_head_ru = data.get("headline_ru", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                ru_head_uz = data.get("headline_uz", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                a_ru = data.get("analysis_ru", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                a_uz = data.get("analysis_uz", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                h_tags = data.get("hashtags", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                
                new_ru = f"{emoji} <b>{ru_head_ru}</b>\n\n{a_ru}"
                new_uz = f"{emoji} <b>{ru_head_uz}</b>\n\n{a_uz}\n\n🏷 {h_tags}"

                # Update DB
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("UPDATE articles SET text_ru = ?, text_uz = ? WHERE id = ?", (new_ru, new_uz, article_id))
                cursor.execute("SELECT photo_url, media_type FROM articles WHERE id = ?", (article_id,))
                row = cursor.fetchone()
                conn.commit()
                conn.close()
                
                photo_url = row[0] if row else DEFAULT_IMAGE
                media_type = row[1] if row and len(row) > 1 and row[1] else "photo"
                
                caption_ru = f"🇷🇺 <b>НОВАЯ НОВОСТЬ ДЛЯ ПУБЛИКАЦИИ:</b>\n\n{new_ru}"
                caption_ru = f"{caption_ru}\n📢 @aileaderuz"
                
                if len(caption_ru) > 4000:
                    caption_ru = caption_ru[:4000] + "..."
                    
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
                    [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
                    [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
                ])
                
                final_photo = await resolve_photo(photo_url, fallback_prompt="technology artificial intelligence news")
                await send_article_media(context, update.message.chat_id, final_photo, media_type, caption_ru, keyboard)
                break
            except Exception as e:
                err_str = str(e).replace("<", "&lt;").replace(">", "&gt;")
                if "429" in err_str or "503" in err_str:
                    if attempt < max_retries - 1:
                        sleep_time = 15 * (attempt + 1)
                        await asyncio.sleep(sleep_time)
                        continue
                    else:
                        await update.message.reply_text(f"❌ Нейросеть перегружена. Попробуйте позже.\n\nОшибка: {err_str}")
                else:
                    await update.message.reply_text(f"❌ Системная ошибка:\n\n<code>{err_str}</code>", parse_mode="HTML")
                    break
        return

    # If no state, route the intent (News submission vs Chat command)
    urls_for_intent = re.findall(r'(https?://[^\s]+)', str(text))
    # It is a news submission IF:
    # 1. Contains a link, OR
    # 2. Has media (photo/video/document), OR
    # 3. Was forwarded from somewhere, OR
    # 4. Text is very long (> 150 chars)
    is_news = (
        bool(urls_for_intent) or 
        bool(getattr(msg, 'photo', None)) or 
        bool(getattr(msg, 'video', None)) or 
        bool(getattr(msg, 'document', None)) or 
        bool(getattr(msg, 'forward_origin', None)) or
        bool(getattr(msg, 'forward_from_chat', None)) or
        len(str(text)) > 150
    )

    if not is_news:
        logger.info("Routing to Chat Command...")
        await handle_chat_message(update, context, str(text))
        return

    logger.info("Routing as News Post...")

    # --- Quick pre-filter: reject obviously off-topic content BEFORE calling Gemini ---
    # This saves API cost and avoids the confusing "processing..." + "rejected" flow.
    # We only run this if text is substantial enough to be meaningful (>30 chars).
    if len(str(text)) > 30 and not is_tech_relevant(str(text)):
        logger.info(f"manual_post_handler: pre-filter rejected (off-topic text).")
        await update.message.reply_text(
            "ℹ️ Эта новость не подходит для канала @aileaderuz.\n\n"
            "Канал публикует только:\n"
            "• ⚖️ CyberLaw & кибербезопасность\n"
            "• 🤖 LegalTech & AI-регулирование\n"
            "• 💰 FinTech & крипто-право\n"
            "• 📜 Законодательство (Узбекистан, ЕС, США)"
        )
        return

    logger.info("Replying with status message for News Post...")
    await update.message.reply_text("⏳ Обрабатываю новую (ручную) новость...")

    # --- Enrich text: for short messages that are just a URL, fetch full article content ---
    urls_in_text = re.findall(r'(https?://[^\s]+)', str(text))
    is_just_url = len(str(text).strip()) < 200 and bool(urls_in_text)

    # YouTube fast-path
    extracted_yt_id = extract_youtube_video_id(str(text))
    if extracted_yt_id:
        try:
            oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={extracted_yt_id}&format=json"
            resp = await asyncio.to_thread(requests.get, oembed_url, timeout=5)
            if resp.status_code == 200:
                yt_data = resp.json()
                yt_title = yt_data.get("title", "")
                if yt_title:
                    logger.info(f"OEmbed YouTube title: {yt_title}")
                    text = f"Видео с YouTube: {yt_title}\nСсылка: {text}"
                    is_just_url = False
        except Exception as e:
            logger.error(f"Failed to fetch YouTube title via oEmbed: {e}")

    # For all other URLs: scrape the article text
    # Detect if message is mostly just a URL (few words besides the URL)
    text_without_urls = re.sub(r'https?://[^\s]+', '', str(text)).strip()
    is_just_url = bool(urls_in_text) and len(text_without_urls) < 60

    # Always extract source URL before potentially overwriting `text`
    source_url_for_link = urls_in_text[0] if urls_in_text else ""

    if is_just_url and not extracted_yt_id and urls_in_text:
        target_url = urls_in_text[0]
        logger.info(f"Message is a bare URL, fetching article content from: {target_url}")
        scraped_text = await fetch_article_text(target_url)
        if scraped_text and len(scraped_text) > 80:
            text = scraped_text
            logger.info(f"Enriched text with scraped article ({len(text)} chars)")
        else:
            logger.warning(f"Could not scrape article text from {target_url}, using URL as context")
            text = f"Статья по ссылке: {target_url}"

    logger.info("Calling process_and_translate...")
    translated = await process_and_translate(str(text))
    logger.info(f"Translation returned. Success: {bool(translated)}")
    if not translated or "error" in translated:
        err_str = translated.get("error", "Unknown error") if translated else "Internal Fallback Error"
        err_str = str(err_str).replace("<", "&lt;").replace(">", "&gt;")
        await update.message.reply_text(f"❌ Системная ошибка ИИ.\n\nТехническая деталь: <code>{err_str}</code>\n\n(Возможно, статья слишком короткая, либо это внутренняя ошибка Gemini API)", parse_mode="HTML")
        return

    # Guard: don't continue if Gemini rejected OR if the actual visible text content is too thin.
    # IMPORTANT: check _visible_len() not just bool(str) — '⚡ <b></b>\n\n' is truthy but empty!
    ru_content = translated.get('ru', '')
    uz_content = translated.get('uz', '')
    ru_visible = _visible_len(ru_content)
    uz_visible = _visible_len(uz_content)
    logger.info(f"Visible text check: ru={ru_visible} chars, uz={uz_visible} chars, reject={translated.get('reject')}")

    if translated.get('reject') or ru_visible < 80 or uz_visible < 80:
        logger.warning(f"manual_post_handler: blocking post — reject={translated.get('reject')}, ru_vis={ru_visible}, uz_vis={uz_visible}")
        await update.message.reply_text(
            "ℹ️ Эта новость не подходит для канала @aileaderuz.\n\n"
            "Канал публикует только:\n"
            "• ⚖️ CyberLaw & кибербезопасность\n"
            "• 🤖 LegalTech & AI-регулирование\n"
            "• 💰 FinTech & крипто-право\n"
            "• 📜 Законодательство (Узбекистан, ЕС, США)"
        )
        return

    link = ""
    # Support PTB 20+ forward_origin
    if getattr(update.message, 'forward_origin', None) and getattr(update.message.forward_origin, 'type', '') == 'channel':
        chat = getattr(update.message.forward_origin, 'chat', None)
        msg_id = getattr(update.message.forward_origin, 'message_id', None)
        if chat and getattr(chat, 'username', None) and msg_id:
            link = f"https://t.me/{chat.username}/{msg_id}"
            
    # Support older PTB forward_from_chat
    if not link and getattr(update.message, 'forward_from_chat', None):
        chat = update.message.forward_from_chat
        msg_id = getattr(update.message, 'forward_from_message_id', None)
        if chat and getattr(chat, 'username', None) and msg_id:
            link = f"https://t.me/{chat.username}/{msg_id}"
            
    # If we already know the source URL from the URL-only path, use it
    if not link and source_url_for_link:
        link = source_url_for_link

    # Try URL regex on text as fallback
    if not link:
        urls = re.findall(r'(https?://[^\s]+)', str(text))
        if urls:
            link = urls[-1]
            
    if not link:
        link = f"manual_{int(time.time())}"

    media_type = "photo"
    photo_url = getattr(msg.photo[-1], 'file_id', None) if getattr(msg, 'photo', None) else None
    
    # Check if a video was sent, take its file_id directly
    if not photo_url and getattr(msg, 'video', None):
        photo_url = msg.video.file_id
        media_type = "video"
        
    # Check if image was sent as a document
    if not photo_url and getattr(msg, 'document', None) and msg.document.mime_type:
        if msg.document.mime_type.startswith('image/'):
            photo_url = msg.document.file_id
        elif msg.document.mime_type.startswith('video/'):
            photo_url = msg.document.file_id
            media_type = "video"

    # If NO media in Telegram at all, try scraping the original source link
    if not photo_url and link.startswith("http"):
        # YouTube fast-path
        yt_id = extract_youtube_video_id(link)
        if yt_id:
            logger.info(f"Detected YouTube link, returning maxres thumbnail for ID: {yt_id}")
            photo_url = f"https://i.ytimg.com/vi/{yt_id}/maxresdefault.jpg"
            media_type = "photo" # Thumbnails act as a photo block
        else:
            logger.info(f"No media found in TG message, attempting to scrape original source: {link}")
            scraped_img = await extract_og_image(link)
            if scraped_img:
                photo_url = scraped_img
        
    # Absolute fallback
    if not photo_url:
        photo_url = DEFAULT_IMAGE

    article_id = save_article(link, translated['uz'], translated['ru'], photo_url, media_type)
    if article_id == -1:
        await update.message.reply_text("❌ Ошибка при сохранении.")
        return
        
    # Format: RU block → divider → UZ block → one source line → channel
    ru_block = translated['ru']
    uz_block = translated['uz']
    source_line = f"\n\n🔗 Подробно / Batafsil: {link}" if not link.startswith("manual_") else ""
    channel_line = "\n📢 @aileaderuz"
    body = f"{ru_block}\n\n➖➖➖\n\n{uz_block}"
    footer = source_line + channel_line
    
    caption_combined = safe_caption(body + footer)
        
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
        [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
        [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
    ])
    
    try:
        final_photo = await resolve_photo(photo_url, fallback_prompt=translated.get('image_prompt', 'technology news digital'))
        await send_article_media(context, update.message.chat_id, final_photo, media_type, caption_combined, keyboard)
    except Exception as photo_err:
        logger.error(f"All photo fallbacks exhausted for manual post: {photo_err}")
        await update.message.reply_text(f"❌ Не удалось доставить сообщение: {photo_err}")

async def publish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("|")
    action = data[0]
    article_id = int(data[1])
    
    if action == "edit":
        # set admin state to editing
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('admin_state', ?)", (f"edit_{article_id}",))
        conn.commit()
        conn.close()
        await query.edit_message_caption(caption="✏️ Напишите в чат, как именно вы хотите изменить этот пост (на русском языке):", reply_markup=None)
        return
        
    if action == "cancel":
        await query.edit_message_caption(caption="❌ Отменено.", reply_markup=None)
        return
        
    if action == "pub":
        channel_id = get_publish_channel()
        if not channel_id:
            await query.edit_message_caption(caption="❌ Канал для публикации не установлен. Используйте /set_channel", reply_markup=None)
            return
            
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT link, text_uz, text_ru, photo_url, media_type FROM articles WHERE id = ?", (article_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # Handle schema updates where media_type might be None for old articles
            link, text_uz, text_ru, photo_url, media_type = row
            media_type = media_type or "photo"

            # --- Guard: don't publish articles with empty text ---
            if not text_ru or not text_uz:
                logger.error(f"Article {article_id} has empty text_ru or text_uz — aborting publish.")
                await query.edit_message_caption(caption="❌ Статья пустая — нейросеть не смогла её обработать. Отмените и попробуйте снова.", reply_markup=None)
                return

            source_line = f"\n\n🔗 Подробно / Batafsil: {link}" if not link.startswith("manual_") else ""
            channel_line = "\n📢 @aileaderuz"
            body = f"{text_ru}\n\n➖➖➖\n\n{text_uz}"
            footer = source_line + channel_line
            caption_combined = safe_caption(body + footer)

            # --- Photo: always go through resolve_photo for proper fallbacks ---
            # Step 1: if it's a Telegram file_id (not a URL), download it via Bot API
            final_photo = None
            if photo_url and not photo_url.startswith("http"):
                try:
                    tg_file = await context.bot.get_file(photo_url)
                    final_photo = bytes(await tg_file.download_as_bytearray())
                    logger.info(f"Downloaded Telegram file_id for publish ({len(final_photo)} bytes)")
                except Exception as e:
                    logger.warning(f"Telegram file_id download failed: {e} — will use resolve_photo fallback")

            # Step 2: resolve_photo handles HTTP URLs, Pollinations AI, and DEFAULT_IMAGE
            if not final_photo:
                final_photo = await resolve_photo(photo_url, fallback_prompt="technology law digital news")

            try:
                await send_article_media(context, channel_id, final_photo, media_type, caption_combined)
                confirmation = safe_caption(f"✅ Опубликовано в канал!\n\n{caption_combined}", limit=900)
                await query.edit_message_caption(caption=confirmation, reply_markup=None, parse_mode="HTML")
            except Exception as photo_err:
                logger.error(f"publish_callback send failed: {photo_err}")
                await query.edit_message_caption(caption=f"❌ Ошибка публикации: {photo_err}", reply_markup=None)

async def fetch_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually trigger the aggregator job (for testing and debugging)."""
    admin_id = get_admin_chat()
    if str(update.message.chat_id) != str(admin_id):
        return
    channel_id = get_publish_channel()
    if not channel_id:
        await update.message.reply_text("❌ Канал не установлен. Используйте /set_channel @channel")
        return
    await update.message.reply_text("⏳ Запускаю поиск новостей...")
    try:
        await run_aggregator_job(context)
        await update.message.reply_text("✅ Готово! Если новостей нет — все уже обработаны или источники пусты.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current bot configuration."""
    admin_id = get_admin_chat()
    channel_id = get_publish_channel()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articles WHERE date(timestamp) = date('now') AND text_ru != ''")
    today_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articles")
    total_count = cursor.fetchone()[0]
    conn.close()
    status_text = (
        f"📊 <b>Статус бота:</b>\n\n"
        f"👤 Admin ID: <code>{admin_id or '❌ не задан'}</code>\n"
        f"📢 Канал: <code>{channel_id or '❌ не задан'}</code>\n"
        f"📰 Новостей сегодня: <b>{today_count}</b>\n"
        f"📦 Всего в БД: <b>{total_count}</b>\n\n"
        f"🟢 Режим: вручную (автопоиск отключён)\n"
        f"ℹ️ Перешлите новость прямо в этот чат, бот обработает и готовит публикацию."
    )
    await update.message.reply_text(status_text, parse_mode="HTML")

def main() -> None:
    init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("set_channel", set_channel_command))
    app.add_handler(CommandHandler("set_admin", set_admin_command))
    app.add_handler(CommandHandler("fetch", fetch_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CallbackQueryHandler(publish_callback, pattern=r"^(pub|cancel|edit)\|.*"))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, manual_post_handler))

    # AUTO-AGGREGATOR DISABLED — manual mode only.
    # The admin sends news directly to the bot; no background fetching.
    # To re-enable: uncomment the line below.
    # job_queue.run_repeating(run_aggregator_job, interval=7200, first=60)

    logger.info("Bot is running V7 (Hook style, /fetch, /status)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()