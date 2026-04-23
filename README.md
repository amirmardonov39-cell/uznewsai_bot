
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
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
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
    emoji: str = Field(description="One tight, relevant emoji, e.g. ⚡️")
    headline_ru: str = Field(description="Catchy, natural headline in Russian, max 8 words. NO colons (без двоеточий).")
    headline_uz: str = Field(description="Catchy, natural headline in Uzbek, max 8 words. NO colons (без двоеточий).")
    analysis_ru: str = Field(description="STRICTLY 1 sentence IN RUSSIAN with key facts. Write naturally like a human and finish the thought completely. Max 140 characters.")
    analysis_uz: str = Field(description="STRICTLY 1 sentence IN UZBEK with key facts. Write naturally like a human and finish the thought completely. Max 140 characters.")
    hashtags: str = Field(description="1-2 narrow tags like #CyberLaw #AI")
    image_prompt: str = Field(description="Short English prompt for AI image")

def strip_artificial_words(text: str) -> str:
    """Remove artificial marker words like 'Важно:' and 'Muhim:' from text."""
    text = re.sub(r'\bВажно:\s*', '', text)
    text = re.sub(r'\bMuhim:\s*', '', text)
    text = re.sub(r'\bВАЖНО:\s*', '', text)
    text = re.sub(r'\bMUHIM:\s*', '', text)
    return text

def force_one_sentence(text: str, max_chars: int = 140) -> str:
    """
    HARD enforcement: always returns only the FIRST sentence.
    No matter what the AI returns — we take exactly one sentence.
    """
    if not text:
        return text
    text = text.strip()
    # Split on first sentence-ending punctuation followed by space or end
    match = re.search(r'([.!?»\"\')])(\s|$)', text)
    if match:
        first = text[:match.start() + 1].strip()
    else:
        # No punctuation found — take everything up to max_chars
        first = text
    # Final hard char limit
    if len(first) > max_chars:
        # cut at last space within limit
        cut = first[:max_chars].rsplit(' ', 1)[0].rstrip()
        if cut and cut[-1] not in '.!?':
            cut += '.'
        first = cut
    return first

def truncate_to_sentence(text: str, limit: int) -> str:
    """
    If text exceeds `limit` chars, cuts at the last complete sentence
    (ending with . ! ?) within the limit. Never leaves an ellipsis —
    the result always ends with proper punctuation.
    """
    if len(text) <= limit:
        return text
    chunk = text[:limit]
    # Find the last sentence-ending punctuation
    match = re.search(r'[.!?][^.!?]*$', chunk)
    if match:
        return chunk[:match.start() + 1]  # include the punctuation
    # No sentence boundary found — cut at last space and add a period
    cut = chunk.rsplit(' ', 1)[0].rstrip()
    if cut and cut[-1] not in '.!?':
        cut += '.'
    return cut

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
        body_plain = truncate_to_sentence(body_plain, body_limit)

    return body_plain + footer

async def send_article_media(context, chat_id, final_photo, media_type, caption_combined, keyboard=None):
    """Sends photo/video with caption. Falls back to text-only if any photo step fails."""

    async def _send_photo(photo):
        if not photo:  # None or empty bytes — don't even try
            return None
            
        # Wrap raw bytes in InputFile with a filename so Telegram API accepts it
        if isinstance(photo, bytes):
            filename = "media.jpg"
            if media_type == "video": filename = "media.mp4"
            elif media_type == "document": filename = "document.file"
            elif media_type == "audio": filename = "audio.mp3"
            elif media_type == "voice": filename = "voice.ogg"
            elif media_type == "animation": filename = "animation.mp4"
            photo_to_send = InputFile(photo, filename=filename)
        else:
            photo_to_send = photo
            
        safe_cap = safe_caption(caption_combined, limit=1024)
        
        # Dynamically dispatch to the right Telegram method based on media_type
        if media_type == "video":
            return await context.bot.send_video(chat_id=chat_id, video=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")
        elif media_type == "document":
            return await context.bot.send_document(chat_id=chat_id, document=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")
        elif media_type == "audio":
            return await context.bot.send_audio(chat_id=chat_id, audio=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")
        elif media_type == "voice":
            return await context.bot.send_voice(chat_id=chat_id, voice=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")
        elif media_type == "animation":
            return await context.bot.send_animation(chat_id=chat_id, animation=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")
        else:
            return await context.bot.send_photo(chat_id=chat_id, photo=photo_to_send, caption=safe_cap, reply_markup=keyboard, parse_mode="HTML")

    async def _send_text_only():
        """Always works — disable_web_page_preview prevents Telegram from fetching URLs in caption."""
        safe_cap = safe_caption(caption_combined, limit=4000)
        return await context.bot.send_message(
            chat_id=chat_id,
            text=safe_cap,
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=False,
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

SYSTEM_PROMPT = """Role: Ты — эксперт-аналитик в области LegalTech, киберправа и финтеха. Твоя задача — формировать краткие новостные сводки по открытым источникам.

Task: Выдели самое главное из текста. Пиши только ключевые факты про суть новости, как живой человек, естественно и грамотно.
Темы:
- Cyber Law & Crimes: Киберпреступность, регулирование ИИ.
- LegalTech & AI: Автоматизация права, ИИ для юристов.
- FinTech & Law: Криптовалюты, цифровой сум.

Constraints:
1. Выжимай суть (факт + интрига) СТРОГО в 1 (одно) предложение. Дописывай мысль до конца, не обрывай текст. analysis_ru пиши СТРОГО на русском, analysis_uz СТРОГО на узбекском. Максимум 200 символов на каждый язык.
2. Придумай привлекательный, естественный заголовок (headline_ru, headline_uz) до 10 слов. КАТЕГОРИЧЕСКИ ЗАПРЕЩАЕТСЯ использовать двоеточия (:) в заголовках.
3. Узбекский текст (headline_uz и analysis_uz) ДОЛЖЕН БЫТЬ БЕЗУПРЕЧНЫМ по смыслу и стилистике. Используй абсолютно естественный, грамотный и профессиональный узбекский язык (sof va ravon o'zbek tilida, xatosiz). Правильно переводи термины (ЦБ -> Markaziy bank, НАПП -> NAPP). Сохраняй полный смысл оригинальной новости. Не должно быть корявого машинного перевода (masalan, "so'ramasdan", "ijaraga oldi" ўрнига чиройлироқ сўзлар топинг).

ОТВЕЧАЙ СТРОГО JSON. Никаких HTML тегов внутри полей."""

# Keywords for pre-filter: AI, tech, fintech, law, grants, events
TECH_KEYWORDS = [
    # --- AI / ML ---
    "ai", "artificial intelligence", "machine learning", "deep learning",
    "neural", "llm", "gpt", "agi", "openai", "chatgpt", "deepseek",
    "anthropic", "claude", "gemini", "copilot", "mistral", "llama",
    "нейро", "искусственный интеллект", "ии", "генеративн",
    # --- Tech general ---
    "tech", "software", "hardware", "startup", "robot", "automation",
    "cloud", "data", "algorithm", "gpu", "chip", "semiconductor",
    "open source", "api", "model", "benchmark", "programming", "developer",
    "digital", "internet", "5g", "quantum", "cybersecurity", "hack",
    "технолог", "программ", "кибербезопасность", "разработ", "цифров",
    "стартап", "приложени", "платформ",
    # --- Big Tech ---
    "apple", "google", "microsoft", "meta", "nvidia", "tesla",
    "amazon", "openai", "huawei", "samsung",
    # --- Fintech / Crypto ---
    "fintech", "blockchain", "crypto", "bitcoin", "ethereum",
    "defi", "nft", "cbdc", "цифровой рубль", "цифровой сум",
    "payment", "banking", "neobank", "invest", "venture", "ipo",
    "финтех", "блокчейн", "криптовалют", "инвестиц",
    # --- Legal / Regulation ---
    "regulation", "policy", "law", "legal", "compliance", "gdpr",
    "legislation", "court", "lawsuit", "fine", "ban", "privacy",
    "закон", "право", "регулиров", "суд", "штраф", "юридич",
    "персональные данные", "защита данных", "qonun", "huquq",
    # --- Grants & Funding ---
    "grant", "funding", "гранты", "грант", "финансиров",
    "innovation fund", "инновационный фонд", "it-park",
    "fellowship", "scholarship", "стипендия", "конкурс",
    "accelerator", "incubator", "акселератор", "инкубатор",
    "seed", "series a", "series b", "pre-seed",
    # --- Events & Conferences ---
    "conference", "summit", "forum", "expo", "exhibition", "hackathon",
    "workshop", "webinar", "meetup", "gitex", "ces", "web summit",
    "конференц", "выставка", "форум", "хакатон", "вебинар",
    "мероприятие", "event", "techcrunch disrupt", "innovate",
    # --- Uzbekistan / CIS specific ---
    "uzbekistan", "узбекистан", "digital uzbekistan", "цифровизац",
    "silicon", "hub", "it park", "astana hub", "skolkovo",
    "ташкент", "tashkent", "самарканд", "samarkand",
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

async def download_image(url: str, timeout: int = 20, min_size: int = 1000) -> bytes:
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

async def resolve_photo(photo_url: str, fallback_prompt: str = "technology news digital") -> object:
    """
    Download and validate a photo for Telegram.
    Returns: bytes (valid image) | Telegram file_id str | None (no image — send text-only)
    NEVER uses AI-generated images. NEVER returns a raw http URL.
    """
    if not photo_url:
        photo_url = get_thematic_image(fallback_prompt)

    if not photo_url.startswith("http"):
        return photo_url  # Telegram file_id — valid as-is

    # Download and validate the real image
    img_bytes = await download_image(photo_url)
    if img_bytes:
        return img_bytes

    logger.warning(f"Image download failed or rejected: {photo_url} — attempting fallback")
    fallback_url = get_thematic_image(fallback_prompt)
    if photo_url != fallback_url:
        img_bytes_fallback = await download_image(fallback_url)
        if img_bytes_fallback:
            return img_bytes_fallback

    return None

async def process_and_translate(text_content: str) -> dict:
    # Limit input to avoid huge prompts
    input_text = text_content[:2500] if len(text_content) > 2500 else text_content

    def parse_gemini_json(response_text: str) -> dict:
        try:
            data = json.loads(response_text)
        except Exception:
            return {"error": f"JSON Decode Error: {response_text}"}
            
        emoji = data.get("emoji") or "⚡️"
        if emoji.strip() in ("🚫", "\U0001F6AB"):
            emoji = "⚡️"

        ru_header_ru = strip_artificial_words((data.get('headline_ru') or '').strip()).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(":", " -")
        ru_header_uz = strip_artificial_words((data.get('headline_uz') or '').strip()).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(":", " -")

        analysis_ru_raw = strip_artificial_words((data.get('analysis_ru') or '').strip())
        analysis_ru_raw = analysis_ru_raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        analysis_ru_raw = force_one_sentence(analysis_ru_raw, 250)

        analysis_uz_raw = strip_artificial_words((data.get('analysis_uz') or '').strip())
        analysis_uz_raw = analysis_uz_raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        analysis_uz_raw = force_one_sentence(analysis_uz_raw, 250)

        hashtags = (data.get('hashtags') or '#TechNews').strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        ru_text = f"{emoji} <b>{ru_header_ru}</b>\n{analysis_ru_raw}"
        uz_text = f"{emoji} <b>{ru_header_uz}</b>\n{analysis_uz_raw}\n\n🏷 {hashtags}"

        if not ru_header_ru and not analysis_ru_raw:
            return {"error": "AI is refusing to process this text (possibly due to safety filters or insufficient content)."}

        logger.info(f"Final RU analysis ({len(analysis_ru_raw)} chars): {analysis_ru_raw}")

        return {
            "ru": ru_text,
            "uz": uz_text,
            "title_ru": ru_header_ru,
            "image_prompt": (data.get('image_prompt') or 'digital technology ai').replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        }

    generator_config = types.GenerateContentConfig(
        # system_instruction keeps the role separate from the news content
        system_instruction=SYSTEM_PROMPT,
        response_mime_type="application/json",
        response_schema=TranslatedArticle,
        temperature=0.65,       # slightly more creative = punchier hooks
        max_output_tokens=300   # physically impossible to write a novel with 300 tokens
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

DEFAULT_IMAGE = "https://image.pollinations.ai/prompt/technology%20news%20digital?width=1280&height=720&nologo=true"

def get_thematic_image(prompt: str) -> str:
    """Fallback if no original image is found. Generates an AI image via Pollinations.ai"""
    safe_prompt = urllib.parse.quote(prompt.strip())
    # Free, instant AI image generation without API key
    return f"https://image.pollinations.ai/prompt/{safe_prompt}?width=1280&height=720&nologo=true"

async def extract_og_image(url: str) -> str:
    """Scrapes the original source URL for an OpenGraph or Twitter image."""
    if not url or not url.startswith("http"):
        return None
    try:
        # Avoid blocking by using a standard user agent
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }
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

async def run_aggregator_job(context: ContextTypes.DEFAULT_TYPE):
    channel_id = get_publish_channel()
    admin_id = get_admin_chat()
    
    if not channel_id:
        logger.warning("Aggregator: publish_channel not set — skipping run.")
        return
    if not admin_id:
        logger.warning("Aggregator: admin_chat not set — skipping run.")
        return

    logger.info("Running aggregator job...")
    
    # Check daily limit (max 30 per day)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articles WHERE date(timestamp) = date('now') AND text_ru IS NOT NULL AND text_ru != '' AND link NOT LIKE 'manual_%'")
    count_today = cursor.fetchone()[0]
    conn.close()
    
    if count_today >= 30:
        logger.info("Daily limit of 30 articles reached. Skipping fetch.")
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

        # Skip if Gemini flagged it as off-topic (title starts with 🚫)
        if translated.get('title_ru', '').startswith('🚫'):
            logger.info(f"Gemini flagged item as off-topic: {url}")
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
            
        # 3. Send preview to Admin for review
        body = f"{translated['ru']}\n\n{translated['uz']}"
        footer = ""
        if not url.startswith("manual_"):
            footer += f"\n\n🔗 Подробно / Batafsil: {url}"
        footer += "\n📢 @aileaderuz"

        combined_caption = body + footer

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
                ru_head_ru = data.get("headline_ru", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(":", " -")
                ru_head_uz = data.get("headline_uz", "").strip().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(":", " -")
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
                
                photo_url = row[0] if row else None
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
    # 2. Has media (photo/video/document/audio/voice/animation), OR
    # 3. Was forwarded from somewhere, OR
    # 4. Text is very long (> 150 chars)
    is_news = (
        bool(urls_for_intent) or 
        bool(getattr(msg, 'photo', None)) or 
        bool(getattr(msg, 'video', None)) or 
        bool(getattr(msg, 'document', None)) or 
        bool(getattr(msg, 'audio', None)) or 
        bool(getattr(msg, 'voice', None)) or 
        bool(getattr(msg, 'animation', None)) or 
        bool(getattr(msg, 'forward_origin', None)) or
        bool(getattr(msg, 'forward_from_chat', None)) or
        len(str(text)) > 150
    )

    if not is_news:
        logger.info("Routing to Chat Command...")
        await handle_chat_message(update, context, str(text))
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
        
    # --- Advanced link extraction for manual posts ---
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

    # Check text_link entities first (hidden hyperlinks)
    entities = getattr(msg, 'entities', None) or getattr(msg, 'caption_entities', None) or []
    if not link and entities:
        for entity in entities:
            if entity.type == 'text_link' and entity.url:
                link = entity.url
                break
    
    # Try URL regex on text as fallback
    if not link:
        urls = re.findall(r'(https?://[^\s]+)', str(text))
        if urls:
            link = urls[-1]
            
    if not link:
        link = f"manual_{int(time.time())}"

    media_type = "photo"
    photo_url = None
    
    if getattr(msg, 'photo', None):
        photo_url = msg.photo[-1].file_id
        media_type = "photo"
    elif getattr(msg, 'video', None):
        photo_url = msg.video.file_id
        media_type = "video"
    elif getattr(msg, 'animation', None):
        photo_url = msg.animation.file_id
        media_type = "animation"
    elif getattr(msg, 'document', None):
        photo_url = msg.document.file_id
        # Let's not try to be smart with mime_type. If it's a document, it's a document.
        media_type = "document"
    elif getattr(msg, 'audio', None):
        photo_url = msg.audio.file_id
        media_type = "audio"
    elif getattr(msg, 'voice', None):
        photo_url = msg.voice.file_id
        media_type = "voice"

    # If NO media in Telegram at all, try scraping the original source link
    if not photo_url and link.startswith("http"):
        # YouTube fast-path
        yt_id = extract_youtube_video_id(link)
        if yt_id:
            logger.info(f"Detected YouTube link, returning maxres thumbnail for ID: {yt_id}")
            photo_url = f"https://i.ytimg.com/vi/{yt_id}/maxresdefault.jpg"
            media_type = "photo" # Thumbnails act as a photo block
        else:
            target_photo_url = source_url_for_link if (source_url_for_link and 't.me' in link) else link
            logger.info(f"No media found in TG message, attempting to scrape original source: {target_photo_url}")
            scraped_img = await extract_og_image(target_photo_url)
            if scraped_img:
                photo_url = scraped_img
        
    # Absolute fallback
    if not photo_url:
        photo_url = DEFAULT_IMAGE

    article_id = save_article(link, translated['uz'], translated['ru'], photo_url, media_type)
    if article_id == -1:
        await update.message.reply_text("❌ Ошибка при сохранении.")
        return
        
    body = f"{translated['ru']}\n\n{translated['uz']}"
    footer = ""
    if not link.startswith("manual_"):
        footer += f"\n\n🔗 Подробно / Batafsil: {link}"
    footer += "\n📢 @aileaderuz"
    
    caption_combined = body + footer
        
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
            body = f"{text_ru}\n\n{text_uz}"
            footer = ""
            if not link.startswith("manual_"):
                footer += f"\n\n🔗 Подробно / Batafsil: {link}"
            footer += "\n📢 @aileaderuz"
            
            pass # no truncation
            caption_combined = body + footer
                
            img_bytes = None
            if photo_url and photo_url.startswith("http"):
                try:
                    img_bytes = await download_image(photo_url)
                except Exception as e:
                    logger.error(f"Image db download failed: {e}")
            
            try:
                final_photo = img_bytes if img_bytes else (photo_url if photo_url else None)
                await send_article_media(context, channel_id, final_photo, media_type, caption_combined)
                await query.edit_message_caption(caption=f"✅ Опубликовано в канал!\n\n{caption_combined}", reply_markup=None, parse_mode="HTML")
            except Exception as photo_err:
                if "Message is not modified" in str(photo_err):
                    return
                if "Can't use file of type" in str(photo_err) and photo_url and not photo_url.startswith("http"):
                    try:
                        tg_file = await context.bot.get_file(photo_url)
                        downloaded_bytes = bytes(await tg_file.download_as_bytearray())
                        await send_article_media(context, channel_id, downloaded_bytes, media_type, caption_combined)
                        await query.edit_message_caption(caption=f"✅ Опубликовано в канал! (через обход Telegram API)\n\n{caption_combined}", reply_markup=None, parse_mode="HTML")
                        return
                    except Exception as dl_err:
                        logger.error(f"Fallback publish download failed: {dl_err}")
                        if "Message is not modified" in str(dl_err):
                            return
                        
                try:
                    await query.edit_message_caption(caption=f"❌ Ошибка публикации: {photo_err}", reply_markup=None)
                except Exception:
                    pass

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
        f"⏰ Автопоиск каждый час\n"
        f"🔧 /fetch — запустить поиск сейчас"
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

    job_queue = app.job_queue
    job_queue.run_repeating(run_aggregator_job, interval=3600, first=30)

    logger.info("Bot is running V7 (Hook style, /fetch, /status)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
