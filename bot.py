
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

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.error("Missing TELEGRAM_TOKEN or GEMINI_API_KEY.")
    exit(1)

# Initialize Gemini Client
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = "gemini-2.5-flash"

class TranslatedArticle(BaseModel):
    emoji: str = Field(description="One relevant emoji, max 2 chars", default="📰")
    title_ru: str = Field(description="Catchy title in Russian", default="")
    title_uz: str = Field(description="Catchy title in Uzbek", default="")
    p1_ru: str = Field(description="Paragraph 1 (Headline/Core). Must start exactly with '🚀 '", default="")
    p1_uz: str = Field(description="Paragraph 1 (Headline/Core). Must start exactly with '🚀 '", default="")
    p2_ru: str = Field(description="Paragraph 2 (Context/Why it matters). Must start exactly with '💡 '", default="")
    p2_uz: str = Field(description="Paragraph 2 (Context/Why it matters). Must start exactly with '💡 '", default="")
    p3_ru: str = Field(description="Paragraph 3 (Takeaway/Conclusion). Must start exactly with '📌 '", default="")
    p3_uz: str = Field(description="Paragraph 3 (Takeaway/Conclusion). Must start exactly with '📌 '", default="")
    image_prompt: str = Field(description="Short English prompt for AI image (max 10 words, keywords only, e.g. 'fintech artificial intelligence bubble')", default="digital technology artificial intelligence")

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
    # Safely attach new column for video support backwards compatibility
    try:
        cursor.execute("ALTER TABLE articles ADD COLUMN media_type TEXT DEFAULT 'photo'")
    except sqlite3.OperationalError:
        pass
        
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

def save_article(link: str, text_uz: str, text_ru: str, photo_url: str, media_type: str = "photo") -> int:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO articles (link, text_uz, text_ru, photo_url, media_type)
            VALUES (?, ?, ?, ?, ?)
        """, (link, text_uz, text_ru, photo_url, media_type))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return -1
    finally:
        conn.close()

SYSTEM_PROMPT = """You are a top-tier global tech analyst and bilingual copywriter (Uzbek & Russian) working for deep trust tech.
1. Provide deep insights, not just translation. Write in Official Russian and Native-level Official Uzbek (literary, flawless grammar).
2. COMBINED LENGTH LIMIT: Keep each language translation STRICTLY UNDER 400 characters total. It is CRITICAL to fit both languages together in one Telegram Post (max 1024 chars).
3. Do NOT include HTML tags in your text.
4. Always provide exactly 3 distinct very short paragraphs per language following this strict structure:
   - p1: 🚀 Суть (Core news/Headline)
   - p2: 💡 Контекст (Why it matters globally/locally)
   - p3: 📌 Итог (Actionable takeaway or future impact)
5. Generate a short, descriptive `image_prompt` in ENGLISH (max 10 keywords).
6. This channel is STRICTLY about: Artificial Intelligence, Machine Learning, Technology, Programming, Cybersecurity, Startups, and Tech policy.
7. If the text is completely unrelated to these topics, YOU MUST SET THE EMOJI FIELD EXACTLY TO: 🚫"""

# Keywords that MUST be present for RSS items (at least one) to pass pre-filter
TECH_KEYWORDS = [
    "ai", "artificial intelligence", "machine learning", "deep learning",
    "neural", "llm", "gpt", "tech", "software", "hardware", "startup",
    "cybersecurity", "security", "hack", "programming", "developer",
    "robot", "automation", "cloud", "data", "algorithm", "gpu", "chip",
    "semiconductor", "open source", "api", "model", "benchmark",
    "digital", "internet", "broadband", "5g", "quantum", "crypto",
    "blockchain", "нейро", "искусственный интеллект", "технолог",
    "программ", "кибербезопасность", "разработ", "цифров", "стартап",
    "openai", "chatgpt", "deepseek", "anthropic", "claude", "agi",
    "fintech", "invest", "venture", "innovation", "regulation", "policy",
    "apple", "google", "microsoft", "meta", "nvidia", "tesla"
]

def is_tech_relevant(text: str) -> bool:
    """Simple keyword pre-filter: returns True if the text looks tech-related."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in TECH_KEYWORDS)

async def download_image(url: str, timeout: int = 30) -> bytes:
    if not url or not url.startswith("http"):
        return b""
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            content = resp.content
            if len(content) < 1000:  # Too small = not a real image
                raise ValueError(f"Image too small ({len(content)} bytes), likely an error page")
            return content
    except Exception as e:
        logger.error(f"Failed to download image {url}: {e}")
        return b""

def process_and_translate(text_content: str) -> dict:
    prompt = f"{SYSTEM_PROMPT}\n\nText to process:\n{text_content}"
    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=TranslatedArticle,
                temperature=0.3
            )
        )
        data = json.loads(response.text)
            
        emoji = data.get("emoji", "📰")
        
        ru_paras = [data.get(k, '').strip() for k in ['p1_ru', 'p2_ru', 'p3_ru'] if data.get(k, '').strip()]
        ru_text = f"{emoji} {data.get('title_ru', '').strip()}\n\n" + "\n\n".join(ru_paras)
        
        uz_paras = [data.get(k, '').strip() for k in ['p1_uz', 'p2_uz', 'p3_uz'] if data.get(k, '').strip()]
        uz_text = f"{emoji} {data.get('title_uz', '').strip()}\n\n" + "\n\n".join(uz_paras)

        return {
            "ru": ru_text.strip(),
            "uz": uz_text.strip(),
            "title_ru": data.get('title_ru', 'News').strip(),
            "image_prompt": data.get('image_prompt', 'digital technology ai')
        }
    except Exception as e:
        if "429" in str(e):
            logger.warning("Gemini 429 Quota Exceeded. Sleeping 15s before retry...")
            time.sleep(15)
            try:
                response = client.models.generate_content(
                    model=MODEL_ID,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=TranslatedArticle,
                        temperature=0.3
                    )
                )
                data = json.loads(response.text)
                    
                emoji = data.get("emoji", "📰")
                
                ru_paras = [data.get(k, '').strip() for k in ['p1_ru', 'p2_ru', 'p3_ru'] if data.get(k, '').strip()]
                ru_text = f"{emoji} {data.get('title_ru', '').strip()}\n\n" + "\n\n".join(ru_paras)
                
                uz_paras = [data.get(k, '').strip() for k in ['p1_uz', 'p2_uz', 'p3_uz'] if data.get(k, '').strip()]
                uz_text = f"{emoji} {data.get('title_uz', '').strip()}\n\n" + "\n\n".join(uz_paras)
        
                return {
                    "ru": ru_text.strip(),
                    "uz": uz_text.strip(),
                    "title_ru": data.get('title_ru', 'News').strip(),
                    "image_prompt": data.get('image_prompt', 'digital technology ai')
                }
            except Exception as e2:
                logger.error(f"Fallback failed. API Error: {e2}")
        else:
            logger.error(f"Gemini API Error: {e}")
        return None

SOURCES = {
    "telegram": [
        "https://t.me/s/uzbbenelux",
        "https://t.me/s/xor_journal",
        "https://t.me/s/droidergram",
        "https://t.me/s/ai_machinelearning_big_data",
        "https://t.me/s/deeplearning_ru",
        "https://t.me/s/digest_uz",
        "https://t.me/s/exploitex",
        "https://t.me/s/pulatov_kh"
    ],
    "rss": [
        "https://habr.com/ru/rss/all/all/",
        "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "https://feeds.arstechnica.com/arstechnica/technology-lab",
        "https://www.theverge.com/rss/index.xml",
        "https://techcrunch.com/feed/",
        "https://venturebeat.com/feed/",
        "https://www.wired.com/feed/rss",
        "https://exploit.media/feed/",
        "https://forklog.com/feed/",
        "https://hnrss.org/newest?q=AI",          # HackerNews AI filtered
        "https://www.artificialintelligence-news.com/feed/", # AI News
        "https://www.technologyreview.com/feed/", # MIT Tech Review
        "https://www.zdnet.com/topic/artificial-intelligence/rss.xml",
        "https://www.marktechpost.com/feed/",     # MarkTechPost (AI Research)
        "https://cnet.com/rss/news/"
    ]
}

DEFAULT_IMAGE = "https://telegra.ph/file/55de2abdf5e6e3d7c56dc.jpg"

def get_thematic_image(prompt: str) -> str:
    """Fallback if no original image is found."""
    return DEFAULT_IMAGE

def extract_og_image(url: str) -> str:
    """Scrapes the original source URL for an OpenGraph or Twitter image."""
    if not url or not url.startswith("http"):
        return None
    try:
        # Avoid blocking by using a standard user agent
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=5)
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

def fetch_latest_news():
    news_items = []
    
    # 1. Telegram Web Previews
    for tg_url in SOURCES["telegram"]:
        try:
            resp = requests.get(tg_url, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                messages = soup.find_all('div', class_='tgme_widget_message')
                for msg in messages[-5:]:
                    text_div = msg.find('div', class_='tgme_widget_message_text')
                    date_a = msg.find('a', class_='tgme_widget_message_date')
                    
                    if not text_div or not date_a:
                        continue
                        
                    text = text_div.get_text(separator='\n', strip=True)
                    link = date_a.get('href')
                    
                    # Try fetching image from photo preview
                    photo_url = DEFAULT_IMAGE
                    photo_wrap = msg.find('a', class_='tgme_widget_message_photo_wrap')
                    if photo_wrap and 'style' in photo_wrap.attrs:
                        style = photo_wrap['style']
                        if "background-image:url('" in style:
                            s = style.find("background-image:url('") + 22
                            e = style.find("')", s)
                            extracted = style[s:e]
                            if extracted:
                                photo_url = extracted
                                
                    # Try fetching image from video thumbnail if no photo
                    if photo_url == DEFAULT_IMAGE:
                        video_wrap = msg.find('i', class_='tgme_widget_message_video_thumb')
                        if video_wrap and 'style' in video_wrap.attrs:
                            style = video_wrap['style']
                            if "background-image:url('" in style:
                                s = style.find("background-image:url('") + 22
                                e = style.find("')", s)
                                extracted = style[s:e]
                                if extracted:
                                    photo_url = extracted
                    
                    if text and link:
                        news_items.append({"text": text, "link": link, "photo_url": photo_url})
        except Exception as e:
            logger.error(f"Error scraping Telegram channel {tg_url}: {e}")

    # 2. RSS Feeds
    for rss_url in SOURCES["rss"]:
        try:
            feed = feedparser.parse(rss_url)
            for entry in feed.entries[:2]: 
                text_content = BeautifulSoup(entry.summary, "html.parser").get_text(separator=' ', strip=True) if hasattr(entry, 'summary') else ""
                link = getattr(entry, 'link', '')
                
                photo_url = DEFAULT_IMAGE
                if hasattr(entry, 'media_content') and len(entry.media_content) > 0:
                    photo_url = entry.media_content[0].get('url', DEFAULT_IMAGE)
                
                # Check for image inside summary HTML
                if photo_url == DEFAULT_IMAGE and hasattr(entry, 'summary'):
                    summary_soup = BeautifulSoup(entry.summary, "html.parser")
                    img = summary_soup.find('img')
                    if img and img.get('src'):
                        photo_url = img['src']
                        
                # Check for image inside content HTML
                if photo_url == DEFAULT_IMAGE and hasattr(entry, 'content'):
                    for content in entry.content:
                        if content.value:
                            content_soup = BeautifulSoup(content.value, "html.parser")
                            img = content_soup.find('img')
                            if img and img.get('src'):
                                photo_url = img['src']
                                break
                
                if link and text_content:
                    news_items.append({"text": text_content, "link": link, "photo_url": photo_url})
        except Exception as e:
            logger.error(f"Error scraping RSS {rss_url}: {e}")
            
    # Shuffle sources to ensure diversity
    random.shuffle(news_items)
    return news_items

async def run_aggregator_job(context: ContextTypes.DEFAULT_TYPE):
    channel_id = get_publish_channel()
    admin_id = get_admin_chat()
    
    if not channel_id or not admin_id:
        return

    logger.info("Running aggregator job...")
    news_items = fetch_latest_news()
    processed_count: int = 0

    for item in news_items:
        if processed_count >= 5:
            break
            
        url = item['link']
        if is_link_processed(url):
            continue

        # Pre-filter: skip obviously off-topic content before even calling Gemini
        if not is_tech_relevant(item['text']):
            logger.info(f"Skipping off-topic item (pre-filter): {url}")
            # Still mark as processed so we don't keep re-checking it
            save_article(url, "", "", "")
            continue
            
        logger.info(f"Processing new item: {url}")
        
        # 1. Translate via Gemini
        translated = process_and_translate(item['text'])
        
        if not translated:
            continue

        # Skip if Gemini flagged it as off-topic (title starts with 🚫)
        if translated.get('title_ru', '').startswith('🚫'):
            logger.info(f"Gemini flagged item as off-topic: {url}")
            save_article(url, "", "", "")
            continue
            
        text_uz = translated['uz']
        text_ru = translated['ru']
        photo_url = item['photo_url']
        
        if not photo_url or photo_url == DEFAULT_IMAGE:
            photo_url = get_thematic_image(translated.get('image_prompt', 'tech news'))
            
        # 2. Save and get article ID
        article_id = save_article(url, text_uz, text_ru, photo_url)
        if article_id == -1:
            continue
            
        # 3. Send preview to Admin for review
        combined_caption = f"🇷🇺 {text_ru}\n\n🇺🇿 {text_uz}"
        if not url.startswith("manual_"):
            combined_caption += f"\n\n🔗 <a href='{url}'>Источник / Manba</a>"
        combined_caption += "\n📢 @deeptrusttech"
        
        # Absolute safety fallback for Telegram limit
        if len(combined_caption) > 1024:
            combined_caption = combined_caption[:1024]
            
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
            [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
            [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
        ])
        
        img_bytes = None
        if photo_url and photo_url.startswith("http"):
            img_bytes = await download_image(photo_url)
        
        final_photo = img_bytes if img_bytes else DEFAULT_IMAGE
        
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=final_photo,
                caption=combined_caption,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            processed_count = processed_count + 1
        except Exception as e:
            logger.error(f"Failed to send to admin {admin_id}: {e}")

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
        
        REVISION_PROMPT = f"""You are a top-tier global tech analyst and bilingual copywriter.
The user wants to revise a previously generated news post.
Current text:
{old_text}

User Edit Instruction:
{text}

Please rewrite the news according to the user's instructions.
CRITICAL RULES:
1. You MUST keep the EXACT same JSON schema response format (TranslatedArticle).
2. Do NOT include HTML tags in the text fields (no <b> or <br>), just plain text.
3. Keep the 3 paragraphs concise (max 150 chars per field).
4. Provide translation in Official Russian and Native-level Official Uzbek.
5. Always provide exactly 3 distinct paragraphs following this strict structure:
   - p1: 🚀 Суть (Core news/Headline)
   - p2: 💡 Контекст (Why it matters)
   - p3: 📌 Итог (Actionable takeaway)"""

        try:
            response = client.models.generate_content(
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
                
            new_ru = data.get("ru", "")
            new_uz = data.get("uz", "")
            
            # Update DB
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("UPDATE articles SET text_ru = ?, text_uz = ? WHERE id = ?", (new_ru, new_uz, article_id))
            cursor.execute("SELECT photo_url FROM articles WHERE id = ?", (article_id,))
            photo_row = cursor.fetchone()
            conn.commit()
            conn.close()
            
            photo_url = photo_row[0] if photo_row else DEFAULT_IMAGE
            
            caption_ru = f"🇷🇺 <b>НОВАЯ НОВОСТЬ ДЛЯ ПУБЛИКАЦИИ:</b>\n\n{new_ru}"
            safe_text = caption_ru[:800] + "..." if len(caption_ru) > 800 else caption_ru
            caption_ru = f"{safe_text}\n📢 @deeptrusttech"
            
            if len(caption_ru) > 1024:
                caption_ru = caption_ru[:1024]
                
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
                [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
                [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
            ])
            
            img_bytes = None
            if photo_url and photo_url.startswith("http"):
                img_bytes = await download_image(photo_url)
            
            await update.message.reply_photo(
                photo=img_bytes if img_bytes else DEFAULT_IMAGE,
                caption=caption_ru,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return

    # If no state, process as a brand new manual text logic
    logger.info("Replying with status message...")
    await update.message.reply_text("⏳ Обрабатываю новую (ручную) новость...")
    
    logger.info("Calling process_and_translate...")
    translated = process_and_translate(str(text))
    logger.info(f"Translation returned. Success: {bool(translated)}")
    if not translated:
        await update.message.reply_text("❌ Ошибка при обращении к API Gemini (возможно, исчерпан лимит). Попробуйте позже.")
        return
        
    # --- Advanced link extraction for manual posts (do this FIRST) ---
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
            
    # Attempt Regex from text
    if not link:
        urls = re.findall(r'(https?://[^\s]+)', str(text))
        if urls:
            link = urls[-1] # Usually the source link is at the bottom
            
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
        logger.info(f"No media found in TG message, attempting to scrape original source: {link}")
        scraped_img = extract_og_image(link)
        if scraped_img:
            photo_url = scraped_img
        
    # Absolute fallback
    if not photo_url:
        photo_url = DEFAULT_IMAGE

    article_id = save_article(link, translated['uz'], translated['ru'], photo_url, media_type)
    if article_id == -1:
        await update.message.reply_text("❌ Ошибка при сохранении.")
        return
        
    caption_combined = f"{translated['uz']}\n\n➖➖➖\n\n{translated['ru']}"
    
    if not link.startswith("manual_"):
        caption_combined += f"\n\n🔗 Источник: {link}"
        
    caption_combined += "\n📢 @deeptrusttech"
    
    if len(caption_combined) > 1024:
        caption_combined = caption_combined[:1024]
        
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub|{article_id}")],
        [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit|{article_id}")],
        [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel|{article_id}")]
    ])
    
    img_bytes = None
    if photo_url and photo_url.startswith("http"):
        try:
            img_bytes = await download_image(photo_url)
        except Exception as e:
            logger.error(f"Image download failed: {e}")
    
    try:
        final_photo = img_bytes if img_bytes else (photo_url if photo_url else DEFAULT_IMAGE)
        if media_type == "video" and not img_bytes:
            await update.message.reply_video(
                video=final_photo,
                caption=caption_combined,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await update.message.reply_photo(
                photo=final_photo,
                caption=caption_combined,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    except Exception as photo_err:
        if "Can't use file of type" in str(photo_err) and photo_url and not photo_url.startswith("http"):
            try:
                tg_file = await context.bot.get_file(photo_url)
                downloaded_bytes = bytes(await tg_file.download_as_bytearray())
                await update.message.reply_photo(
                    photo=downloaded_bytes,
                    caption=caption_combined,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                return
            except Exception as dl_err:
                logger.error(f"Fallback download failed: {dl_err}")
                
        logger.error(f"Failed to send photo: {photo_err}")
        await update.message.reply_text(f"❌ Ошибка отправки: {photo_err}")

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
            caption_combined = f"{text_uz}\n\n➖➖➖\n\n{text_ru}"
            
            if not link.startswith("manual_"):
                caption_combined += f"\n\n🔗 Источник: {link}"
                
            caption_combined += "\n📢 @deeptrusttech"
                
            if len(caption_combined) > 1024:
                caption_combined = caption_combined[:1024]
                
            img_bytes = None
            if photo_url and photo_url.startswith("http"):
                try:
                    img_bytes = await download_image(photo_url)
                except Exception as e:
                    logger.error(f"Image db download failed: {e}")
            
            try:
                final_photo = img_bytes if img_bytes else (photo_url if photo_url else DEFAULT_IMAGE)
                if media_type == "video" and not img_bytes:
                    await context.bot.send_video(
                        chat_id=channel_id,
                        video=final_photo,
                        caption=caption_combined,
                        parse_mode="HTML"
                    )
                else:
                    await context.bot.send_photo(
                        chat_id=channel_id,
                        photo=final_photo,
                        caption=caption_combined,
                        parse_mode="HTML"
                    )
                await query.edit_message_caption(caption=f"✅ Опубликовано в канал!\n\n{caption_combined}", reply_markup=None, parse_mode="HTML")
            except Exception as photo_err:
                if "Can't use file of type" in str(photo_err) and photo_url and not photo_url.startswith("http"):
                    try:
                        tg_file = await context.bot.get_file(photo_url)
                        downloaded_bytes = bytes(await tg_file.download_as_bytearray())
                        if media_type == "video":
                            await context.bot.send_video(
                                chat_id=channel_id,
                                video=downloaded_bytes,
                                caption=caption_combined,
                                parse_mode="HTML"
                            )
                        else:
                            await context.bot.send_photo(
                                chat_id=channel_id,
                                photo=downloaded_bytes,
                                caption=caption_combined,
                                parse_mode="HTML"
                            )
                        await query.edit_message_caption(caption=f"✅ Опубликовано в канал! (через обход Telegram API)\n\n{caption_combined}", reply_markup=None, parse_mode="HTML")
                        return
                    except Exception as dl_err:
                        logger.error(f"Fallback publish download failed: {dl_err}")
                        
                await query.edit_message_caption(caption=f"❌ Ошибка публикации: {photo_err}", reply_markup=None)

def main() -> None:
    init_db()
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("set_channel", set_channel_command))
    app.add_handler(CommandHandler("set_admin", set_admin_command))
    app.add_handler(CallbackQueryHandler(publish_callback, pattern="^(pub|cancel|edit)\|.*"))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, manual_post_handler))

    job_queue = app.job_queue
    job_queue.run_repeating(run_aggregator_job, interval=60, first=10)

    logger.info("Bot is running V6 (Strict Formatting, Branding & Fast Delivery)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()