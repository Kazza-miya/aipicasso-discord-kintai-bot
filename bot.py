import os
import logging
import asyncio
import hashlib
from datetime import datetime
import pytz

import discord
import aiohttp
import requests
from flask import Flask
from threading import Thread
from dotenv import load_dotenv
from functools import wraps

# ─── ログ設定 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
)

# ─── タイムゾーン設定 ───────────────────────────────────────
JST = pytz.timezone("Asia/Tokyo")

# ─── 環境変数読み込み ───────────────────────────────────────
load_dotenv()
DISCORD_TOKEN           = os.getenv("DISCORD_TOKEN")
SLACK_BOT_TOKEN         = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID        = os.getenv("SLACK_CHANNEL_ID")
DAILY_REPORT_CHANNEL_ID = os.getenv("DAILY_REPORT_CHANNEL_ID")

if not DISCORD_TOKEN or not SLACK_BOT_TOKEN:
    logging.error("DISCORD_TOKEN か SLACK_BOT_TOKEN が設定されていません。")
    exit(1)

# ─── リトライデコレータ ─────────────────────────────────────
def retry(max_retries: int = 3, backoff_factor: float = 2.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"{func.__name__} failed (attempt {attempt}/{max_retries}): {e}")
                    if attempt == max_retries:
                        logging.error(f"{func.__name__} giving up after {max_retries} attempts")
                        return None
                    # バックオフ
                    await asyncio.sleep(backoff_factor ** (attempt - 1))
        return wrapper
    return decorator

# ─── 状態管理用変数 ───────────────────────────────────────
last_sheet_events = {}   # 最終イベント時刻
clock_in_times    = {}   # 出勤時刻
rest_start_times  = {}   # 休憩開始時刻
rest_durations    = {}   # 累積休憩時間（秒）
last_events       = {}   # 多重発火抑制用

# ─── ユーティリティ関数 ───────────────────────────────────
def normalize(name: str) -> str:
    return name.lower().replace("　", " ").replace("・", " ").strip() if name else ""

def generate_event_hash(user_id, event_type, channel_name, timestamp):
    raw = f"{user_id}-{event_type}-{channel_name}-{timestamp.strftime('%Y%m%d%H%M%S%f')}"
    return hashlib.md5(raw.encode()).hexdigest()

def format_duration(seconds: int) -> str:
    minutes = seconds // 60
    hours   = minutes // 60
    minutes = minutes % 60
    return f"{hours:02d}:{minutes:02d}"

# ─── 通知対象ユーザー設定 ─────────────────────────────────
ALLOWED_USERS = {
    normalize("井上 璃久 / Riku Inoue"),
    normalize("平井 悠喜 / Yuki Hirai"),
    normalize("松岡満貴 / Maki Matsuoka"),
    normalize("桑名優輔 / Yusuke Kuwana"),    
}

# ─── Slack ユーザーキャッシュ ──────────────────────────────
slack_user_cache = {}

def build_slack_user_cache():
    try:
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        resp = requests.get("https://slack.com/api/users.list", headers=headers, timeout=10).json()
        for m in resp.get("members", []):
            if m.get("deleted"):
                continue
            uid  = m["id"]
            prof = m.get("profile", {})
            slack_user_cache[normalize(prof.get("real_name",""))]    = uid
            slack_user_cache[normalize(prof.get("display_name",""))] = uid
        logging.info("Slack user cache built.")
    except Exception as e:
        logging.error(f"build_slack_user_cache error: {e}")

def get_slack_user_id_sync(discord_name: str):
    norm = normalize(discord_name)
    if norm in slack_user_cache:
        return slack_user_cache[norm]
    try:
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        resp = requests.get("https://slack.com/api/users.list", headers=headers, timeout=10).json()
        for m in resp.get("members", []):
            if m.get("deleted"):
                continue
            uid  = m["id"]
            prof = m.get("profile", {})
            slack_user_cache[normalize(prof.get("real_name",""))]    = uid
            slack_user_cache[normalize(prof.get("display_name",""))] = uid
            if norm in normalize(prof.get("real_name","")) or norm in normalize(prof.get("display_name","")):
                return uid
    except Exception as e:
        logging.error(f"get_slack_user_id_sync error: {e}")
    return None

# ─── 非同期 Slack 通知 with Retry ────────────────────────────
@retry(max_retries=3, backoff_factor=2.0)
async def send_slack_message(text, mention_user_id=None, thread_ts=None, use_daily_channel=False):
    channel = DAILY_REPORT_CHANNEL_ID if use_daily_channel else SLACK_CHANNEL_ID
    msg     = f"<@{mention_user_id}>\n{text}" if mention_user_id else text
    payload = {"channel": channel, "text": msg}
    if thread_ts:
        payload["thread_ts"] = thread_ts

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                "Content-Type": "application/json"
            },
            json=payload
        ) as resp:
            data = await resp.json()
            if not data.get("ok"):
                # Slack API エラーは例外化してリトライ
                raise Exception(f"Slack API error: {data}")
            return data.get("ts")

# ─── Discord クライアント設定 ─────────────────────────────
intents = discord.Intents.default()
intents.voice_states = True
intents.members      = True
client = discord.Client(intents=intents)

@client.event
async def on_voice_state_update(member, before, after):
    try:
        now   = datetime.now(JST)
        name  = member.display_name
        norm  = normalize(name)
        if norm not in ALLOWED_USERS:
            return

        # イベント種別判定
        event_type = None
        if not before.channel and after.channel:
            event_type = "clock_in"
        elif before.channel and not after.channel:
            event_type = "clock_out"
        elif before.channel and after.channel and before.channel != after.channel:
            event_type = "move"
        if not event_type:
            return

        # 多重発火抑制
        key          = f"{member.id}-{event_type}"
        channel_name = (after.channel or before.channel).name
        ehash        = generate_event_hash(member.id, event_type, channel_name, now)
        prev         = last_events.get(key)
        if prev and (now - prev["timestamp"]).total_seconds() < 3 and prev["event_hash"] == ehash:
            return
        last_events[key] = {"timestamp": now, "event_hash": ehash}

        # 休憩管理
        if after.channel and after.channel.name == "休憩室":
            rest_start_times[name] = now
        if before.channel and before.channel.name == "休憩室":
            start = rest_start_times.pop(name, None)
            if start:
                rest_durations[name] = rest_durations.get(name, 0) + (now - start).total_seconds()

        # 出勤処理
        if event_type == "clock_in" and name not in clock_in_times and after.channel.name != "休憩室":
            rest_durations[name] = 0
            clock_in_times[name] = now
            last_sheet_events[f"{name}-出勤"] = now
            await send_slack_message(
                f"{name} が「{after.channel.name}」に出勤しました。\n"
                f"出勤時間\n{now.strftime('%Y/%m/%d %H:%M:%S')}"
            )

        # 移動処理
        elif event_type == "move" and name in clock_in_times:
            last = last_sheet_events.get(f"{name}-move")
            if not last or (now - last).total_seconds() >= 3:
                last_sheet_events[f"{name}-move"] = now
                await send_slack_message(f"{name} が「{after.channel.name}」に移動しました。")

        # 退勤処理
        if event_type == "clock_out" and name in clock_in_times:
            clock_out = now
            clock_in  = clock_in_times.pop(name, None)
            rest_sec  = rest_durations.pop(name, 0)
            work_sec  = int((clock_out - clock_in).total_seconds() - rest_sec) if clock_in else 0

            msg = (
                f"{name} が「{before.channel.name}」を退出しました。\n"
                f"退勤時間\n{now.strftime('%Y/%m/%d %H:%M:%S')}\n\n"
                f"勤務時間\n{format_duration(work_sec)}"
            )
            ts = await send_slack_message(msg)
            if ts:
                await asyncio.sleep(2)
                uid = await asyncio.to_thread(get_slack_user_id_sync, name)
                mention = f"<@{uid}>\n" if uid else ""
                thread_msg = (
                    f"{mention}以下のテンプレを <#{DAILY_REPORT_CHANNEL_ID}> に記載してください：\n"
                    "◆日報一言テンプレート\nやったこと\n・\n次にやること\n・\nひとこと\n・"
                )
                await send_slack_message(thread_msg, thread_ts=ts)

    except Exception as e:
        logging.error(f"on_voice_state_update error: {e}")

async def monitor_voice_channels():
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            now = datetime.now(JST)
            for guild in client.guilds:
                for member in guild.members:
                    norm = normalize(member.display_name)
                    if norm not in ALLOWED_USERS:
                        continue

                    if member.display_name in clock_in_times and not member.voice:
                        clock_in = clock_in_times.pop(member.display_name)
                        elapsed  = (now - clock_in).total_seconds()
                        if elapsed < 60:
                            continue

                        rest_sec = rest_durations.pop(member.display_name, 0)
                        work_sec = int((now - clock_in).total_seconds() - rest_sec)

                        msg = (
                            f"{member.display_name} の接続が切れました（強制退勤と見なします）。\n"
                            f"退勤時間\n{now.strftime('%Y/%m/%d %H:%M:%S')}\n\n"
                            f"勤務時間\n{format_duration(work_sec)}"
                        )
                        ts = await send_slack_message(msg)
                        if ts:
                            await asyncio.sleep(2)
                            uid = await asyncio.to_thread(get_slack_user_id_sync, member.display_name)
                            mention = f"<@{uid}>\n" if uid else ""
                            thread_msg = (
                                f"{mention}以下のテンプレを <#{DAILY_REPORT_CHANNEL_ID}> に記載してください：\n"
                                "◆日報一言テンプレート\nやったこと\n・\n次にやること\n・\nひとこと\n・"
                            )
                            await send_slack_message(thread_msg, thread_ts=ts)

        except Exception as e:
            logging.error(f"monitor_voice_channels error: {e}")

        await asyncio.sleep(15)

# ─── Flask アプリ（ヘルスチェック）───────────────────────
app = Flask(__name__)
@app.route("/")
def health_check():
    return "OK"

def run_discord_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.start(DISCORD_TOKEN))

@client.event
async def on_ready():
    logging.info(f"{client.user} is ready. Starting monitoring task.")
    client.loop.create_task(monitor_voice_channels())

if __name__ == "__main__":
    build_slack_user_cache()
    Thread(target=run_discord_bot, daemon=True).start()
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
