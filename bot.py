import os
import time
import logging
import asyncio
import hashlib
from datetime import datetime
from typing import Optional

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
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logging.warning(f"{func.__name__} failed (attempt {attempt}/{max_retries}): {e}")
                    if attempt == max_retries:
                        logging.error(
                            f"{func.__name__} giving up after {max_retries} attempts",
                            exc_info=True,
                        )
                        return None
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

# ─── 除外ユーザー設定 ───────────────────────────────────────
EXCLUDED_USERS = {
    normalize("宮内 和貴 / Kazuki Miyauchi"),
    normalize("ryuji"),
    normalize("井上 璃久 / Riku Inoue"),
}

# ─── Slack チャンネル振り分け設定 ──────────────────────────
USER_SLACK_CHANNEL_MAP = {
    normalize("Li Qiuhan"): "C093TJYG3C0",
    normalize("桑名優輔 / Yusuke Kuwana"): "C093BK505FU",
    normalize("郭宇培/GUO YUPEI"): "C08GADEEHE1",
}

# ─── Slack API 定数 ───────────────────────────────────────────
SLACK_CHAT_POST_URL = "https://slack.com/api/chat.postMessage"

# ─── Slack ユーザーキャッシュ ──────────────────────────────
slack_user_cache = {}

def get_user_slack_channel(name: str) -> Optional[str]:
    """ユーザーごとのSlack投稿先チャンネルIDを返す。"""
    channel = USER_SLACK_CHANNEL_MAP.get(normalize(name), SLACK_CHANNEL_ID)
    if not channel:
        logging.warning(
            "Slack channel ID is not configured for %s. Message will be skipped.",
            name,
        )
    return channel


class SlackAPIError(Exception):
    """Slack API 呼び出し時のエラーを表す。"""


class SlackAPIClient:
    """Slack Web API との通信を行うクライアント。"""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock: Optional[asyncio.Lock] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=10)
                self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def post_message(self, payload: dict) -> dict:
        session = await self._get_session()
        headers = {
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        }
        try:
            async with session.post(SLACK_CHAT_POST_URL, headers=headers, json=payload) as resp:
                if resp.status == 429:
                    retry_after_header = resp.headers.get("Retry-After")
                    try:
                        retry_after = float(retry_after_header) if retry_after_header else 1.0
                    except ValueError:
                        retry_after = 1.0
                    await asyncio.sleep(retry_after)
                    raise SlackAPIError(
                        f"Slack API rate limited: retry after {retry_after} seconds"
                    )

                try:
                    data = await resp.json(content_type=None)
                except aiohttp.ContentTypeError:
                    text = await resp.text()
                    raise SlackAPIError(
                        f"Slack API returned non-JSON response (status {resp.status}): {text[:200]}"
                    )

                if resp.status >= 500:
                    raise SlackAPIError(f"Slack API temporary failure: {resp.status}")

                if not data.get("ok"):
                    raise SlackAPIError(f"Slack API error: {data}")

                return data
        except aiohttp.ClientError as e:
            raise SlackAPIError(f"Slack API request failed: {e}") from e

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None


slack_api_client = SlackAPIClient()

def build_slack_user_cache():
    try:
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        response = requests.get("https://slack.com/api/users.list", headers=headers, timeout=10)
        response.raise_for_status()
        resp = response.json()
        if not resp.get("ok"):
            logging.error("Slack users.list returned error: %s", resp)
            return

        slack_user_cache.clear()
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
        response = requests.get("https://slack.com/api/users.list", headers=headers, timeout=10)
        response.raise_for_status()
        resp = response.json()
        if not resp.get("ok"):
            logging.error("Slack users.list returned error: %s", resp)
            return None

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
async def send_slack_message(
    text,
    mention_user_id=None,
    thread_ts=None,
    use_daily_channel=False,
    channel_override=None,
):
    if use_daily_channel:
        channel = DAILY_REPORT_CHANNEL_ID
    elif channel_override:
        channel = channel_override
    else:
        channel = SLACK_CHANNEL_ID

    if not channel:
        logging.error("Slack channel is not specified. Message will be skipped: %s", text)
        return None

    msg     = f"<@{mention_user_id}>\n{text}" if mention_user_id else text
    payload = {"channel": channel, "text": msg}
    if thread_ts:
        payload["thread_ts"] = thread_ts

    data = await slack_api_client.post_message(payload)
    return data.get("ts")

# ─── Discord クライアント設定 ─────────────────────────────
intents = discord.Intents.default()
intents.voice_states = True
intents.members      = True
client = discord.Client(intents=intents)
monitor_task: Optional[asyncio.Task] = None

@client.event
async def on_voice_state_update(member, before, after):
    try:
        now   = datetime.now(JST)
        name  = member.display_name
        norm  = normalize(name)
        if norm in EXCLUDED_USERS:
            return

        user_channel = get_user_slack_channel(name)

        event_type = None
        if not before.channel and after.channel:
            event_type = "clock_in"
        elif before.channel and not after.channel:
            event_type = "clock_out"
        elif before.channel and after.channel and before.channel != after.channel:
            event_type = "move"
        if not event_type:
            return

        key          = f"{member.id}-{event_type}"
        channel_name = (after.channel or before.channel).name
        ehash        = generate_event_hash(member.id, event_type, channel_name, now)
        prev         = last_events.get(key)
        if prev and (now - prev["timestamp"]).total_seconds() < 3 and prev["event_hash"] == ehash:
            return
        last_events[key] = {"timestamp": now, "event_hash": ehash}

        if after.channel and after.channel.name == "休憩室":
            rest_start_times[name] = now
        if before.channel and before.channel.name == "休憩室":
            start = rest_start_times.pop(name, None)
            if start:
                rest_durations[name] = rest_durations.get(name, 0) + (now - start).total_seconds()

        if event_type == "clock_in" and name not in clock_in_times and after.channel.name != "休憩室":
            rest_durations[name] = 0
            clock_in_times[name] = now
            last_sheet_events[f"{name}-出勤"] = now
            message = (
                f"{name} が「{after.channel.name}」に出勤しました。\n"
                f"出勤時間\n{now.strftime('%Y/%m/%d %H:%M:%S')}"
            )
            await send_slack_message(message, channel_override=user_channel)

        elif event_type == "move" and name in clock_in_times:
            last = last_sheet_events.get(f"{name}-move")
            if not last or (now - last).total_seconds() >= 3:
                last_sheet_events[f"{name}-move"] = now
                await send_slack_message(
                    f"{name} が「{after.channel.name}」に移動しました。",
                    channel_override=user_channel,
                )

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
            await send_slack_message(msg, channel_override=user_channel)
    except Exception as e:
        logging.error(f"on_voice_state_update error: {e}")


@client.event
async def on_error(event, *args, **kwargs):
    logging.exception("Unhandled exception in Discord event '%s'", event)


async def monitor_voice_channels():
    await client.wait_until_ready()
    try:
        while not client.is_closed():
            try:
                now = datetime.now(JST)
                for guild in client.guilds:
                    for member in guild.members:
                        norm = normalize(member.display_name)
                        if norm in EXCLUDED_USERS:
                            continue

                        if member.display_name in clock_in_times and not member.voice:
                            clock_in = clock_in_times.pop(member.display_name)
                            elapsed  = (now - clock_in).total_seconds()
                            if elapsed < 60:
                                continue

                            rest_sec = rest_durations.pop(member.display_name, 0)
                            work_sec = int((now - clock_in).total_seconds() - rest_sec)
                            user_channel = get_user_slack_channel(member.display_name)

                            msg = (
                                f"{member.display_name} の接続が切れました（強制退勤と見なします）。\n"
                                f"退勤時間\n{now.strftime('%Y/%m/%d %H:%M:%S')}\n\n"
                                f"勤務時間\n{format_duration(work_sec)}"
                            )
                            await send_slack_message(msg, channel_override=user_channel)
            except Exception as e:
                logging.error(f"monitor_voice_channels error: {e}")
            await asyncio.sleep(15)
    except asyncio.CancelledError:
        logging.info("monitor_voice_channels task cancelled")
        raise


async def stop_monitoring_task():
    global monitor_task
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    monitor_task = None

# ─── Flask アプリ（ヘルスチェック）───────────────────────
app = Flask(__name__)
@app.route("/")
def health_check():
    return "OK"

def run_discord_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        while True:
            try:
                loop.run_until_complete(client.start(DISCORD_TOKEN))
            except discord.errors.LoginFailure as e:
                logging.critical("Discord login failed: %s", e)
                break
            except Exception as e:
                logging.exception("Discord client crashed: %s", e)
                try:
                    loop.run_until_complete(stop_monitoring_task())
                except Exception:
                    logging.debug("monitor task cancellation skipped during restart.")
                try:
                    loop.run_until_complete(client.close())
                except Exception:
                    logging.debug("Discord client close skipped during restart.")
                try:
                    loop.run_until_complete(slack_api_client.close())
                except Exception:
                    logging.debug("Slack session close skipped during restart.")
                time.sleep(5)
            else:
                break
    finally:
        if not loop.is_closed():
            try:
                loop.run_until_complete(stop_monitoring_task())
            except Exception:
                logging.debug("monitor task cancellation skipped due to prior state.")
            try:
                loop.run_until_complete(client.close())
            except Exception:
                logging.debug("Discord client close skipped due to prior state.")
            try:
                loop.run_until_complete(slack_api_client.close())
            except Exception:
                logging.debug("Slack session close skipped due to prior state.")
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

@client.event
async def on_ready():
    logging.info(f"{client.user} is ready. Starting monitoring task.")
    global monitor_task
    if monitor_task and not monitor_task.done():
        logging.info("monitor_voice_channels task already running; skipping duplicate start.")
        return
    monitor_task = client.loop.create_task(monitor_voice_channels())

if __name__ == "__main__":
    build_slack_user_cache()
    Thread(target=run_discord_bot, daemon=True).start()
    from waitress import serve
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)
