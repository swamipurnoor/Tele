#!/usr/bin/env python3
# ============================== SETTINGS ===============================
# Nothing secret lives in this file (the repo is PUBLIC). Everything comes from
# environment variables / GitHub Actions secrets:
#   TG_API_ID, TG_API_HASH   from https://my.telegram.org
#   TG_SESSION               Telethon StringSession (made by make_session.py)
#   TG_SOURCE, TG_TARGET     channel / group IDs, e.g. -1001234567890
#   TG_FROM, TG_TO           optional message range (leave empty = everything)
import os as _os
API_ID = _os.environ.get("TG_API_ID", "")
API_HASH = _os.environ.get("TG_API_HASH", "")
SOURCE = int(_os.environ.get("TG_SOURCE") or 0)
TARGET = int(_os.environ.get("TG_TARGET") or 0)
FROM_MESSAGE = _os.environ.get("TG_FROM") or None
TO_MESSAGE = _os.environ.get("TG_TO") or None
# ========================================================================
"""
Telegram copier (Telethon)

Copies EVERY message of SOURCE into TARGET, in the original order, by downloading
and re-uploading (nothing is forwarded): text, photos, videos, PDFs/other files,
audio/voice notes, and albums (kept together). Thumbnails/previews of videos,
PDFs, audio and other files are copied too.

If SOURCE is a group with TOPICS (a forum), each topic is copied into a topic with
the same name in TARGET (TARGET must also be a group with topics enabled, and you
must be an admin). Topics are created automatically. The global order of the
messages is preserved.

What you see
  * one detailed line per message, a live upload bar, and an estimated percentage
    of the whole range with ETA (also written to copier.log)
  * every problem is printed loudly (stderr) and logged; nothing fails silently

What happens when something goes wrong
  * internet/Telegram trouble: retried FOREVER with growing waits (max 5 min),
    flood limits are waited out; safe for unattended overnight runs
  * one message that can never be copied (file over the upload limit, media
    removed, rejected by Telegram): a placeholder text saying what failed is posted
    in its place, it is written to failed.log, and copying continues
  * polls, locations, contacts, dice and self-destructing media cannot be
    copied: they are ignored, but every one is printed and written to skipped.log
  * setup problems (no permission in TARGET, revoked login, disk full, ...): the
    script stops immediately with a clear message; just rerun after fixing it

Progress is safe: on every start the script reads TARGET, works out exactly how
much of the range is already copied, and rewrites progress.json to match.

Usage
    python copier.py list                         # channels/chats with their IDs
    python copier.py topics                       # topics of SOURCE and TARGET
    python copier.py status                       # show saved progress
    python copier.py run                          # copy everything (resumes)
    python copier.py run --dry-run                # sync + show what is left, copy nothing
    python copier.py run --from LINK_OR_ID        # start at this message (inclusive)
    python copier.py run --to LINK_OR_ID          # stop at this message (inclusive)

    LINK is what Telegram gives you via "Copy Message Link", e.g.
    https://t.me/c/4364866470/1234  -- a plain message number (1234) works too.
    --from / --to override FROM_MESSAGE / TO_MESSAGE for one run. FROM cannot be
    changed once copying has started (the target is filled in order from it).

Other options
    --no-sync       trust progress.json instead of reading the target (not advised)
    --prefetch N    how many messages may wait, already downloaded or queued, ahead of the
                    upload (default 2). Downloads always run one at a time, in order.

(TG_API_ID / TG_API_HASH environment variables override the credentials above.)
"""

import argparse
import asyncio
import errno
import glob
import json
import logging
import os
import random
import re
import shutil
import socket
import sys
import time
import traceback
from collections import Counter
from datetime import datetime

from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon import errors as tgerr
from telethon.errors import RPCError, ServerError
from telethon.tl.functions.messages import (
    CreateForumTopicRequest,
    GetForumTopicsRequest,
    SendMultiMediaRequest,
    UploadMediaRequest,
)
from telethon.tl.types import (
    ForumTopic,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    InputReplyToMessage,
    InputSingleMedia,
    MessageActionTopicCreate,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    UpdateNewChannelMessage,
    UpdateNewMessage,
)

# ----------------------------- CONFIG ---------------------------------
# Environment variables, if set, override the credentials at the top of the file.
API_ID = int(API_ID) if API_ID else 0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_NAME = os.path.join(BASE_DIR, "copier_session")  # login session (keep private!)
STATE_FILE = os.path.join(BASE_DIR, "progress.json")     # progress tracker
LOG_FILE = os.path.join(BASE_DIR, "copier.log")          # everything that was printed
SKIPPED_LOG = os.path.join(BASE_DIR, "skipped.log")      # ignored (uncopyable) messages
FAILED_LOG = os.path.join(BASE_DIR, "failed.log")        # messages replaced by a placeholder
TMP_DIR = os.path.join(BASE_DIR, "downloads")            # temporary download folder

PREFETCH = 2                     # messages that may wait ahead of the upload (downloads run one at a time)
SAVE_EVERY = 2.0                 # seconds between progress saves (target channel is the real record)
MAX_BACKOFF = 300                # longest wait between retries of a network problem (seconds)
MAX_CONSECUTIVE_FAILURES = 5     # this many Telegram-rejected messages in a row = stop (setup problem?)
MSG_WEIGHT = 100_000             # "bytes" each message counts for in the percentage/ETA estimate
STATUS_EVERY = 300               # seconds between status summaries
MARKER = "⚠️ [copier] COULD NOT COPY"   # start of every placeholder text
# -----------------------------------------------------------------------

os.makedirs(TMP_DIR, exist_ok=True)
client = None  # created in main()
IS_TTY = sys.stdout.isatty()
logging.basicConfig(format="%(asctime)s telethon %(levelname)s: %(message)s", level=logging.WARNING)


# ------------------------------ output ---------------------------------
def fmt_size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1000


def fmt_time(seconds):
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


_live_shown = [False]


def live(text):
    """Single updating line (upload bar). Only on a real terminal."""
    if not IS_TTY:
        return
    width = shutil.get_terminal_size((100, 20)).columns - 1
    sys.stdout.write("\r\033[K" + text[:width])
    sys.stdout.flush()
    _live_shown[0] = True


def log(msg, level="INFO"):
    """Timestamped line on screen (stderr for problems) and in copier.log."""
    if IS_TTY and _live_shown[0]:
        sys.stdout.write("\r\033[K")
        _live_shown[0] = False
    stream = sys.stderr if level in ("WARN", "ERROR") else sys.stdout
    print(f"[{datetime.now():%H:%M:%S}] {msg}", file=stream, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {level:5} {msg}\n")


def append_file(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {line}\n")


# ------------------------- progress tracking ---------------------------
def load_state():
    state = {"source": SOURCE, "target": TARGET, "last_id": 0, "done": 0}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state.update(json.load(f))
    state.pop("seen", None)      # keys from older versions
    state.pop("pending", None)
    return state


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)  # atomic: never leaves a half-written file


_last_save = [0.0]


def maybe_save(state, force=False):
    now = time.monotonic()
    if force or now - _last_save[0] >= SAVE_EVERY:
        save_state(state)
        _last_save[0] = now


# --------------------------- message analysis --------------------------
def describe(msg):
    """(category, key) for a copyable message, else None.

    category: "text" (key = text), "photo", "doc" (key = file size).
    None = service message, empty message or a type we can't copy.
    """
    if getattr(msg, "action", None) is not None:
        return None
    media = msg.media
    raw = msg.raw_text or ""
    if media is None or isinstance(media, MessageMediaWebPage):
        return ("text", raw) if raw else None
    if getattr(media, "ttl_seconds", None):
        return None
    if isinstance(media, MessageMediaPhoto) and media.photo is not None:
        return ("photo", None)
    if isinstance(media, MessageMediaDocument) and media.document is not None:
        f = msg.file
        return ("doc", f.size if f else None)
    return None


def skip_reason(msg):
    """Why a content message is ignored (None = nothing worth reporting)."""
    if getattr(msg, "action", None) is not None or describe(msg) is not None:
        return None
    media = msg.media
    if media is None:
        return None
    if getattr(media, "ttl_seconds", None):
        return "self-destructing media"
    if isinstance(media, MessageMediaPhoto):
        return "photo that is expired/removed"
    if isinstance(media, MessageMediaDocument):
        return "file that is expired/removed"
    return type(media).__name__.replace("MessageMedia", "").lower() or "unsupported media"


def kind_name(m):
    if m.video_note:
        return "round video"
    if m.voice:
        return "voice message"
    if m.video:
        return "video"
    if m.gif:
        return "GIF"
    if m.audio:
        return "audio"
    if m.sticker:
        return "sticker"
    if m.photo:
        return "photo"
    if m.document:
        return "file"
    return "text"


def unit_bytes(unit):
    return sum((m.file.size or 0) for m in unit if m.file)


def unit_label(unit):
    m = unit[0]
    if len(unit) > 1:
        return f"album of {len(unit)} files (messages {unit[0].id}-{unit[-1].id})"
    bits = [kind_name(m)]
    if m.file and m.file.name:
        bits.append(m.file.name)
    if m.file and m.file.size:
        bits.append(fmt_size(m.file.size))
    if kind_name(m) == "text":
        bits.append(f"{len(m.raw_text)} chars")
    return f"message {m.id} ({', '.join(bits)})"


def caption_msg(unit):
    """The message of a media unit that carries the caption (if any)."""
    for m in unit:
        if m.raw_text:
            return m
    return None


def unit_expected(unit, limit):
    """What this unit turns into in the target channel, message by message."""
    exp = [describe(m) for m in unit]
    if exp[0][0] != "text":
        cm = caption_msg(unit)
        if cm and len(cm.raw_text) > limit:      # too long for a caption -> extra text message
            exp.append(("text", cm.raw_text))
    return exp


def matches(exp, got):
    """exp / got are (category, key) descriptors."""
    if got is None or got[0] != exp[0]:
        return False
    if exp[0] in ("text", "doc"):
        return got[1] == exp[1]
    return True


def label(d):
    if d is None:
        return "unsupported/empty"
    kind, key = d
    if kind == "text":
        return f"text '{key[:30]}'"
    if kind == "doc":
        return f"file of {key} bytes"
    return kind


def force_doc(m):
    """Plain files (pdf, zip, images sent 'as file') must stay files."""
    return bool(m.document) and not (m.video or m.gif or m.audio or m.voice or m.video_note)


# ------------------------------- topics --------------------------------
def topic_of(msg):
    """Topic id of a message inside a forum group (1 = General)."""
    rt = getattr(msg, "reply_to", None)
    if rt is not None and getattr(rt, "forum_topic", False):
        return rt.reply_to_top_id or rt.reply_to_msg_id
    return 1


async def list_topics(entity):
    """All topics of a forum group, oldest first."""
    topics, have = [], set()
    offset_date, offset_id, offset_topic = None, 0, 0
    while True:
        r = await client(GetForumTopicsRequest(
            peer=entity, offset_date=offset_date, offset_id=offset_id,
            offset_topic=offset_topic, limit=100))
        new = [t for t in r.topics if isinstance(t, ForumTopic) and t.id not in have]
        for t in new:
            have.add(t.id)
        topics += new
        if len(r.topics) < 100 or not new:
            break
        last = r.topics[-1]
        offset_topic, offset_id = last.id, getattr(last, "top_message", 0)
        top = next((m for m in r.messages if m.id == offset_id), None)
        offset_date = top.date if top else None
    return sorted(topics, key=lambda t: t.id)


class TopicMap:
    """source topic id -> target topic id (kept in progress.json, rebuilt by name if lost)."""

    def __init__(self, mapping, src_entity, dst_entity):
        self.map = mapping                  # {"<src id>": {"title": ..., "dst": id}}
        self.src_entity, self.dst_entity = src_entity, dst_entity
        self.src_topics = {}                # id -> ForumTopic

    def dst_for(self, src_tid):
        e = self.map.get(str(src_tid))
        return e["dst"] if e else None

    def title(self, src_tid):
        t = self.src_topics.get(src_tid)
        return t.title if t else f"topic {src_tid}"

    async def resolve(self):
        """Match source topics with topics that already exist in the target (creates nothing)."""
        src = await list_topics(self.src_entity)
        dst = await list_topics(self.dst_entity)
        self.src_topics = {t.id: t for t in src}
        dst_ids = {t.id for t in dst}
        self.map["1"] = {"title": "General", "dst": 1}
        for k in [k for k, v in self.map.items() if k != "1" and v["dst"] not in dst_ids]:
            del self.map[k]                 # that target topic no longer exists
        claimed = {v["dst"] for v in self.map.values()}
        for st in src:
            if str(st.id) in self.map:
                continue
            for dt in dst:
                if dt.id not in claimed and dt.title == st.title:
                    self.map[str(st.id)] = {"title": st.title, "dst": dt.id}
                    claimed.add(dt.id)
                    break
        return len(src), len(dst)

    async def ensure(self, src_tid, state):
        """Target topic for this source topic; created (same name) the first time it is needed."""
        dst = self.dst_for(src_tid)
        if dst is not None:
            return dst
        st = self.src_topics.get(src_tid)
        title = st.title if st else f"Topic {src_tid}"
        color = getattr(st, "icon_color", None)
        emoji = getattr(st, "icon_emoji_id", None)

        async def create(with_emoji):
            r = await client(CreateForumTopicRequest(
                peer=self.dst_entity, title=title, icon_color=color,
                icon_emoji_id=emoji if with_emoji else None,
                random_id=random.randrange(1, 2 ** 62)))
            for u in getattr(r, "updates", []):
                m = getattr(u, "message", None)
                if m is not None and isinstance(getattr(m, "action", None), MessageActionTopicCreate):
                    return m.id
            claimed = {v["dst"] for v in self.map.values()}      # fallback: look it up by name
            found = [t.id for t in await list_topics(self.dst_entity)
                     if t.title == title and t.id not in claimed]
            if not found:
                raise RuntimeError(f"topic '{title}' was created but its id could not be found")
            return max(found)

        try:
            dst = await with_retry(lambda: create(True), f"create topic '{title}'")
        except MessageCopyError:
            dst = await with_retry(lambda: create(False), f"create topic '{title}'")
        self.map[str(src_tid)] = {"title": title, "dst": dst}
        maybe_save(state, force=True)
        log(f"📁 created topic '{title}' in the target (source topic {src_tid} -> target topic {dst})")
        return dst


# ------------------------- reading the channels ------------------------
async def iter_units(source, min_id, max_id, log_skips=False):
    """Yield lists of messages: one message, or a whole album, oldest first."""
    group, gid = [], None
    async for msg in client.iter_messages(
        source, reverse=True, min_id=min_id, max_id=max_id, wait_time=0
    ):
        if describe(msg) is None:
            reason = skip_reason(msg)
            if reason and log_skips:
                log(f"↷ ignoring message {msg.id}: {reason} (cannot be copied)", "WARN")
                append_file(SKIPPED_LOG, f"message {msg.id}: {reason}")
            continue
        g = msg.grouped_id
        if group and g is not None and g == gid:
            group.append(msg)
            continue
        if group:
            yield group
        group, gid = [msg], g
    if group:
        yield group


async def count_range(source, min_id, max_id):
    """(messages, bytes, ignored) that would be copied in this id range."""
    msgs = nbytes = ignored = 0
    async for msg in client.iter_messages(
        source, reverse=True, min_id=min_id, max_id=max_id, wait_time=0
    ):
        if describe(msg) is not None:
            msgs += 1
            nbytes += (msg.file.size or 0) if msg.file else 0
        elif skip_reason(msg):
            ignored += 1
    return msgs, nbytes, ignored


# ------------------------------ syncing --------------------------------
class SyncMismatch(Exception):
    pass


async def sync_progress(source, min_id, max_id, limit, tmap=None):
    """Walk SOURCE and TARGET side by side to find how much is already copied.

    Returns (last_id, done_msgs, text_only_id, target_count, target_has_extra, done_bytes).
    """
    # 1. slim index of the target: (id, descriptor) lists, one per topic (or one in total)
    by_key = {}
    async for m in client.iter_messages(TARGET, reverse=True, wait_time=0):
        d = describe(m)
        if d is not None:
            by_key.setdefault(topic_of(m) if tmap else None, []).append((m.id, d))

    # 2. walk the source range unit by unit
    pos = {}
    last_id, done, done_bytes, tgt_count = min_id, 0, 0, 0
    text_only_id, extra = None, False

    async for unit in iter_units(source, min_id, max_id):
        topic = topic_of(unit[0]) if tmap else None
        key = tmap.dst_for(topic) if tmap else None
        seq = by_key.get(key, ())
        exp = unit_expected(unit, limit)
        p = pos.get(key, 0)
        seen, placeholder = 0, False
        for i, e in enumerate(exp):
            if p >= len(seq):
                break
            t_id, got = seq[p]
            if got[0] == "text" and got[1].startswith(MARKER):
                p += 1                      # a placeholder stands in for this whole unit
                tgt_count += 1
                placeholder = True
                break
            if not matches(e, got):
                src_id = unit[min(i, len(unit) - 1)].id
                where = f" in topic '{tmap.title(topic)}'" if tmap else ""
                raise SyncMismatch(
                    f"target message {t_id} ({label(got)}) does not line up with "
                    f"source message {src_id}{where} (expected {label(e)})"
                )
            p += 1
            seen += 1
            tgt_count += 1
        pos[key] = p
        if placeholder or seen == len(exp):
            last_id, done = unit[-1].id, done + len(unit)
            done_bytes += unit_bytes(unit)
            continue
        if seen == 0:
            break                                   # this unit is simply not copied yet
        if seen == len(unit) and len(exp) > len(unit):
            text_only_id = unit[0].id               # media is there, caption text is missing
            break
        raise SyncMismatch(
            f"target ends in the middle of the album starting at source message {unit[0].id}"
        )
    else:
        extra = any(pos.get(k, 0) < len(v) for k, v in by_key.items())
    return last_id, done, text_only_id, tgt_count, extra, done_bytes


# ------------------------ errors and retrying --------------------------
class FatalError(Exception):
    """Setup/local problem: stop the script, the user must fix something."""


class MessageCopyError(Exception):
    """This one message cannot be copied (a placeholder is posted instead)."""

    def __init__(self, reason, counts=True):
        super().__init__(reason)
        self.reason = reason
        self.counts = counts        # counts toward MAX_CONSECUTIVE_FAILURES


FATAL_RPC = tuple(
    getattr(tgerr, n) for n in (
        "ChatWriteForbiddenError", "ChatAdminRequiredError", "UserBannedInChannelError",
        "ChannelPrivateError", "ChannelInvalidError", "PeerIdInvalidError",
        "AuthKeyUnregisteredError", "SessionRevokedError", "UserDeactivatedError",
        "ChatRestrictedError",
    ) if hasattr(tgerr, n)
)
NET_ERRNOS = {
    errno.ECONNRESET, errno.ECONNABORTED, errno.ECONNREFUSED, errno.ETIMEDOUT,
    errno.ENETUNREACH, errno.ENETDOWN, errno.EHOSTUNREACH, errno.EHOSTDOWN,
    errno.EPIPE, errno.ENOTCONN,
}


def is_network_oserror(e):
    return (isinstance(e, (ConnectionError, TimeoutError, socket.gaierror))
            or getattr(e, "errno", None) in NET_ERRNOS)


async def reconnect_if_needed():
    try:
        if not client.is_connected():
            await client.connect()
    except Exception as e:                 # still offline: the retry loop will try again
        log(f"reconnect failed ({type(e).__name__}: {e})", "WARN")


async def with_retry(action, what, landed=None, refresh=None):
    """Run an async action.

    * flood limits: waited out
    * network / server problems: retried forever with growing waits (up to MAX_BACKOFF)
    * permission / login problems: FatalError (stop the script)
    * anything else Telegram rejects: MessageCopyError (this message gets a placeholder)

    landed: optional async check used before a retry, to make sure the failed attempt
    did not actually get through (prevents double posts).
    """
    delay, trouble_since, expired = 5, None, 0
    while True:
        try:
            res = await action()
            if trouble_since is not None:
                log(f"✔ {what}: working again after {fmt_time(time.monotonic() - trouble_since)}")
            return res
        except tgerr.FloodError as e:
            wait = getattr(e, "seconds", 30) + 2
            log(f"⏳ Telegram flood limit during {what}: waiting {fmt_time(wait)}", "WARN")
            await asyncio.sleep(wait)
        except tgerr.FileReferenceExpiredError:
            expired += 1
            if refresh and expired <= 3:
                log(f"{what}: file reference expired, refreshing the message ({expired}/3)", "WARN")
                await refresh()
                continue
            raise MessageCopyError("file reference expired and could not be refreshed")
        except FATAL_RPC as e:
            raise FatalError(f"{type(e).__name__} during {what}: {e}") from e
        except (ServerError, OSError, asyncio.TimeoutError) as e:
            if isinstance(e, OSError) and not is_network_oserror(e):
                raise FatalError(f"local error during {what}: {type(e).__name__}: {e}") from e
            now = time.monotonic()
            trouble_since = trouble_since if trouble_since is not None else now
            log(f"✖ {what} failed: {type(e).__name__}: {e}. Retrying in {fmt_time(delay)} "
                f"(trouble for {fmt_time(now - trouble_since)}, will keep retrying)", "WARN")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF)
            await reconnect_if_needed()
            if landed and await safe_landed(landed):
                log(f"{what}: the previous attempt actually went through - not repeating it")
                return None
        except RPCError as e:
            raise MessageCopyError(f"{type(e).__name__}: {e}") from e


async def safe_landed(landed):
    while True:
        try:
            return await landed()
        except (ServerError, OSError, asyncio.TimeoutError):
            await asyncio.sleep(5)
            await reconnect_if_needed()


class Tracker:
    """Remembers the newest message id in TARGET, to detect posts that 'landed'."""

    def __init__(self):
        self.last = 0

    async def newest(self):
        msgs = await client.get_messages(TARGET, limit=1)
        return msgs[0].id if msgs else 0

    async def init(self):
        self.last = await with_retry(self.newest, "reading the target channel")

    def note(self, res):
        if res is None:
            return
        ids = [m.id for m in (res if isinstance(res, list) else [res])]
        self.last = max([self.last] + ids)

    async def landed(self):
        cur = await self.newest()
        if cur > self.last:
            self.last = cur
            return True
        return False


# ------------------------ local files / progress -----------------------
def remove_local(*msg_ids):
    """Delete every local file belonging to these messages. Retries on lock errors."""
    for mid in msg_ids:
        for p in glob.glob(os.path.join(TMP_DIR, f"{mid}.*")):
            for attempt in range(5):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                    break
                except OSError as e:
                    if attempt == 4:
                        log(f"could not delete {p}: {e}", "WARN")
                    else:
                        time.sleep(1)


def clean_tmp():
    for name in os.listdir(TMP_DIR):
        p = os.path.join(TMP_DIR, name)
        if os.path.isfile(p):
            try:
                os.remove(p)
            except OSError as e:
                log(f"could not delete {p}: {e}", "WARN")


class Progress:
    """Estimated completion of the range. Weight = bytes + MSG_WEIGHT per message."""

    def __init__(self, done_msgs, done_bytes, total_msgs, total_bytes):
        self.done_msgs, self.done_bytes = done_msgs, done_bytes
        self.total_msgs, self.total_bytes = total_msgs, total_bytes
        self.session_msgs = self.session_bytes = 0
        self.placeholders = self.ignored = 0
        self.t0 = time.monotonic()

    @staticmethod
    def _w(msgs, nbytes):
        return nbytes + msgs * MSG_WEIGHT

    def add(self, msgs, nbytes):
        self.done_msgs += msgs
        self.done_bytes += nbytes
        self.session_msgs += msgs
        self.session_bytes += nbytes

    def pct(self):
        total = self._w(self.total_msgs, self.total_bytes)
        return 100.0 if total <= 0 else min(100.0, 100.0 * self._w(self.done_msgs, self.done_bytes) / total)

    def eta(self):
        elapsed = time.monotonic() - self.t0
        sess = self._w(self.session_msgs, self.session_bytes)
        if sess <= 0 or elapsed < 5:
            return "ETA ?"
        left = self._w(self.total_msgs, self.total_bytes) - self._w(self.done_msgs, self.done_bytes)
        return f"ETA {fmt_time(left / (sess / elapsed))}"

    def line(self):
        return (f"{self.pct():5.1f}% | {self.done_msgs:,}/{self.total_msgs:,} msgs | "
                f"{fmt_size(self.done_bytes)}/{fmt_size(self.total_bytes)} | {self.eta()}")


# ---------------------- download / upload one unit ---------------------
def pick_thumb(m):
    """Best thumbnail of a file that Telegram accepts as a custom thumb (JPEG, <=320px)."""
    thumbs = list(getattr(m.document, "thumbs", None) or [])
    sized = [t for t in thumbs if getattr(t, "w", None) and getattr(t, "h", None)]
    ok = [t for t in sized if t.w <= 320 and t.h <= 320]
    if ok:
        return max(ok, key=lambda t: t.w * t.h)
    if sized:
        return min(sized, key=lambda t: t.w * t.h)
    return thumbs[-1] if thumbs else None


async def download_thumb(m):
    """Download the file's own thumbnail. A missing thumbnail never blocks the copy."""
    if not m.document:
        return None
    t = pick_thumb(m)
    if t is None:
        return None
    path = os.path.join(TMP_DIR, f"{m.id}.thumb.jpg")
    try:
        saved = await with_retry(lambda: client.download_media(m, file=path, thumb=t),
                                 f"thumbnail of message {m.id}")
    except MessageCopyError as e:
        log(f"no thumbnail copied for message {m.id}: {e.reason} (the file itself is fine)", "WARN")
        return None
    return saved if saved and os.path.exists(saved) else None


async def download_one(m):
    ext = (m.file.ext if m.file and m.file.ext else None) or (".jpg" if m.photo else ".bin")
    path = os.path.join(TMP_DIR, f"{m.id}{ext}")
    holder = [m]
    size = m.file.size if m.file and m.file.size else 0

    async def refresh():
        fresh = await client.get_messages(SOURCE, ids=m.id)
        if fresh:
            holder[0] = fresh

    log(f"⬇ downloading message {m.id} ({kind_name(m)}, {fmt_size(size)})")
    t0 = time.monotonic()
    saved = await with_retry(lambda: client.download_media(holder[0], file=path),
                             f"download of message {m.id}", refresh=refresh)
    if not saved or not os.path.exists(saved):
        raise MessageCopyError("Telegram returned no media (removed or inaccessible)", counts=False)
    dt = max(time.monotonic() - t0, 1e-6)
    thumb = await download_thumb(m)
    log(f"⬇ downloaded  message {m.id} in {fmt_time(dt)} ({fmt_size(os.path.getsize(saved) / dt)}/s)"
        f"{' + thumbnail' if thumb else ''}")
    return saved, thumb


async def prepare(unit, text_only, max_upload, dl_lock):
    """Download everything a unit needs. Returns [(file path, thumbnail path or None), ...]."""
    if text_only or describe(unit[0])[0] == "text":
        return []
    for m in unit:
        if m.file and m.file.size and m.file.size > max_upload:
            raise MessageCopyError(
                f"file is {fmt_size(m.file.size)}, larger than the {fmt_size(max_upload)} "
                f"Telegram upload limit for this account", counts=False)
    async with dl_lock:                  # ONE download at a time, strictly in message order
        files = []
        for m in unit:
            files.append(await download_one(m))
        return files


def upload_bar(text):
    t0, last, mile = time.monotonic(), [0.0], [0]

    def cb(current, total):
        if not total:
            return
        now = time.monotonic()
        pct = current * 100 / total
        if IS_TTY:
            if now - last[0] < 0.4 and current < total:
                return
            last[0] = now
            speed = current / max(now - t0, 1e-6)
            live(f"⬆ {text}  {pct:5.1f}%  {fmt_size(current)}/{fmt_size(total)}  {fmt_size(speed)}/s")
        elif total >= 50e6 and int(pct // 25) > mile[0] and pct < 100:
            mile[0] = int(pct // 25)
            log(f"⬆ {text}  {pct:.0f}%  ({fmt_size(current)}/{fmt_size(total)})")

    return cb


async def send_album(unit, files, cm, long_caption, reply_to, cb):
    """Send an album with the original attributes AND thumbnails.

    Telethon's send_file(list) cannot attach thumbnails, so each file is uploaded here
    and the album is sent with one SendMultiMedia request.
    """
    total = sum(os.path.getsize(p) for p, _ in files) or 1
    try:
        media, base = [], 0
        for m, (path, thumb) in zip(unit, files):
            fh = await client.upload_file(
                path, progress_callback=lambda cur, _t, b=base: cb(b + cur, total))
            if m.photo:
                r = await client(UploadMediaRequest(TARGET, media=InputMediaUploadedPhoto(file=fh)))
                fm = utils.get_input_media(r.photo)
            else:
                th = await client.upload_file(thumb) if thumb else None
                r = await client(UploadMediaRequest(TARGET, media=InputMediaUploadedDocument(
                    file=fh, mime_type=m.document.mime_type, attributes=m.document.attributes,
                    thumb=th, force_file=force_doc(m))))
                fm = utils.get_input_media(r.document)
            keep = cm is not None and m is cm and not long_caption
            media.append(InputSingleMedia(
                media=fm, random_id=int.from_bytes(os.urandom(8), "big", signed=True),
                message=cm.raw_text if keep else "", entities=cm.entities if keep else None))
            base += os.path.getsize(path)
    except (TypeError, AttributeError) as e:     # nothing was posted yet: safe to fall back
        log(f"album with thumbnails not possible ({type(e).__name__}: {e}); sending it with "
            f"Telethon's plain album call (no thumbnails)", "WARN")
        caps = [""] * len(unit)
        if cm and not long_caption:
            caps[unit.index(cm)] = cm.text
        return await client.send_file(
            TARGET, [p for p, _ in files], caption=caps,
            force_document=all(force_doc(m) for m in unit),
            reply_to=reply_to, progress_callback=cb)

    result = await client(SendMultiMediaRequest(
        peer=TARGET, multi_media=media,
        reply_to=InputReplyToMessage(reply_to) if reply_to else None))
    return [u.message for u in getattr(result, "updates", [])
            if isinstance(u, (UpdateNewMessage, UpdateNewChannelMessage))
            and getattr(u, "message", None) is not None]


async def send_unit(unit, paths, text_only, limit, tracker, reply_to=None):
    first = unit[0]

    # plain text message
    if describe(first)[0] == "text":
        res = await with_retry(
            lambda: client.send_message(
                TARGET, first.raw_text,
                formatting_entities=first.entities,     # exact formatting, no markdown round-trip
                link_preview=bool(first.web_preview),
                reply_to=reply_to,
            ),
            f"sending message {first.id}", tracker.landed,
        )
        tracker.note(res)
        return

    cm = caption_msg(unit)
    long_caption = bool(cm) and len(cm.raw_text) > limit
    cb = upload_bar(unit_label(unit))

    if not text_only:
        if len(unit) == 1:
            m = first
            res = await with_retry(
                lambda: client.send_file(
                    TARGET, paths[0][0], thumb=paths[0][1],
                    caption="" if long_caption or not cm else cm.raw_text,
                    formatting_entities=None if long_caption or not cm else cm.entities,
                    attributes=m.document.attributes if m.document else None,
                    supports_streaming=True,
                    force_document=force_doc(m),
                    voice_note=bool(m.voice),
                    video_note=bool(m.video_note),
                    reply_to=reply_to,
                    progress_callback=cb,
                ),
                f"upload of message {m.id}", tracker.landed,
            )
        else:
            res = await with_retry(
                lambda: send_album(unit, paths, cm, long_caption, reply_to, cb),
                f"album upload (messages {unit[0].id}-{unit[-1].id})", tracker.landed,
            )
        tracker.note(res)

    # caption too long for a media caption -> send it as a follow-up text message
    if long_caption:
        res = await with_retry(
            lambda: client.send_message(
                TARGET, cm.raw_text, formatting_entities=cm.entities,
                link_preview=False, reply_to=reply_to,
            ),
            f"caption text of message {cm.id}", tracker.landed,
        )
        tracker.note(res)


async def send_placeholder(unit, reason, tracker, reply_to):
    text = f"{MARKER} {unit_label(unit)}: {reason}"[:4000]
    try:
        res = await with_retry(
            lambda: client.send_message(TARGET, text, link_preview=False, reply_to=reply_to),
            f"posting the placeholder for message {unit[0].id}", tracker.landed,
        )
    except MessageCopyError as e:
        raise FatalError(f"could not even post a placeholder: {e.reason}") from e
    tracker.note(res)


# ------------------------------ commands -------------------------------
def parse_ref(value):
    """Message number or a Telegram message link -> message id."""
    if value is None or str(value).strip() == "":
        return None
    v = str(value).strip()
    if v.isdigit():
        return int(v)
    m = re.search(r"/c/(\d+)", v)
    if m and int("-100" + m.group(1)) != SOURCE:
        sys.exit(f"'{value}' is a link to a different channel than SOURCE.")
    nums = re.findall(r"\d+", v.split("?")[0])
    if not nums:
        sys.exit(f"Could not read a message number from '{value}'.")
    return int(nums[-1])


async def cmd_list():
    print(f"{'ID':<16} NAME")
    async for d in client.iter_dialogs():
        if d.is_channel or d.is_group:
            print(f"{d.id:<16} {d.name}")


async def cmd_topics():
    await client.get_dialogs()
    for name, ident in (("SOURCE", SOURCE), ("TARGET", TARGET)):
        ent = await client.get_entity(ident)
        if not getattr(ent, "forum", False):
            print(f"{name} ({ident}) is not a group with topics.")
            continue
        print(f"{name} ({ident}) topics:")
        for t in await list_topics(ent):
            print(f"  {t.id:<8} {t.title}")


def cmd_status():
    s = load_state()
    print(f"Source messages copied: {s['done']}")
    print(f"Last source message   : {s['last_id']}")
    print(f"Source / target       : {s['source']} -> {s['target']}")
    if "from_id" in s:
        print(f"Range                 : {s['from_id'] or 'first'} .. {s.get('to_id') or 'last'}")
    if s.get("topics"):
        print(f"Topics mapped         : {len(s['topics'])}")


async def produce(queue, live_tasks, source, start_id, max_id, text_only_id, max_upload, dl_lock):
    try:
        async for unit in iter_units(source, start_id, max_id, log_skips=True):
            text_only = unit[0].id == text_only_id
            task = asyncio.create_task(prepare(unit, text_only, max_upload, dl_lock))
            live_tasks.add(task)
            task.add_done_callback(live_tasks.discard)
            await queue.put((unit, task, text_only))
        await queue.put(None)
    except asyncio.CancelledError:
        raise
    except Exception as e:                       # hand the error to the consumer
        await queue.put(e)


async def cmd_run(args):
    state = load_state()
    if state["source"] != SOURCE or state["target"] != TARGET:
        log("progress.json belongs to different SOURCE/TARGET channels. "
            "Delete progress.json if you really want to start over.", "ERROR")
        return

    # command-line flags override FROM_MESSAGE / TO_MESSAGE from the top of the file
    from_id = parse_ref(args.from_ref if args.from_ref is not None else FROM_MESSAGE) or 0
    to_id = parse_ref(args.to_ref if args.to_ref is not None else TO_MESSAGE) or 0
    if from_id and to_id and from_id > to_id:
        log("FROM must not be after TO.", "ERROR")
        return

    # The target is filled in order starting at FROM, so FROM can't change once copying began.
    old_from = state.get("from_id")
    if state["done"] > 0 and old_from is not None and old_from != from_id:
        log(f"progress.json was recorded for a range starting at message {old_from or 'first'}, "
            f"but FROM is now {from_id or 'first'}. The target is filled in order from the old "
            f"start, so it can't be continued from a different one. Set FROM back, or use a new "
            f"empty target and delete progress.json.", "ERROR")
        return
    old_to = state.get("to_id")
    if state["done"] > 0 and old_to is not None and old_to != to_id:
        log(f"TO changed ({old_to or 'last'} -> {to_id or 'last'}); progress is re-checked "
            f"against the target.")
    state["from_id"], state["to_id"] = from_id, to_id

    min_id = from_id - 1 if from_id else 0       # iter_messages bounds are exclusive
    max_id = to_id + 1 if to_id else 0

    await client.get_dialogs()  # loads channels so private IDs resolve
    source = await client.get_entity(SOURCE)
    target = await client.get_entity(TARGET)

    me = await client.get_me()
    premium = bool(getattr(me, "premium", False))
    limit = 4096 if premium else 1024
    max_upload = 4_194_304_000 if premium else 2_097_152_000
    clean_tmp()

    log("=" * 70)
    log(f"Copying {SOURCE} -> {TARGET}  |  range {from_id or 'first'} .. {to_id or 'last'}  |  "
        f"account {'Premium' if premium else 'standard'} (upload limit {fmt_size(max_upload)})")

    # ---- topics (forum groups) ---------------------------------------------
    tmap = None
    if getattr(source, "forum", False):
        if not getattr(target, "forum", False):
            log("SOURCE is a group with topics, so TARGET must be a group with topics too. "
                "Create a group, enable Topics in its settings, make yourself admin and put "
                "its ID in TARGET.", "ERROR")
            return
        state.setdefault("topics", {})
        tmap = TopicMap(state["topics"], source, target)
        n_src, n_dst = await tmap.resolve()
        log(f"Topic mode: {n_src} topics in the source, {n_dst} already in the target "
            f"(missing ones are created when first needed).")

    # ---- 1. work out what is already in the target ---------------------------
    text_only_id = None
    if args.no_sync:
        start_id = max(state["last_id"], min_id)
        done_msgs, done_bytes = state["done"], 0
        if start_id > min_id:
            done_msgs, done_bytes, _ = await count_range(source, min_id, start_id + 1)
        log(f"Skipping sync, trusting progress.json (resuming after message {start_id}).")
    else:
        log("Reading the target channel to see what is already copied...")
        t0 = time.monotonic()
        try:
            last_id, done_msgs, text_only_id, tgt_count, extra, done_bytes = await sync_progress(
                source, min_id, max_id, limit, tmap)
        except SyncMismatch as e:
            log(f"STOPPED: {e}.", "ERROR")
            log("The target must be an in-order 1:1 copy of the source range (or empty). "
                "Nothing was copied and progress.json was not changed.", "ERROR")
            return
        state["last_id"], state["done"] = last_id, done_msgs
        save_state(state)
        start_id = last_id
        log(f"Target holds {tgt_count:,} messages -> {done_msgs:,} source messages already copied, "
            f"resuming after source message {last_id}  ({fmt_time(time.monotonic() - t0)})")
        if text_only_id:
            log("The long caption of the next message is still missing; it will be sent first.")
        if extra:
            log("Note: the target has more messages than this source range.", "WARN")

    # ---- 2. how much is left ---------------------------------------------------
    log("Counting what is left...")
    rem_msgs, rem_bytes, ignored = await count_range(source, start_id, max_id)
    prog = Progress(done_msgs, done_bytes, done_msgs + rem_msgs, done_bytes + rem_bytes)
    log(f"Left to copy: {rem_msgs:,} messages, {fmt_size(rem_bytes)}  |  "
        f"overall now {prog.line()}")
    if ignored:
        log(f"{ignored:,} message(s) in the remaining range can't be copied (polls, locations, "
            f"contacts, ...) and will be ignored; each is printed and listed in skipped.log.",
            "WARN")

    if args.dry_run:
        if tmap:
            per_topic = Counter()
            async for unit in iter_units(source, start_id, max_id):
                per_topic[tmap.title(topic_of(unit[0]))] += len(unit)
            log("Remaining messages per topic:")
            for name, c in per_topic.most_common():
                log(f"  {name}: {c:,}")
        log("Dry run: nothing was copied.")
        return

    # ---- 3. copy: downloads run ahead, uploads stay in order --------------------
    tracker = Tracker()
    await tracker.init()
    queue = asyncio.Queue(maxsize=max(1, args.prefetch))
    live_tasks = set()
    producer = asyncio.create_task(
        produce(queue, live_tasks, source, start_id, max_id, text_only_id, max_upload,
                asyncio.Lock()))
    consecutive_failures = 0
    last_status = time.monotonic()
    log("Starting. Stop any time with Ctrl+C; rerun to resume exactly where it stopped.")
    log("=" * 70)

    try:
        while True:
            item = await queue.get()
            if item is None:
                break
            if isinstance(item, BaseException):
                raise item
            unit, task, text_only = item
            src_tid = topic_of(unit[0]) if tmap else None
            where = f" [{tmap.title(src_tid)}]" if tmap else ""
            size = unit_bytes(unit)
            t_up, failed_reason = 0.0, None
            try:
                reply_to = None
                if tmap:
                    dst = await tmap.ensure(src_tid, state)
                    reply_to = dst if dst != 1 else None
                try:
                    await task
                    paths = task.result()
                    t1 = time.monotonic()
                    await send_unit(unit, paths, text_only, limit, tracker, reply_to)
                    t_up = time.monotonic() - t1
                    consecutive_failures = 0
                except MessageCopyError as e:
                    failed_reason = e.reason
                    log(f"✖ COULD NOT COPY {unit_label(unit)}{where}: {e.reason}", "ERROR")
                    append_file(FAILED_LOG, f"{unit_label(unit)}{where}: {e.reason}")
                    await send_placeholder(unit, e.reason, tracker, reply_to)
                    prog.placeholders += 1
                    consecutive_failures = consecutive_failures + 1 if e.counts else 0
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        raise FatalError(
                            f"{consecutive_failures} messages in a row were rejected by Telegram "
                            f"(last: {e.reason}). This looks like a setup problem, not bad "
                            f"messages. Fix it and rerun; progress is safe.")
            finally:
                remove_local(*(m.id for m in unit))     # always delete local files

            state["last_id"] = unit[-1].id
            state["done"] += len(unit)
            prog.add(len(unit), size)
            maybe_save(state)

            if failed_reason is None:
                speed = f" @ {fmt_size(size / t_up)}/s" if size and t_up > 0.5 else ""
                log(f"✔ {unit_label(unit)}{where}  uploaded{speed}  | {prog.line()}")
            else:
                log(f"  (placeholder posted)  | {prog.line()}", "WARN")

            if time.monotonic() - last_status >= STATUS_EVERY:
                last_status = time.monotonic()
                el = time.monotonic() - prog.t0
                log(f"--- STATUS: {prog.line()} | this session: {prog.session_msgs:,} msgs, "
                    f"{fmt_size(prog.session_bytes)} in {fmt_time(el)} "
                    f"(avg {fmt_size(prog.session_bytes / max(el, 1))}/s) | "
                    f"placeholders: {prog.placeholders} ---")

        el = time.monotonic() - prog.t0
        log("=" * 70)
        log(f"FINISHED. {prog.session_msgs:,} messages, {fmt_size(prog.session_bytes)} copied "
            f"in {fmt_time(el)}. Overall: {prog.line()}")
        if prog.placeholders:
            log(f"{prog.placeholders} message(s) could not be copied and were replaced by a "
                f"placeholder - see {FAILED_LOG}", "WARN")
        if os.path.exists(SKIPPED_LOG):
            log(f"Ignored (uncopyable) messages are listed in {SKIPPED_LOG}", "WARN")
    finally:
        producer.cancel()
        for t in list(live_tasks):
            t.cancel()
        await asyncio.gather(producer, *list(live_tasks), return_exceptions=True)
        clean_tmp()
        save_state(state)


def parse_args():
    p = argparse.ArgumentParser(
        description="Copy every message of SOURCE into TARGET by download + re-upload, in order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "commands:\n"
            "  run      copy messages (default); syncs with the target first, so it always resumes\n"
            "  list     print your channels/chats with their IDs\n"
            "  topics   print the topics of SOURCE and TARGET (forum groups)\n"
            "  status   show what progress.json says\n"
            "\n"
            "examples:\n"
            "  python copier.py run --dry-run\n"
            "  python copier.py run\n"
            "  python copier.py run --from https://t.me/c/4364866470/1200\n"
            "  python copier.py run --from 1200 --to 1850\n"
            "\n"
            "LINK_OR_ID: use 'Copy Message Link' in Telegram, or just the message number.\n"
            "Settings (credentials, SOURCE, TARGET, FROM_MESSAGE, TO_MESSAGE): top of the script.\n"
            "Log files: copier.log (everything), skipped.log (ignored), failed.log (placeholders).\n"
            "Stop any time with Ctrl+C; running again resumes exactly where it stopped."
        ),
    )
    p.add_argument("command", nargs="?", default="run", choices=["run", "list", "topics", "status"],
                   help="run (default), list, topics, or status")
    p.add_argument("--from", dest="from_ref", metavar="LINK_OR_ID",
                   help="first message to copy (inclusive); overrides FROM_MESSAGE")
    p.add_argument("--to", dest="to_ref", metavar="LINK_OR_ID",
                   help="last message to copy (inclusive); overrides TO_MESSAGE")
    p.add_argument("--dry-run", action="store_true", help="sync and report, copy nothing")
    p.add_argument("--no-sync", action="store_true", help="trust progress.json")
    p.add_argument("--prefetch", type=int, default=PREFETCH,
                   help="messages that may wait ahead of the upload (downloaded one at a time, in order)")
    return p.parse_args()


async def main(args):
    global client
    if args.command == "status":
        cmd_status()
        return
    if not API_ID or not API_HASH:
        log("TG_API_ID / TG_API_HASH are not set.", "ERROR")
        return
    if args.command in ("run", "topics") and not (SOURCE and TARGET):
        log("TG_SOURCE / TG_TARGET are not set.", "ERROR")
        return

    string_session = os.environ.get("TG_SESSION", "").strip()
    client = TelegramClient(StringSession(string_session) if string_session else SESSION_NAME,
                            API_ID, API_HASH, flood_sleep_threshold=120,
                            connection_retries=5, retry_delay=3, auto_reconnect=True)
    if string_session:                       # unattended (GitHub Actions): never ask for a login
        await client.connect()
        if not await client.is_user_authorized():
            raise FatalError("TG_SESSION is not logged in (revoked or wrong). "
                             "Run make_session.py again and update the secret.")
    else:
        await client.start()  # asks phone number + login code the first time
    try:
        if args.command == "list":
            await cmd_list()
        elif args.command == "topics":
            await cmd_topics()
        else:
            await cmd_run(args)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    arguments = parse_args()
    try:
        asyncio.run(main(arguments))
    except KeyboardInterrupt:
        log("Stopped. Run the script again - it re-reads the target and resumes exactly.")
    except FatalError as e:
        log(f"FATAL: {e}", "ERROR")
        log("Nothing is lost: fix the problem and run the script again to resume.", "ERROR")
        sys.exit(1)
    except Exception:
        log("UNEXPECTED ERROR (please report this):\n" + traceback.format_exc(), "ERROR")
        sys.exit(1)