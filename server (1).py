# server.py  —  PortalChat v5
# Railway: reads PORT env var automatically. Push to GitHub, Railway deploys it.
import asyncio
import json
import os
import uuid
import time
from collections import defaultdict
import websockets

# ── STATE ──────────────────────────────────────────────────────────
clients: dict = {}                            # username -> websocket
user_status: dict = {}                        # username -> "online"|"idle"|"offline"
friends: dict = defaultdict(set)              # username -> set of friends
pending_friend_requests: dict = defaultdict(set)  # username -> set who sent requests

channels: dict = {"general": [], "memes": []}
typing_users: dict = defaultdict(set)

# ── HELPERS ────────────────────────────────────────────────────────
def now_ts():
    return time.time()

def make_message(user, text):
    return {
        "id": str(uuid.uuid4()), "user": user, "text": text,
        "timestamp": now_ts(), "reactions": {},
        "deleted": False, "edited": False,
    }

def dm_channel_name(a, b):
    return "dm:{}_{}".format(*sorted([a, b]))

async def safe_send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except Exception:
        pass

async def broadcast_to_all(payload):
    if not clients:
        return
    data = json.dumps(payload)
    await asyncio.gather(
        *[ws.send(data) for ws in list(clients.values())],
        return_exceptions=True
    )

async def broadcast_to_channel(channel, payload):
    await broadcast_to_all(payload)

async def send_presence(user, status):
    await broadcast_to_all({"type": "presence", "user": user, "status": status})

def serialize_message(msg):
    return {
        "id": msg["id"], "user": msg["user"], "text": msg["text"],
        "timestamp": msg["timestamp"], "deleted": msg["deleted"],
        "edited": msg.get("edited", False),
        "reactions": {e: len(u) for e, u in msg["reactions"].items() if u},
    }

def serialize_history(ch):
    return [serialize_message(m) for m in channels.get(ch, [])]

def get_public_channels():
    return [c for c in channels if not c.startswith("dm:")]

def get_friends_for(user):
    result = []
    for f in sorted(friends[user]):
        result.append({"user": f, "status": user_status.get(f, "offline"), "pending": False})
    for p in sorted(pending_friend_requests[user]):
        result.append({"user": p, "status": user_status.get(p, "offline"), "pending": True})
    return result

async def send_initial_state(user, ws):
    await safe_send(ws, {"type": "channel_list", "channels": get_public_channels()})
    await safe_send(ws, {"type": "channel_history", "channel": "general", "messages": serialize_history("general")})
    await safe_send(ws, {"type": "friend_list", "friends": get_friends_for(user)})
    await safe_send(ws, {"type": "presence_snapshot",
                         "users": [{"user": u, "status": s} for u, s in user_status.items()]})

# ── HANDLERS ───────────────────────────────────────────────────────
async def h_message(user, data):
    ch = data.get("channel", "general")
    text = data.get("text", "").strip()
    if not text:
        return
    msg = make_message(user, text)
    channels.setdefault(ch, []).append(msg)
    await broadcast_to_channel(ch, {"type": "message", "channel": ch, "message": serialize_message(msg)})

async def h_dm(user, data):
    target = data.get("to")
    text = data.get("text", "").strip()
    if not target or not text:
        return
    ch = dm_channel_name(user, target)
    channels.setdefault(ch, [])
    msg = make_message(user, text)
    channels[ch].append(msg)
    for u in [user, target]:
        if u in clients:
            await safe_send(clients[u], {
                "type": "dm_message", "channel": ch,
                "from": user, "to": target, "message": serialize_message(msg),
            })

async def h_reaction(user, data):
    ch = data.get("channel")
    mid = data.get("message_id")
    emoji = data.get("emoji")
    if not ch or not mid or not emoji:
        return
    msg = next((m for m in channels.get(ch, []) if m["id"] == mid), None)
    if not msg:
        return
    r = msg["reactions"]
    s = r.setdefault(emoji, set())
    if user in s:
        s.discard(user)
    else:
        s.add(user)
    if not s:
        r.pop(emoji, None)
    await broadcast_to_channel(ch, {
        "type": "reaction_update", "channel": ch, "message_id": mid,
        "reactions": {e: len(u) for e, u in r.items() if u},
    })

async def h_edit(user, data):
    ch = data.get("channel")
    mid = data.get("message_id")
    text = data.get("text", "").strip()
    if not ch or not mid or not text:
        return
    msg = next((m for m in channels.get(ch, []) if m["id"] == mid), None)
    if not msg or msg["user"] != user:
        return
    msg["text"] = text
    msg["edited"] = True
    await broadcast_to_channel(ch, {"type": "message_edited", "channel": ch, "message": serialize_message(msg)})

async def h_delete(user, data):
    ch = data.get("channel")
    mid = data.get("message_id")
    if not ch or not mid:
        return
    msg = next((m for m in channels.get(ch, []) if m["id"] == mid), None)
    if not msg or msg["user"] != user:
        return
    msg["deleted"] = True
    await broadcast_to_channel(ch, {"type": "message_deleted", "channel": ch, "message_id": mid})

async def h_typing(user, data):
    ch = data.get("channel", "general")
    typing_users[ch].add(user) if data.get("typing") else typing_users[ch].discard(user)
    await broadcast_to_channel(ch, {"type": "typing", "channel": ch, "users": list(typing_users[ch])})

async def h_friend_request(user, data):
    target = data.get("to")
    if not target or target == user or target in friends[user]:
        return
    if user in pending_friend_requests[target]:
        return
    pending_friend_requests[target].add(user)
    if target in clients:
        await safe_send(clients[target], {"type": "friend_request", "from": user})
    if user in clients:
        await safe_send(clients[user], {"type": "friend_list", "friends": get_friends_for(user)})

async def h_friend_accept(user, data):
    from_user = data.get("from")
    if not from_user or from_user not in pending_friend_requests[user]:
        return
    pending_friend_requests[user].discard(from_user)
    friends[user].add(from_user)
    friends[from_user].add(user)
    for u in [user, from_user]:
        if u in clients:
            await safe_send(clients[u], {"type": "friend_list", "friends": get_friends_for(u)})

async def h_friend_remove(user, data):
    target = data.get("user")
    if not target:
        return
    friends[user].discard(target)
    friends[target].discard(user)
    for u in [user, target]:
        if u in clients:
            await safe_send(clients[u], {"type": "friend_list", "friends": get_friends_for(u)})

async def h_create_channel(user, data):
    name = data.get("name", "").strip().lower().replace(" ", "-")
    if not name or name.startswith("dm:"):
        return
    channels.setdefault(name, [])
    await broadcast_to_all({"type": "channel_list", "channels": get_public_channels()})

# ── CONNECTION ─────────────────────────────────────────────────────
async def handle_client(ws, path=None):
    user = None
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        hello = json.loads(raw)
        if hello.get("type") != "hello" or not hello.get("user", "").strip():
            await safe_send(ws, {"type": "error", "message": "Send hello first"})
            return

        user = hello["user"].strip()

        # Kick old duplicate session
        old = clients.get(user)
        if old and old is not ws:
            await safe_send(old, {"type": "error", "message": "Logged in elsewhere"})
            try:
                await old.close()
            except Exception:
                pass

        clients[user] = ws
        user_status[user] = "online"
        friends.setdefault(user, set())
        print(f"[+] {user}  ({len(clients)} online)")

        await send_presence(user, "online")
        await send_initial_state(user, ws)

        async for raw_msg in ws:
            try:
                data = json.loads(raw_msg)
            except Exception:
                continue
            t = data.get("type")
            if   t == "message":        await h_message(user, data)
            elif t == "dm_message":     await h_dm(user, data)
            elif t == "reaction":       await h_reaction(user, data)
            elif t == "edit_message":   await h_edit(user, data)
            elif t == "delete_message": await h_delete(user, data)
            elif t == "typing":         await h_typing(user, data)
            elif t == "friend_request": await h_friend_request(user, data)
            elif t == "friend_accept":  await h_friend_accept(user, data)
            elif t == "friend_remove":  await h_friend_remove(user, data)
            elif t == "create_channel": await h_create_channel(user, data)
            elif t == "channel_history":
                ch = data.get("channel", "general")
                await safe_send(ws, {"type": "channel_history", "channel": ch, "messages": serialize_history(ch)})
            elif t == "friend_list":
                await safe_send(ws, {"type": "friend_list", "friends": get_friends_for(user)})
            elif t == "presence":
                s = data.get("status", "online")
                user_status[user] = s
                await send_presence(user, s)

    except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
        pass
    except Exception as e:
        print(f"[!] {user}: {e}")
    finally:
        if user:
            if clients.get(user) is ws:
                clients.pop(user, None)
            user_status[user] = "offline"
            print(f"[-] {user}  ({len(clients)} online)")
            await send_presence(user, "offline")
            for ch in list(typing_users):
                if user in typing_users[ch]:
                    typing_users[ch].discard(user)
                    await broadcast_to_channel(ch, {"type": "typing", "channel": ch, "users": list(typing_users[ch])})

# ── ENTRY ──────────────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"PortalChat starting — port {port}")
    async with websockets.serve(handle_client, "0.0.0.0", port,
                                ping_interval=30, ping_timeout=10):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
