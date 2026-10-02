#!/usr/bin/env python3
"""Run this ONCE on your own computer to create the login for GitHub Actions.

    pip install telethon
    python make_session.py

It asks for your API ID / API hash, phone number, the code Telegram sends you
(and your 2FA password if you have one), then prints a long text string.
Put that string in the GitHub secret TG_SESSION. Never commit it or share it:
anyone with it has full access to your Telegram account.

Use this fresh login ONLY for GitHub Actions. Do not run copier.py locally with
the same string while Actions is running, or Telegram kills the session.
"""
import getpass

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

api_id = int(input("API ID: ").strip())
api_hash = getpass.getpass("API hash (hidden): ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    me = client.get_me()
    print(f"\nLogged in as {me.first_name} (id {me.id}).")
    print("\n=== TG_SESSION (copy everything on the next line) ===")
    print(client.session.save())
    print("=== end ===")
