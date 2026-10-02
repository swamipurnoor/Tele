# Telegram copier on GitHub Actions

One-time setup

1. Create a PUBLIC repo and push these files (keep the folder layout, including `.github/workflows/`).
2. On your own computer: `pip install telethon`, then `python make_session.py`. Copy the long string it prints.
3. Repo -> Settings -> Secrets and variables -> Actions -> New repository secret. Add:
   - `TG_API_ID`, `TG_API_HASH` (my.telegram.org)
   - `TG_SESSION` (the string from step 2)
   - `TG_SOURCE`, `TG_TARGET` (IDs like `-100123...`; `python copier.py list` locally shows them)
   - `LOG_PASSPHRASE` (any long random text; encrypts the log artifacts)
   - optional: `TG_FROM`, `TG_TO`
4. Actions tab -> telegram-copier -> Run workflow. Done.

After that it chains itself: each run copies for 5h30m, then starts the next one. Progress is read back
from the target channel every time, so nothing has to be saved between runs. When everything is copied it
disables itself. If it ever stops, press Run workflow again (or wait for the 6-hourly schedule).

Read the logs: download the `logs-N` artifact, then
`gpg -d logs.tar.gz.gpg | tar xz`
