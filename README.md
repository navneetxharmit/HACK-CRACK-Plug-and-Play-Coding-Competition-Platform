# HACK & CRACK — Plug-and-Play Coding Competition Platform

Host a live coding or quiz competition on localhost or your local network in under 2 minutes — no database setup or frontend coding required.

## Features
- **Zero-Config Database:** Built-in SQLite (`node:sqlite`) automatically creates tables and seeds 100 competitor accounts (`user01`–`user100`) + an admin account.
- **Real-Time Updates:** Live leaderboard, online/offline status, and admin activity feed powered by WebSockets (`ws`).
- **Admin Control Panel:** Start, end, or reset the competition, unlock/reset individual users, inspect submissions, and review flagged cheating attempts.
- **Anti-Cheat System:** First offense locks the account for admin review (spare or disqualify); second offense auto-disqualifies.
- **Single-Folder Setup:** Just `index.html`, `server.js`, and `package.json`.

## Quick Start

### Prerequisites
- **Node.js v22.5+** (recommended: Node.js 24+)

### Run Locally
```bash
npm install
npm start
```
Then open **`http://localhost:3000`** in your browser.

## Default Credentials
- **Admin:** `admin` / `adminhash@2006`
- **Competitors:** `user01` through `user100` *(username only, no password)*

> **Before hosting a real event:** Change the admin password in `server.js` and customize the competition questions inside `index.html`.
