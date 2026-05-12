# Raspberry Pi 5 Bootstrap Runbook

End-to-end procedure for deploying the headline-alerter on a fresh Pi 5 with NVMe storage and remote (Tailscale) access. Allow ~90 minutes from "parts on the desk" to "WhatsApp alerts arriving from the Pi."

## Hardware checklist

Before starting, confirm you have:

- [ ] Raspberry Pi 5 (8GB recommended)
- [ ] Crucial P310 500GB NVMe (or equivalent M.2 2280 NVMe drive)
- [ ] Argon ONE V3 M.2 NVMe case (Pi 5 specific — not the Pi 4 "Argon ONE M.2" version)
- [ ] Official Raspberry Pi 27W USB-C power supply (or equivalent 5V/5A)
- [ ] microSD card (16–32GB minimum; A1 or A2 rated)
- [ ] Ethernet cable (recommended) or known-working WiFi credentials
- [ ] A laptop with a microSD reader (or USB SD reader) running Pi Imager

## Pre-flight (do these now, before parts arrive)

### 1. Install Raspberry Pi Imager

Download from <https://www.raspberrypi.com/software/> for your laptop's OS. ~50MB.

### 2. Create a Tailscale account

Tailscale gives you remote SSH access to the Pi from anywhere (your phone, laptop, anywhere on Wi-Fi or cellular) without exposing the Pi to the public internet.

Sign up at <https://login.tailscale.com/start>. Free tier is generous (3 users, 100 devices). You can sign in with Google / GitHub / Microsoft — no separate password to remember.

### 3. (Optional) Generate an SSH key on your laptop

This runbook uses **password authentication** for SSH — simpler and forgiving if anything goes wrong. You can skip this step.

If you'd rather use key-based auth later (more secure, no password typing), generate one with:

```bash
ssh-keygen -t ed25519 -C "your-email@example.com"
cat ~/.ssh/id_ed25519.pub
```

You can add the key to the Pi after first boot via `ssh-copy-id <user>@headline-alerter.local`. The runbook's Phase 2 doesn't bake the key in.

---

## Phase 1 — Hardware assembly

### 1.1 Install the NVMe drive into the Argon V3 case

Follow the Argon V3 manual (paper insert in the case box). Summary:

1. Remove the Argon V3 top cover (a few screws on the bottom).
2. Slot the NVMe drive into the M.2 2280 slot at an angle, push down, secure with the supplied tiny screw.
3. Don't connect the Pi yet — leave the bottom open.

### 1.2 Mount the Pi 5 onto the case base

1. Place the Pi 5 PCB face-down onto the Argon base — the GPIO header lines up with the case's pin block, and the small PCIe FPC cable plugs into the Pi's PCIe connector.
2. Secure with the supplied screws.
3. The PCIe FPC cable connects the Pi's PCIe port to the NVMe slot. Make sure both ends are firmly seated.

### 1.3 Reassemble

Replace the top cover. Argon V3's fan + heatsink contact pads will press against the SoC and PMIC chips when the lid screws down — make sure no thermal pad film/cover is left on.

Don't power it on yet — flash the microSD first.

---

## Phase 2 — Flash microSD with Pi OS Lite (headless config)

### 2.1 Open Raspberry Pi Imager on your laptop

Insert the microSD card (if it came pre-installed with an OS, that's fine — Pi Imager will overwrite).

### 2.2 Choose OS

Click **CHOOSE OS** → **Raspberry Pi OS (other)** → **Raspberry Pi OS Lite (64-bit)**.

(Lite = no desktop environment. We're running a headless server.)

### 2.3 Choose storage

Click **CHOOSE STORAGE** → select your microSD card. **Triple-check** you're selecting the SD card and not your laptop's SSD.

### 2.4 Configure (the killer feature)

Click **NEXT** → "Would you like to apply OS customization settings?" → **EDIT SETTINGS**.

Fill in:

**General tab:**
- **Set hostname**: `headline-alerter` (or whatever you like; this is how you'll find the Pi on the network — `ssh user@headline-alerter.local`)
- **Set username and password**: pick a username (e.g. `pi` or your own) and a strong password
- **Configure wireless LAN**: enable and enter your WiFi SSID + password (skip if using Ethernet only — but enabling WiFi as a backup is wise)
- **Set locale settings**: your timezone and keyboard layout

**Services tab:**
- **Enable SSH**: ✅ check this
- **Use public-key authentication only**: ❌ **leave unchecked**. SSH will use the password you set above. (If you'd rather use key-only auth, check this and paste your `~/.ssh/id_ed25519.pub` here — stronger but you're locked out if you lose the key file.)

Click **SAVE** → back at the main screen, click **WRITE**.

The flash takes 2–5 minutes. Verification adds another minute.

When done, eject the card from your laptop.

---

## Phase 3 — First boot and SSH access

### 3.1 Power up the Pi

1. Insert the microSD card into the Pi (slot is on the underside of the board — accessible through a small slot in the Argon V3 case).
2. Plug in Ethernet (recommended) and/or rely on the WiFi config you baked in.
3. Plug in the USB-C power. The Pi 5 should boot within ~30 seconds.

The Argon V3 has a power button on the side — you can use it later for safe shutdown / wake. For first boot, just plugging in power should boot it automatically.

### 3.2 SSH in from your laptop

Find the Pi's IP address:

- **Easiest**: try `ssh <username>@headline-alerter.local` from your laptop. mDNS should resolve the hostname you set in Pi Imager.
- **Fallback**: log into your home router's admin page, look for a device named `headline-alerter`. Note the IP and use that.
- **Power user**: `nmap -p 22 192.168.1.0/24` (replace with your subnet) and look for the Pi's MAC vendor (Raspberry Pi Trading).

```bash
ssh <username>@headline-alerter.local
```

You'll be prompted for the password you set in Pi Imager. (First connection asks you to confirm the Pi's host fingerprint — type `yes`.)

If SSH refuses with "permission denied" repeatedly after you typed the right password, the headless config didn't apply — re-flash the SD with Pi Imager, or attach a monitor + USB keyboard for one-time setup.

### 3.3 First-time updates

Once SSHed in:

```bash
sudo apt update && sudo apt full-upgrade -y
sudo reboot
```

Wait ~30 seconds, SSH back in.

---

## Phase 4 — Migrate boot from microSD to NVMe

The Pi 5 can boot directly from NVMe with a recent bootloader. We need to:
1. Update the bootloader (if needed)
2. Tell the bootloader to prefer NVMe
3. Copy the OS from the SD card to the NVMe
4. Reboot from NVMe

### 4.1 Verify the NVMe is detected

```bash
lsblk
```

Expected: see both `mmcblk0` (the SD card, ~30GB) and `nvme0n1` (the NVMe drive, ~500GB).

If you only see `mmcblk0`, the PCIe FPC cable may not be seated correctly or the Argon V3 case isn't engaging the connector. Power off, recheck the cable, retry.

### 4.2 Update bootloader to the latest version

```bash
sudo apt install -y rpi-eeprom
sudo rpi-eeprom-update -a
sudo reboot
```

Wait ~30 seconds, SSH back in.

### 4.3 Set NVMe as the preferred boot device

```bash
sudo raspi-config
```

Navigate: **6 Advanced Options** → **A4 Boot Order** → **B2 NVMe Boot** → confirm.

Exit raspi-config (says "would you like to reboot?" — say **No**, we're not done yet).

### 4.4 Enable PCIe Gen 3 (optional but recommended)

Pi 5's PCIe lane defaults to Gen 2 for stability. Most NVMe drives (P310 included) work fine at Gen 3, doubling theoretical bandwidth.

```bash
sudo nano /boot/firmware/config.txt
```

Add this line at the bottom (in the `[all]` section):

```
dtparam=pciex1_gen=3
```

Save (Ctrl-O, Enter, Ctrl-X). Don't reboot yet.

### 4.5 Clone the SD card's OS to the NVMe

We'll use **rpi-clone** — a Bash script that handles the dance of cloning the running OS to a target disk, updating partition UUIDs, and making the new disk bootable.

> ⚠️ **Heads-up about rpi-clone's status.** The tool's original maintainer (billw2) stopped pushing updates around 2022, and it was subsequently removed from the Raspberry Pi OS apt repositories. The script still works correctly for this one-shot SD-to-NVMe migration — we just install it from its GitHub source rather than via `apt`. After the migration completes you'll never run it again. If you'd rather avoid the unmaintained-tool dependency, see the **alternative path** at the end of this step.

Install rpi-clone from GitHub:

```bash
sudo apt install -y git
cd /tmp
git clone https://github.com/billw2/rpi-clone.git
cd rpi-clone
sudo cp rpi-clone rpi-clone-setup /usr/local/sbin/
sudo chmod +x /usr/local/sbin/rpi-clone /usr/local/sbin/rpi-clone-setup
cd ~
```

Verify:

```bash
which rpi-clone   # should print /usr/local/sbin/rpi-clone
```

Now clone the running OS to the NVMe drive:

```bash
sudo rpi-clone nvme0n1
```

When prompted to confirm formatting + cloning, type `yes` (the full word, not just `y`) and press Enter. The clone takes ~5–15 minutes.

The tool will:
- Format the NVMe drive (it'll ask for confirmation)
- Copy the running OS, partitions and all
- Update the new disk's `cmdline.txt` and `/etc/fstab` to use NVMe-relative UUIDs

#### Alternative path (no rpi-clone, official tools only)

If you have a **USB-M.2 NVMe adapter** (~£15) and you'd rather avoid the unmaintained tool entirely:

1. Power down the Pi (`sudo poweroff`).
2. Unscrew the Argon ONE V3 case, remove the NVMe SSD from the M.2 slot.
3. Plug the NVMe into your laptop via the USB-M.2 NVMe adapter (different from a SATA adapter — make sure it's NVMe-compatible).
4. Open **Raspberry Pi Imager** on your laptop. Choose **Raspberry Pi OS Lite (64-bit)**, target the NVMe, fill in the same headless config (hostname `headline-alerter`, SSH enabled, password, WiFi, etc.). **WRITE.**
5. Eject the NVMe, reseat it in the Argon V3, screw the case back together.
6. Power the Pi — it boots directly from the NVMe (no migration needed). **Skip Phase 4.6** and continue with Phase 5.

This is the cleaner long-term approach but requires the USB-NVMe adapter and a disassembly step mid-bootstrap.

### 4.6 Power down, remove SD card, power up — boot from NVMe

```bash
sudo poweroff
```

Wait until the green LED stops blinking. Unplug power. Remove the microSD card from the Pi (keep it safe — it's now a recovery card). Plug power back in.

The Pi should boot from NVMe in ~10 seconds. SSH back in:

```bash
ssh <username>@headline-alerter.local
```

Confirm you're on NVMe:

```bash
lsblk
df -h /
```

Root filesystem should be mounted from a partition on `nvme0n1`, and you'll have ~500GB free.

---

## Phase 5 — Install Tailscale

Remote access from anywhere (phone, laptop on a coffee shop WiFi) without exposing the Pi to the internet.

### 5.1 Install Tailscale

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

The command will print a URL like `https://login.tailscale.com/a/abc123...`. Open it in your laptop's browser, sign in (Google / GitHub / Microsoft), and approve the Pi.

### 5.2 Note the Pi's Tailscale IP

```bash
tailscale ip -4
```

Output: `100.x.x.x`. This is the Pi's permanent Tailscale address — it works from anywhere in the world as long as both devices are signed into your Tailnet.

### 5.3 Test from your phone

Install the Tailscale app on your phone, sign in with the same account, enable VPN. From the phone:

- Open a terminal app (Termux on Android, Blink/iSH on iOS)
- `ssh <username>@100.x.x.x` (or `ssh <username>@headline-alerter` if Tailscale's MagicDNS is enabled — it usually is by default)

You should be SSHed into the Pi over your phone's cellular connection. Magic.

---

## Phase 6 — Install Docker

Pi OS doesn't ship with Docker. Use the official convenience script:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

Log out and back in (`exit`, then SSH again) for the group change to take effect.

Verify:

```bash
docker version
docker compose version
```

Both should print version numbers. If `docker compose version` fails, install the plugin:

```bash
sudo apt install -y docker-compose-plugin
```

---

## Phase 7 — Deploy the headline-alerter

### 7.1 Clone the repo

```bash
mkdir -p ~/projects && cd ~/projects
git clone https://github.com/georgelinrepo/headline-alerter.git
cd headline-alerter
```

(If the repo is private, you'll need to set up a deploy key or use HTTPS with a personal access token. For private repos, use SSH:

```bash
ssh-keygen -t ed25519 -C "headline-alerter@pi" -f ~/.ssh/id_ed25519_pi
# Copy ~/.ssh/id_ed25519_pi.pub into GitHub → Settings → SSH and GPG keys
git clone git@github.com:georgelinrepo/headline-alerter.git
```

)

### 7.2 Set up `.env`

```bash
cp .env.example .env
nano .env
```

Fill in:

- `POSTGRES_PASSWORD` — pick a strong password (used internally by Docker Compose; never exposed)
- `ANTHROPIC_API_KEY` — from `console.anthropic.com` (same key you use on your laptop)
- `TWILIO_ACCOUNT_SID` — from Twilio Console
- `TWILIO_AUTH_TOKEN` — from Twilio Console
- `TWILIO_FROM` — `whatsapp:+14155238886` (Twilio Sandbox)
- `ALERT_RECIPIENT` — `whatsapp:+44...` (your phone, full international format)
- All other defaults are fine

Save (Ctrl-O, Enter, Ctrl-X).

### 7.3 Confirm the WhatsApp Sandbox is opted in

If you haven't done so already on this phone+account:

1. Twilio Console → Develop → Messaging → Try it out → Send a WhatsApp message → note the join code
2. From your phone's WhatsApp, send `join <your-code>` to `+1 415 523 8886`
3. Confirm receipt of `Joined <your-code>...`

### 7.4 Build and start

```bash
docker compose up -d
```

First time: builds the 3 service images (ingestor, scorer, alerter). ~5–10 minutes on Pi 5 (slower than your laptop because of arm64 builds + slower CPU).

Then:

```bash
docker compose ps
```

Expected: `kafka` + `postgres` healthy; `kafka-init` + `migrate` Exited (0); `ingestor-cnbc`, `scorer`, `alerter` all Up.

### 7.5 Watch the first events arrive

```bash
docker compose logs -f ingestor-cnbc scorer alerter
```

Within ~60 seconds, you should see:
- `ingestor-cnbc`: `{"event": "emitted", ...}`
- `scorer`: `{"event": "scored", ..., "latency_ms": ...}`
- `alerter`: `{"event": "alerted", ...}` (only for events scoring ≥ 4)

If a real CNBC headline that scores ≥ 4 arrives, your phone should buzz with a WhatsApp message — sourced from the Pi this time, not your laptop.

### 7.6 Run the smoke test (optional but recommended)

```bash
set -a; source .env; set +a
python3 tools/scorer_smoke.py
python3 tools/alerter_smoke.py
```

Both should print `OK — Phase ... smoke test passed`. The alerter smoke fires one real WhatsApp message — you'll feel it.

---

## Phase 8 — Operational notes

### Auto-start on reboot

`restart: unless-stopped` is already set on every Compose service. After `sudo reboot`, all services come back up automatically.

To prove this, reboot the Pi and watch:

```bash
sudo reboot
# wait ~30s, SSH back in
docker compose ps
```

All services should be Up again with no human intervention.

### Stop the laptop's instance now

Once you've confirmed the Pi is delivering alerts, stop your laptop's stack so you don't get duplicate WhatsApp messages:

```bash
# On laptop:
cd /c/Projects/headline-alerter
docker compose stop
```

### Useful commands

```bash
# Live tail of all service logs
docker compose logs -f

# Just the alerter
docker compose logs -f alerter

# DB query: how many events have been scored today?
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT count(*) FROM events_archive WHERE ts_ingested > NOW() - INTERVAL '24 hours';"

# Live view of recent events (port forward needed if accessing from laptop)
python3 tools/tail.py

# Restart a specific service
docker compose restart scorer

# Update the codebase (after pushing changes from laptop)
git pull
docker compose build       # rebuild service images
docker compose up -d       # roll the new images in
```

### Disk usage and log retention

Kafka log segments + Postgres data live in named volumes inside Docker:

```bash
docker system df
```

Kafka topics retain messages for 30 days (configured in `docker-compose.yml`). After the first month, expect ~1.5–2 GB of Kafka data steady-state. Postgres grows ~1 MB/day. Plenty of headroom on the 500GB NVMe.

### Backups

For a personal project, manual backups suffice:

```bash
# Dump the events_archive (the only thing you'd want to keep)
docker compose exec postgres pg_dump -U rates -d rates -t events_archive \
  -t alert_history > ~/headline-alerter-backup-$(date +%F).sql
```

Copy off-Pi via `scp` if you want.

### Tailscale-only access (no SSH over local network)

Once Tailscale is set up, you can disable SSH on the local network if you want (paranoid mode). Edit `/etc/ssh/sshd_config`:

```
ListenAddress 100.x.x.x   # only the Tailscale IP
```

This locks SSH to your Tailnet only. Skip if you'd rather keep local-LAN SSH for emergencies.

---

## Troubleshooting

### Pi won't boot from NVMe

- Did the bootloader update succeed in Phase 4.2? Run `sudo rpi-eeprom-update` to check current version.
- Is the boot order set correctly? `sudo raspi-config` → Advanced → Boot Order → confirm NVMe is first.
- Is the NVMe detected? `lsblk` should show `nvme0n1`.

### Pi shows "under-voltage" warnings

- Are you using the official 27W USB-C PSU? Generic 5V/3A supplies aren't enough for Pi 5 + NVMe under load.
- Try a different USB-C cable — some thin cables can't deliver enough current even with a good PSU.
- `vcgencmd get_throttled` returns a non-zero hex value if throttling has occurred.

### Docker images take forever to build

Pi 5 is slower than a laptop. First build: 5–15 minutes is normal. Subsequent builds use cache and are ~30 seconds.

If you want even faster iteration, you can build the images on your laptop (where it's fast), push to a registry, and pull from the Pi:

```bash
# On laptop
docker compose build --push   # push to your private registry

# On Pi
docker compose pull
docker compose up -d
```

For a personal project, this is overkill — building on the Pi is fine.

### Tailscale disconnects after a while

Run `tailscale status` to check. If it says "no peers visible", your Tailnet may have hit a transient issue — `sudo systemctl restart tailscaled` usually fixes it.

### WhatsApp messages stop arriving

Most likely the WhatsApp Sandbox 24-hour window expired. Reply anything to the sandbox number on your phone to reopen. Check `events.dlq`:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic events.dlq --from-beginning | grep alerter_whatsapp_template
```

If you see those messages, that's the cause.

---

## What's next after this is running

Phase 2+ of the headline-alerter (more sources): BLS RSS, Treasury RSS, Truth Social, X. Each adds curation breadth without changing the architecture.

Or Phase 1c: a browser dashboard you can open at `http://headline-alerter.local:8080/` (or `http://100.x.x.x:8080/` over Tailscale from your phone) with live event streaming.

Or just run it for a month and see what real signals come through before adding more.
