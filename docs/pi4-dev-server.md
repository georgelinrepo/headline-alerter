# Raspberry Pi 4 Dev Server Bootstrap

End-to-end procedure for setting up a Pi 4 8GB as a personal dev server, accessible from the Claude mobile app via **Remote Control** (no port forwarding, no SSH tunneling required). Allow ~75 minutes from "parts on desk" to "Pi appearing in Claude app with green icon."

## What this gets you

- A Pi 4 always-on dev box.
- **Claude Code running on the Pi**, reachable from the Claude mobile app via Remote Control. Tap a session in the app → it's actually running on your Pi (so it has access to your files, can run builds, etc.) without your phone running any heavy compute.
- SSH access from anywhere via Tailscale (for system admin / git operations / occasional terminal work).
- Optional VS Code Remote-SSH from a laptop for editor-friendly dev sessions.

**What Remote Control does NOT need:** port forwarding, public IP, SSH tunneling, Tailscale. The Pi makes outbound HTTPS to Anthropic, which routes traffic from the mobile app. Works behind any residential NAT.

## Hardware checklist

- [ ] Raspberry Pi 4 (8GB recommended)
- [ ] WD Blue SATA M.2 2280 SSD (any capacity)
- [ ] Argon One M.2 case (the Pi 4 version — has a built-in USB 3.0 → SATA bridge)
- [ ] Pi 4 power supply (official 15.3W USB-C, or equivalent 5V/3A)
- [ ] microSD card (16GB+ A1/A2 rated)
- [ ] Ethernet cable (recommended) or known-working WiFi credentials
- [ ] A laptop with a microSD reader running Raspberry Pi Imager

## Pre-flight (do these now, before the Pi is on)

### 1. Install Raspberry Pi Imager

<https://www.raspberrypi.com/software/>. Free.

### 2. (Optional) Generate an SSH key on your laptop

This runbook uses **password authentication** for SSH — simpler. You can skip this step.

If you'd rather use keys later (more secure, no password typing): `ssh-keygen -t ed25519`, then `ssh-copy-id <user>@pi4dev.local` after first boot.

### 3. Sign in to the Claude mobile app

Install the Claude app:
- iOS: <https://apps.apple.com/us/app/claude-by-anthropic/id6473753684>
- Android: <https://play.google.com/store/apps/details?id=com.anthropic.claude>

Sign in with **the same Claude.ai account** as your Pro/Max subscription. This is the only "pairing" needed — Remote Control discovers your Pi automatically when the Pi authenticates with the same account.

### 4. (Optional) Create a Tailscale account

For SSH-from-anywhere. Not strictly required for Remote Control, but recommended. Sign up at <https://login.tailscale.com/start>.

---

## Phase 1 — Hardware assembly

### 1.1 Install the SATA SSD into the Argon One M.2 case

Follow the Argon One M.2 paper manual. Summary:

1. Remove the bottom plate (a few screws).
2. Slot the WD Blue SATA M.2 SSD into the M.2 connector. Note: the Argon One M.2 connector is **B+M key SATA**, not NVMe — it only accepts SATA M.2 drives.
3. Secure with the supplied screw at the far end of the M.2 slot.
4. Note the small **USB jumper** on the case board — leave it in its default position (it connects the case's internal USB 3.0 cable to the Pi's USB 3.0 port via the pigtail).

### 1.2 Seat the Pi 4 onto the case

1. Place the Pi 4 PCB face-down into the case's top half. The case's daughterboard makes contact with the Pi's GPIO header.
2. Secure with the supplied screws.
3. Close the case.
4. **Connect the small USB 3.0 pigtail loop** from the case's side to one of the Pi 4's USB 3.0 ports (the blue ones). This is what makes the internal SATA SSD appear to the Pi as a USB-attached disk.

Don't power it on yet — we flash the SD card first.

---

## Phase 2 — Flash microSD with Pi OS Lite

### 2.1 Open Raspberry Pi Imager

Insert the microSD into your laptop. If it came pre-installed with anything, Pi Imager will overwrite it.

### 2.2 Choose OS

**CHOOSE OS** → **Raspberry Pi OS (other)** → **Raspberry Pi OS Lite (64-bit)**.

Pi OS Lite = no desktop. Smaller, faster, no GUI services chewing RAM. Perfect for a dev box.

### 2.3 Choose storage

**CHOOSE STORAGE** → select the microSD card. Triple-check this is the SD card, not your laptop's drive.

### 2.4 Configure (the killer step)

**NEXT** → "Apply OS customization settings?" → **EDIT SETTINGS**.

**General tab:**
- **Hostname**: `pi4dev`
- **Username**: pick one (e.g. `pi` or your own name)
- **Password**: pick a strong one — this is your primary SSH login method
- **WiFi**: enable and fill in SSID + password (skip if going Ethernet-only — but having WiFi as backup is wise)
- **Locale**: your timezone

**Services tab:**
- **Enable SSH**: ✅
- **Use public-key authentication only**: ❌ **leave unchecked**. SSH will use the password you set above. (If you'd rather use keys, check this and paste your `~/.ssh/id_ed25519.pub`.)

**SAVE** → **WRITE**. ~3–5 minutes. Eject when done.

---

## Phase 3 — First boot and SSH access

### 3.1 Power up

1. Insert the microSD card into the Pi (slot is on the bottom of the Pi PCB; accessible through a small slot in the Argon case base).
2. Plug in Ethernet (recommended for first setup; you can switch to WiFi later if you prefer).
3. Plug in USB-C power. Pi boots in ~30 seconds.

### 3.2 SSH from your laptop

```bash
ssh <username>@pi4dev.local
```

(mDNS — `pi4dev.local` — resolves on most home networks. If it fails: log into your router and find the IP; SSH to that.)

You'll be prompted for the password you set in Pi Imager. (First connection asks you to confirm the Pi's host fingerprint — type `yes`.)

### 3.3 First-time updates

```bash
sudo apt update && sudo apt full-upgrade -y
sudo reboot
```

Wait ~30 seconds, SSH back in.

---

## Phase 4 — Migrate boot from microSD to SATA SSD

The Pi 4 can boot from USB (the case's USB-SATA bridge appears as a USB device to the Pi). We need to:
1. Update the bootloader if needed
2. Set USB as preferred boot
3. Clone the OS from the SD card to the SATA SSD
4. Reboot from SSD

### 4.1 Confirm the SSD is detected

```bash
lsblk
```

Expected: `mmcblk0` (the SD card, ~30GB) AND `sda` or `sdb` (the SATA SSD, your drive's capacity).

If you don't see the SSD, check:
- Is the USB 3.0 pigtail connected to the Pi's blue USB port (not the black USB 2.0)?
- Is the M.2 SSD seated firmly in the case?

### 4.2 Update the bootloader

```bash
sudo apt install -y rpi-eeprom
sudo rpi-eeprom-update -a
sudo reboot
```

Wait ~30s, SSH back in.

### 4.3 Set USB as boot priority

```bash
sudo raspi-config
```

Navigate: **6 Advanced Options** → **A4 Boot Order** → **B2 USB Boot** → confirm.

Exit raspi-config. Decline the reboot prompt — we're not done.

### 4.4 Clone the SD card to the SATA SSD

We'll use **rpi-clone** — a Bash script that handles the dance of cloning the running OS to a target disk, updating partition UUIDs, and making the new disk bootable.

> ⚠️ **Heads-up about rpi-clone's status.** The tool's original maintainer (billw2) stopped pushing updates around 2022, and it was subsequently removed from the Raspberry Pi OS apt repositories. The script still works correctly for this one-shot SD-to-SSD migration — we just install it from its GitHub source rather than via `apt`. After the migration completes you'll never run it again. If you'd rather avoid the unmaintained-tool dependency, see the **alternative path** at the end of this step.

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

Now clone the running OS to the SSD:

```bash
sudo rpi-clone sda    # or sdb — use whatever lsblk shows for the SSD
```

When prompted to confirm formatting + cloning, type `yes` (the full word, not just `y`) and press Enter. The clone takes ~5–15 minutes.

The tool will:
- Format the SSD (confirm prompt)
- Copy the running OS, partitions, and config
- Update the new disk's UUIDs in cmdline.txt and fstab

#### Alternative path (no rpi-clone, official tools only)

If you have a **USB-M.2 SATA adapter** (~£15) and you'd rather avoid the unmaintained tool entirely:

1. Power down the Pi (`sudo poweroff`).
2. Unscrew the Argon One M.2 case, remove the SATA SSD.
3. Plug the SSD into your laptop via the USB-M.2 SATA adapter.
4. Open **Raspberry Pi Imager** on your laptop. Choose **Raspberry Pi OS Lite (64-bit)**, target the SSD, fill in the same headless config (hostname `pi4dev`, SSH enabled, password, WiFi, etc.). **WRITE.**
5. Eject the SSD, reseat it in the Argon case, screw the case back together.
6. Power the Pi — it boots directly from the SSD (no migration needed). **Skip Phases 4.5 entirely** and continue with Phase 5.

This is the cleaner long-term approach but requires the USB-SATA adapter and a disassembly step mid-bootstrap.

Takes ~5–15 minutes.

### 4.5 Reboot from SSD

```bash
sudo poweroff
```

Wait for the green LED to stop. Unplug power. Optionally remove the microSD card (keep it as a recovery card). Plug power back in.

The Pi boots from USB (the SSD) automatically. SSH back in:

```bash
ssh <username>@pi4dev.local
```

Confirm:

```bash
lsblk
df -h /
```

Root should be on the SSD, ~your-drive-capacity free.

---

## Phase 5 — Install Tailscale (for SSH remote)

This is optional for Remote Control but recommended for SSH from anywhere.

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

Visit the printed URL from your laptop. Sign in with the Tailscale account you set up in pre-flight. Approve the Pi.

```bash
tailscale ip -4
```

Note the `100.x.x.x` address. From anywhere (with Tailscale on your phone/laptop), you can now `ssh <username>@pi4dev` (MagicDNS) or `ssh <username>@100.x.x.x`.

---

## Phase 6 — Install Node.js + Claude Code

### 6.1 Install Node.js LTS

Use the official NodeSource repository for the latest stable Node.js (currently v22 LTS):

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo bash -
sudo apt install -y nodejs
```

Confirm:

```bash
node --version    # should print v22.x.x
npm --version
```

### 6.2 Install Claude Code globally

```bash
sudo npm install -g @anthropic-ai/claude-code
```

(Pi 4 build is slow — first npm install can take 2–5 minutes.)

Confirm:

```bash
claude --version
```

Should print v2.1.51 or newer. (Remote Control requires v2.1.51+.)

### 6.3 Authenticate Claude Code (subscription, headless)

```bash
claude /login
```

When prompted, choose **claude.ai** (not Console / API key).

In headless mode, `claude /login` will print a URL with a code. Open it on your laptop or phone, sign in with your Claude Pro/Max account, paste the code back. Auth completes; credentials are stored in `~/.claude/.credentials.json`.

You only do this once. Credentials survive reboots.

### 6.4 Create a working directory and accept workspace trust

```bash
mkdir -p ~/projects/scratch && cd ~/projects/scratch
claude
```

Claude Code starts. Accept the workspace trust prompt. Type `/quit` or Ctrl-D to exit. (We're just initializing the trust state for this directory.)

---

## Phase 7 — Set up Remote Control as a service

The Pi runs `claude remote-control` continuously. A systemd unit makes it survive reboots.

### 7.1 Create the systemd unit

```bash
sudo tee /etc/systemd/system/claude-remote.service > /dev/null <<EOF
[Unit]
Description=Claude Code Remote Control
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/projects/scratch
Environment="PATH=/usr/bin:/usr/local/bin"
ExecStart=/usr/bin/claude remote-control
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

(Adjust `WorkingDirectory` if you want Claude's default cwd to be a different repo. You can change cwd from inside Claude with `/cd /path`.)

### 7.2 Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable claude-remote
sudo systemctl start claude-remote
```

### 7.3 Confirm it's running and registered

```bash
sudo systemctl status claude-remote
journalctl -u claude-remote -n 30
```

Expected: "Active: active (running)". The journal should show Remote Control connecting to Anthropic and printing a session URL.

---

## Phase 8 — Connect from the Claude mobile app

1. Open the Claude mobile app, signed in with your Pro/Max account (same as on the Pi).
2. Tap **Code** in the bottom navigation.
3. Wait a few seconds — the Pi's session should appear with a **green computer icon** (auto-generated session name, something like `pi4dev-graceful-unicorn`).
4. Tap it to open. You're now talking to a Claude Code session running on the Pi.

To verify: ask it `pwd` or `ls` — it'll print the Pi's filesystem, not your phone's.

If the session doesn't appear after 30 seconds:
- `sudo systemctl status claude-remote` on the Pi — is it running?
- `journalctl -u claude-remote -n 50` — any errors?
- Confirm both ends are signed in to the **same Claude account**.

---

## Phase 9 — Optional dev tooling

Useful extras for SSH terminal work:

```bash
sudo apt install -y git tmux ripgrep fzf bat
```

- `git` — clone repos
- `tmux` — only useful for bare SSH sessions, not for Remote Control. Install it for the case where you SSH from your phone in a terminal app and want a session that survives backgrounding the app.
- `ripgrep` — fast file search (`rg "pattern"`)
- `fzf` — fuzzy finder, great for shell history (Ctrl-R)
- `bat` — `cat` with syntax highlighting

### Optional: nicer shell

```bash
sudo apt install -y zsh
chsh -s $(which zsh)
# Log out and back in for shell change to take effect
```

Or install **starship** for a fast cross-shell prompt: `curl -sS https://starship.rs/install.sh | sh`.

---

## Phase 10 — Optional VS Code Remote-SSH from your laptop

If you prefer an editor UI for some work:

1. Install **Remote-SSH** extension in VS Code on your laptop.
2. F1 → "Remote-SSH: Connect to Host" → type `<username>@pi4dev` (over Tailscale) or `<username>@pi4dev.local` (on home network).
3. VS Code installs its server on the Pi (~1 minute first time) and opens a window with the Pi's filesystem.
4. The integrated terminal lets you run `claude` directly — but for mobile access, you still use Remote Control.

---

## Operational notes

### Auto-start on reboot

Already done via systemd:

```bash
sudo reboot
# Wait ~30s, SSH back in
sudo systemctl status claude-remote
```

Should be "active (running)" again. The Claude mobile app will see the Pi reappear with the green icon within ~10 seconds of the service starting.

### Updating Claude Code

When new versions ship:

```bash
sudo npm install -g @anthropic-ai/claude-code@latest
sudo systemctl restart claude-remote
```

### Updating the system

```bash
sudo apt update && sudo apt upgrade -y
sudo systemctl restart claude-remote   # in case Node was updated
```

### Useful commands

```bash
# Watch Remote Control logs live
journalctl -u claude-remote -f

# Quickly bounce the service
sudo systemctl restart claude-remote

# Disable on boot if you want to take it offline temporarily
sudo systemctl stop claude-remote
sudo systemctl disable claude-remote

# Disk usage
df -h
```

### Disk usage and what lives where

| Path | What | Size |
|---|---|---|
| `/` (SATA SSD) | OS, Claude Code, your repos | ~500 GB free typically |
| `~/.claude/` | Claude Code credentials + session cache | few MB |
| `~/projects/` | Your code | grows over time |

---

## Troubleshooting

### Pi won't boot from SATA SSD

- Bootloader updated? `sudo rpi-eeprom-update` to check.
- Boot order set to USB? `sudo raspi-config` → Advanced → Boot Order.
- SSD detected? Power off, reseat M.2 and USB pigtail.

### `claude /login` fails

- Are you on the latest version? `sudo npm install -g @anthropic-ai/claude-code@latest`
- Is `~/.claude/` writable by your user? `ls -ld ~/.claude/`
- Behind a corporate proxy that blocks Anthropic? Check `curl https://api.anthropic.com/v1/` reaches it.

### Remote Control doesn't appear in mobile app

- Same Claude account on both ends? Confirm Pi: `claude /status` (or check ~/.claude/.credentials.json owner).
- Service running? `sudo systemctl status claude-remote`
- Pi has internet? `curl -I https://api.anthropic.com` should print 200/401 (either is fine — both prove reachability).
- Mobile app version current? Update from App Store / Play Store.

### Pi runs hot

The Argon One M.2's fan is sized for Pi 4 thermals — should be fine. If `vcgencmd measure_temp` reads >75°C steady-state:

- Confirm the fan is spinning (visual check; mute room and listen).
- Argon Forty's official fan-control script: `curl https://download.argon40.com/argon1.sh | bash` — gives PWM control + temp curves.

### Under-voltage warnings

Pi 4 needs 5V/3A. Generic phone chargers don't always deliver this under load. Use the official Pi 4 PSU or a known-good USB-C PD adapter.

```bash
vcgencmd get_throttled
```

Non-zero result = something has throttled. Common: under-voltage (bit 0x1) or temperature (bit 0x4).

---

## What's next

The Pi 4 is now a dev box for code work via Claude on your phone. From here:

- Clone repos you actively work on into `~/projects/`
- Optionally set the systemd unit's `WorkingDirectory=` to the project you use most often (or use `/cd` inside Claude)
- The Pi runs ~24/7 (~3–5W idle, ~7W active — about £6/year on UK electricity)
- Pair this with the Pi 5 (running headline-alerter in production) for a clean "dev/prod separation"

## References

- Remote Control docs: <https://code.claude.com/docs/en/remote-control>
- Claude Code quickstart: <https://code.claude.com/docs/en/quickstart>
- Claude Code authentication: <https://code.claude.com/docs/en/authentication>
- Argon One M.2 manual: <https://argon40.com/products/argon-one-m-2-case-for-raspberry-pi-4>
