#!/bin/bash
set -euo pipefail
out=/tmp/fireweed-trim-recovery
mkdir -p "$out"
chmod 755 "$out"
cryptsetup status root > "$out/crypt-before.txt"
lsblk -D > "$out/discard-before.txt"
cat /sys/block/nvme0n1/stat > "$out/device-before.txt"
cryptsetup refresh --allow-discards --persistent root
cryptsetup status root | tee "$out/crypt-after.txt"
lsblk -D | tee "$out/discard-after.txt"
fstrim -v /home | tee "$out/fstrim.txt"
cat /sys/block/nvme0n1/stat > "$out/device-after.txt"
systemctl enable --now fstrim.timer
systemctl status fstrim.timer --no-pager > "$out/timer.txt"
date -Is > "$out/completed.txt"
chmod 644 "$out"/*
printf 'TRIM recovery completed. Results: %s\n' "$out"
