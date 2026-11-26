#!/bin/sh
# Stop the service before removal. Data in /var/lib/parkana is kept.
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
	systemctl stop parkana.service >/dev/null 2>&1 || true
	systemctl disable parkana.service >/dev/null 2>&1 || true
fi
exit 0
