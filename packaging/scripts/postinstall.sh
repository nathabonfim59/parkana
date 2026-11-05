#!/bin/sh
# Own the data directory and config, reload systemd. The service is not
# started automatically: edit /etc/parkana/parkana.env first.
set -e
mkdir -p /var/lib/parkana
chown parkana:parkana /var/lib/parkana
chmod 0750 /var/lib/parkana
if [ -f /etc/parkana/parkana.env ]; then
	chown root:parkana /etc/parkana/parkana.env
	chmod 0640 /etc/parkana/parkana.env
fi
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
fi
exit 0
