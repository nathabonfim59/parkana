#!/bin/sh
# deb postinst / rpm %post. Own the data directory and config, reload
# systemd. A fresh install never enables or starts the service: edit
# /etc/parkana/parkana.env first. A deb upgrade ("configure" with the old
# version in $2) restarts the service if it is running and keeps its enabled
# state; an rpm upgrade restarts in %postun and %posttrans instead.
#
# PARKANA_PKG_SYSTEMD_DIR is for packaging/test only.
set -e
systemd_dir=${PARKANA_PKG_SYSTEMD_DIR:-/run/systemd/system}
mkdir -p /var/lib/parkana
chown parkana:parkana /var/lib/parkana
chmod 0750 /var/lib/parkana
if [ -f /etc/parkana/parkana.env ]; then
	chown root:parkana /etc/parkana/parkana.env
	chmod 0640 /etc/parkana/parkana.env
fi
if command -v systemctl >/dev/null 2>&1 && [ -d "$systemd_dir" ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
	if [ "${1:-}" = configure ] && [ -n "${2:-}" ]; then
		systemctl try-restart parkana.service >/dev/null 2>&1 || true
		# The 1.0.0 prerm stopped and disabled the service before this
		# package was unpacked, so its state is gone.
		if dpkg --compare-versions "$2" le 1.0.0-1 2>/dev/null; then
			echo "parkana: the $2 package stopped and disabled parkana.service on upgrade." >&2
			echo "parkana: if it was running, start it again with: systemctl enable --now parkana" >&2
		fi
	fi
fi
exit 0
