#!/bin/sh
# deb preinst / rpm %pre. Create the parkana system user and group if they
# do not exist. On an rpm upgrade ($1 >= 2) record whether the service is
# enabled and running: a 1.0.0 package's %preun, which runs after this,
# stops and disables it, and %posttrans (posttrans.sh) restores the record.
#
# PARKANA_PKG_SYSTEMD_DIR and PARKANA_PKG_STATE_DIR are for packaging/test only.
set -e
systemd_dir=${PARKANA_PKG_SYSTEMD_DIR:-/run/systemd/system}
state_dir=${PARKANA_PKG_STATE_DIR:-/var/lib/rpm-state/parkana}
if ! getent group parkana >/dev/null 2>&1; then
	if command -v addgroup >/dev/null 2>&1 && ! command -v groupadd >/dev/null 2>&1; then
		addgroup -S parkana
	else
		groupadd --system parkana
	fi
fi
if ! getent passwd parkana >/dev/null 2>&1; then
	if command -v useradd >/dev/null 2>&1; then
		useradd --system --gid parkana --home-dir /var/lib/parkana --no-create-home \
			--shell /usr/sbin/nologin --comment "Parkana log store" parkana
	else
		adduser -S -D -H -G parkana -h /var/lib/parkana -s /sbin/nologin -g "Parkana log store" parkana
	fi
fi
case "${1:-}" in
[2-9] | [1-9][0-9])
	rm -rf "$state_dir"
	if command -v systemctl >/dev/null 2>&1 && [ -d "$systemd_dir" ]; then
		mkdir -p "$state_dir"
		if [ "$(systemctl is-enabled parkana.service 2>/dev/null || true)" = enabled ]; then
			: >"$state_dir/enabled"
		fi
		if systemctl is-active --quiet parkana.service 2>/dev/null; then
			: >"$state_dir/active"
		fi
	fi
	;;
esac
exit 0
