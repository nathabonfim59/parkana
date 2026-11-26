#!/bin/sh
# rpm %posttrans, the last step of an install or upgrade. Restores the state
# preinstall.sh recorded before an upgrade: enable the service if it was
# enabled, start it if it was running, unless it already is (an upgrade from
# 1.0.0, whose %preun stopped and disabled it). Without a record (a fresh
# install) it does nothing.
#
# PARKANA_PKG_SYSTEMD_DIR and PARKANA_PKG_STATE_DIR are for packaging/test only.
systemd_dir=${PARKANA_PKG_SYSTEMD_DIR:-/run/systemd/system}
state_dir=${PARKANA_PKG_STATE_DIR:-/var/lib/rpm-state/parkana}
[ -d "$state_dir" ] || exit 0
if command -v systemctl >/dev/null 2>&1 && [ -d "$systemd_dir" ]; then
	if [ -f "$state_dir/enabled" ] &&
		[ "$(systemctl is-enabled parkana.service 2>/dev/null || true)" != enabled ]; then
		systemctl enable parkana.service >/dev/null 2>&1 || true
	fi
	if [ -f "$state_dir/active" ] && ! systemctl is-active --quiet parkana.service 2>/dev/null; then
		systemctl start parkana.service >/dev/null 2>&1 || true
	fi
fi
rm -rf "$state_dir"
exit 0
