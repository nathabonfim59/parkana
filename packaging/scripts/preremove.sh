#!/bin/sh
# deb prerm / rpm %preun. Stop and disable the service when the package is
# removed (deb "remove" or "deconfigure", rpm $1 = 0). On an upgrade
# (deb "upgrade", rpm $1 >= 1) leave the service alone: the new package
# restarts it if it was running. Data in /var/lib/parkana is kept.
#
# PARKANA_PKG_SYSTEMD_DIR is for packaging/test only.
systemd_dir=${PARKANA_PKG_SYSTEMD_DIR:-/run/systemd/system}
case "${1:-}" in
remove | deconfigure | 0)
	if command -v systemctl >/dev/null 2>&1 && [ -d "$systemd_dir" ]; then
		systemctl stop parkana.service >/dev/null 2>&1 || true
		systemctl disable parkana.service >/dev/null 2>&1 || true
	fi
	;;
esac
exit 0
