#!/bin/sh
# deb postrm / rpm %postun. After an rpm upgrade ($1 >= 1, run from the old
# package once the new files are in place) reload systemd and restart the
# service if it is running; it keeps its enabled state. After a removal
# (deb "remove" or "purge", rpm $1 = 0) only reload. A deb upgrade restarts
# in the new package's postinst instead.
#
# PARKANA_PKG_SYSTEMD_DIR is for packaging/test only.
systemd_dir=${PARKANA_PKG_SYSTEMD_DIR:-/run/systemd/system}
if ! command -v systemctl >/dev/null 2>&1 || [ ! -d "$systemd_dir" ]; then
	exit 0
fi
case "${1:-}" in
remove | purge | 0)
	systemctl daemon-reload >/dev/null 2>&1 || true
	;;
[1-9] | [1-9][0-9])
	systemctl daemon-reload >/dev/null 2>&1 || true
	systemctl try-restart parkana.service >/dev/null 2>&1 || true
	;;
esac
exit 0
