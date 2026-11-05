#!/bin/sh
# Create the parkana system user and group if they do not exist.
set -e
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
exit 0
