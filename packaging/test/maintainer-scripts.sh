#!/bin/sh
# Runs the package maintainer scripts in packaging/scripts in the order dpkg
# and rpm run them for every install, upgrade and remove scenario, against a
# stub systemctl that records its calls and keeps the unit's enabled and
# active state in files, and checks the state each scenario leaves behind.
# packaging/test/v1.0.0 holds the scripts the 1.0.0 packages shipped, the
# "old package" side of an upgrade from 1.0.0.
#
# Test only: PARKANA_PKG_SYSTEMD_DIR replaces /run/systemd/system (the "is
# systemd running" check) and PARKANA_PKG_STATE_DIR replaces the rpm upgrade
# record directory. Nothing outside a temporary directory is touched: mkdir,
# chown and chmod of the real package paths, getent and the user tools are
# stubbed too.
#
# Usage: sh packaging/test/maintainer-scripts.sh   (or make packaging-test)
set -u

here=$(cd "$(dirname "$0")" && pwd)
scripts=${SCRIPTS:-$here/../scripts}
old100=$here/v1.0.0

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

real_mkdir=$(command -v mkdir)
real_chown=$(command -v chown)
real_chmod=$(command -v chmod)
mkdir "$work/bin" "$work/old100"

# Stubs. Each records "name args" in $work/calls.
cat >"$work/bin/systemctl" <<'EOF'
#!/bin/sh
# Stub systemctl: one unit, parkana.service; state in $STUB_STATE.
echo "systemctl $*" >>"$STUB_CALLS"
cmd= unit= now= quiet=
for a in "$@"; do
	case $a in
	--now) now=1 ;;
	-q | --quiet) quiet=1 ;;
	--no-reload | --system) ;;
	-*) ;;
	*) if [ -z "$cmd" ]; then cmd=$a; else unit=$a; fi ;;
	esac
done
case $unit in "" | parkana | parkana.service) ;; *) echo "stub systemctl: unknown unit $unit" >&2; exit 5 ;; esac
active() { [ -f "$STUB_STATE/active" ]; }
start() {
	if ! active; then
		: >"$STUB_STATE/active"
		# Every (re)start is a new main PID.
		echo $(($(cat "$STUB_STATE/pid") + 1)) >"$STUB_STATE/pid"
	fi
}
case $cmd in
daemon-reload) ;;
enable) : >"$STUB_STATE/enabled"; [ -n "$now" ] && start ;;
disable) rm -f "$STUB_STATE/enabled"; [ -n "$now" ] && rm -f "$STUB_STATE/active" ;;
start) start ;;
stop) rm -f "$STUB_STATE/active" ;;
restart) rm -f "$STUB_STATE/active"; start ;;
try-restart | condrestart) if active; then rm -f "$STUB_STATE/active"; start; fi ;;
is-enabled)
	if [ -f "$STUB_STATE/enabled" ]; then s=enabled r=0; else s=disabled r=1; fi
	[ -n "$quiet" ] || echo $s
	exit $r
	;;
is-active)
	if active; then s=active r=0; else s=inactive r=3; fi
	[ -n "$quiet" ] || echo $s
	exit $r
	;;
*) echo "stub systemctl: unsupported command $cmd" >&2; exit 1 ;;
esac
exit 0
EOF

cat >"$work/bin/dpkg" <<'EOF'
#!/bin/sh
# Stub dpkg: only --compare-versions with lt, le, eq, ge, gt (sort -V order).
echo "dpkg $*" >>"$STUB_CALLS"
[ "$1" = --compare-versions ] || { echo "stub dpkg: unsupported $*" >&2; exit 2; }
a=$2 op=$3 b=$4
if [ "$a" = "$b" ]; then c=0
elif [ "$(printf '%s\n%s\n' "$a" "$b" | sort -V | head -n1)" = "$a" ]; then c=-1
else c=1; fi
case $op in
lt | lt-nl) [ "$c" -lt 0 ] ;;
le | le-nl) [ "$c" -le 0 ] ;;
eq) [ "$c" -eq 0 ] ;;
ge | ge-nl) [ "$c" -ge 0 ] ;;
gt | gt-nl) [ "$c" -gt 0 ] ;;
*) exit 2 ;;
esac
EOF

for tool in getent groupadd useradd addgroup adduser; do
	# shellcheck disable=SC2016 # $* and $STUB_CALLS expand in the stub
	printf '#!/bin/sh\necho "%s $*" >>"$STUB_CALLS"\nexit 0\n' "$tool" >"$work/bin/$tool"
done

# mkdir, chown, chmod: the real tool inside $work, recorded no-ops elsewhere.
for tool in mkdir chown chmod; do
	case $tool in
	mkdir) real=$real_mkdir ;;
	chown) real=$real_chown ;;
	chmod) real=$real_chmod ;;
	esac
	cat >"$work/bin/$tool" <<EOF
#!/bin/sh
for a in "\$@"; do
	case \$a in
	"$work"/*) exec "$real" "\$@" ;;
	esac
done
echo "$tool \$*" >>"\$STUB_CALLS"
exit 0
EOF
done
"$real_chmod" +x "$work"/bin/*

# The 1.0.0 scripts test the real /run/systemd/system: point them at the stub.
for f in "$old100"/*.sh; do
	sed "s#/run/systemd/system#$work/run-systemd#g" "$f" >"$work/old100/$(basename "$f")"
done

PATH=$work/bin:$PATH
STUB_CALLS=$work/calls
STUB_STATE=$work/state
PARKANA_PKG_SYSTEMD_DIR=$work/run-systemd
PARKANA_PKG_STATE_DIR=$work/rpm-state/parkana
export PATH STUB_CALLS STUB_STATE PARKANA_PKG_SYSTEMD_DIR PARKANA_PKG_STATE_DIR

failures=0
name=

# reset <enabled|disabled> <active|inactive> [nosystemd]: a fresh scenario.
reset() {
	rm -rf "$STUB_STATE" "$work/rpm-state" "$work/run-systemd"
	: >"$STUB_CALLS"
	"$real_mkdir" -p "$STUB_STATE"
	echo 100 >"$STUB_STATE/pid"
	[ "$1" = enabled ] && : >"$STUB_STATE/enabled"
	[ "$2" = active ] && : >"$STUB_STATE/active"
	[ "${3:-}" = nosystemd ] || "$real_mkdir" -p "$work/run-systemd"
	: >"$work/out"
	: >"$work/err"
}

# run <dir> <script> args...: runs one maintainer script as dpkg/rpm do
# (dpkg: executable, its own "set -e"; rpm: /bin/sh). A script the package
# does not ship is skipped. stdout goes to $work/out, stderr to $work/err.
run() {
	dir=$1 script=$2
	shift 2
	[ -f "$dir/$script" ] || return 0
	echo "# $script $*" >>"$STUB_CALLS"
	if ! sh "$dir/$script" "$@" >>"$work/out" 2>>"$work/err"; then
		fail "$script $* exited non-zero"
	fi
}

fail() {
	echo "FAIL $name: $*"
	failures=$((failures + 1))
	bad=1
}

state() {
	e=disabled a=inactive
	[ -f "$STUB_STATE/enabled" ] && e=enabled
	[ -f "$STUB_STATE/active" ] && a=active
	echo "$e $a"
}

# expect <enabled|disabled> <active|inactive> <restarted|same>: restarted
# means started exactly once during the scenario (the stub's PID counter).
expect() {
	got=$(state)
	[ "$got" = "$1 $2" ] || fail "want $1 $2, got $got"
	pid=$(cat "$STUB_STATE/pid")
	case $3 in
	restarted) [ "$pid" -eq 101 ] || fail "want the service (re)started exactly once, got $((pid - 100))" ;;
	same) [ "$pid" -eq 100 ] || fail "want no (re)start, got $((pid - 100))" ;;
	esac
	if [ -e "$PARKANA_PKG_STATE_DIR" ]; then fail "rpm upgrade record $PARKANA_PKG_STATE_DIR left behind"; fi
}

expect_notice() {
	grep -q 'systemctl enable --now parkana' "$work/err" || fail "want the re-enable notice on stderr, got none"
	if grep -q 'systemctl enable --now parkana' "$work/out"; then fail "re-enable notice on stdout"; fi
}

expect_no_notice() {
	if grep -q 'systemctl enable --now parkana' "$work/out" "$work/err"; then fail "unexpected re-enable notice"; fi
}

no_systemctl() {
	if grep -q '^systemctl' "$STUB_CALLS"; then fail "systemctl called without systemd: $(grep '^systemctl' "$STUB_CALLS" | tr '\n' ';')"; fi
}

begin() {
	name=$1
	bad=0
}

end() {
	if [ "$bad" -eq 0 ]; then
		echo "PASS $name ($(state))"
	else
		echo "  calls:"
		sed 's/^/    /' "$STUB_CALLS"
		if [ -s "$work/out" ] || [ -s "$work/err" ]; then
			echo "  output:"
			sed 's/^/    /' "$work/out" "$work/err"
		fi
	fi
}

new=$scripts
old=$scripts # an upgrade between two packages with the current scripts

# dpkg: install = preinst install, postinst configure "".
# upgrade = old prerm upgrade NEW, new preinst upgrade OLD, unpack,
# old postrm upgrade NEW, new postinst configure OLD.
# remove = prerm remove, postrm remove; purge adds postrm purge.
deb_upgrade() { # <old dir> <old version>
	run "$1" preremove.sh upgrade 1.2.1-1
	run "$new" preinstall.sh upgrade "$2"
	run "$1" postremove.sh upgrade 1.2.1-1
	run "$new" postinstall.sh configure "$2"
}

begin "deb fresh install"
reset disabled inactive
run "$new" preinstall.sh install
run "$new" postinstall.sh configure ""
expect disabled inactive same
expect_no_notice
end

for st in "enabled active restarted" "disabled inactive same" "enabled inactive same"; do
	# shellcheck disable=SC2086 # split "enabled active restarted" into $1..$3
	set -- $st
	begin "deb upgrade $1+$2"
	reset "$1" "$2"
	deb_upgrade "$old" 1.2.0-1
	expect "$1" "$2" "$3"
	expect_no_notice
	end
done

begin "deb remove"
reset enabled active
run "$new" preremove.sh remove
run "$new" postremove.sh remove
expect disabled inactive same
end

begin "deb purge"
reset enabled active
run "$new" preremove.sh remove
run "$new" postremove.sh remove
run "$new" postremove.sh purge
expect disabled inactive same
end

# From 1.0.0 the old prerm stops and disables before any new script runs:
# the state is lost, the new postinst prints how to bring it back.
begin "deb upgrade from 1.0.0 (enabled+active)"
reset enabled active
deb_upgrade "$work/old100" 1.0.0-1
expect disabled inactive same
expect_notice
end

# dpkg error unwinding: a failed old prerm or new preinst makes dpkg call
# "failed-upgrade" and "abort-upgrade"; none of them may start anything.
begin "deb failed-upgrade and abort-upgrade (enabled+inactive)"
reset enabled inactive
run "$new" preremove.sh failed-upgrade 1.2.0-1
run "$new" postremove.sh failed-upgrade 1.2.0-1
run "$new" postremove.sh abort-upgrade 1.2.1-1
run "$new" postinstall.sh abort-upgrade 1.2.1-1
expect enabled inactive same
end

begin "deb deconfigure"
reset enabled active
run "$new" preremove.sh deconfigure in-favour other 1.0
expect disabled inactive same
end

begin "deb abort-upgrade (enabled+active)"
reset enabled active
run "$new" postremove.sh abort-upgrade 1.2.1-1
run "$new" postinstall.sh abort-upgrade 1.2.1-1
expect enabled active same
end

begin "deb upgrade without systemd"
reset disabled inactive nosystemd
deb_upgrade "$old" 1.2.0-1
no_systemctl
end

# rpm: install = %pre 1, %post 1, %posttrans.
# upgrade = new %pre 2, new %post 2, old %preun 1, old %postun 1, new %posttrans.
# erase = %preun 0, %postun 0.
rpm_upgrade() { # <old dir>
	run "$new" preinstall.sh 2
	run "$new" postinstall.sh 2
	run "$1" preremove.sh 1
	run "$1" postremove.sh 1
	run "$new" posttrans.sh 0
}

begin "rpm fresh install"
reset disabled inactive
run "$new" preinstall.sh 1
run "$new" postinstall.sh 1
run "$new" posttrans.sh 0
expect disabled inactive same
end

for st in "enabled active restarted" "disabled inactive same" "enabled inactive same"; do
	# shellcheck disable=SC2086 # split "enabled active restarted" into $1..$3
	set -- $st
	# 1.0.1 -> 1.0.2: the old %postun try-restarts, the new %posttrans
	# finds the state already right and leaves it.
	begin "rpm upgrade $1+$2"
	reset "$1" "$2"
	rpm_upgrade "$old"
	expect "$1" "$2" "$3"
	end
done

begin "rpm erase"
reset enabled active
run "$new" preremove.sh 0
run "$new" postremove.sh 0
expect disabled inactive same
end

# From 1.0.0 the old %preun (after the new %pre) stops and disables; the
# new %pre records the state and %posttrans restores it.
for st in "enabled active restarted" "disabled inactive same" "enabled inactive same"; do
	# shellcheck disable=SC2086 # split "enabled active restarted" into $1..$3
	set -- $st
	begin "rpm upgrade from 1.0.0 ($1+$2)"
	reset "$1" "$2"
	rpm_upgrade "$work/old100"
	expect "$1" "$2" "$3"
	end
done

begin "rpm posttrans without a record"
reset enabled inactive
run "$new" posttrans.sh 0
expect enabled inactive same
if grep -q '^systemctl' "$STUB_CALLS"; then fail "posttrans without a record called systemctl"; fi
end

begin "rpm upgrade without systemd"
reset disabled inactive nosystemd
rpm_upgrade "$old"
no_systemctl
end

if [ "$failures" -gt 0 ]; then
	echo "maintainer-scripts: $failures failure(s)"
	exit 1
fi
echo "maintainer-scripts: all scenarios pass"
