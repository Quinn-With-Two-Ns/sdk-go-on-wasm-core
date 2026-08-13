#!/bin/sh
set -eu

module="$(go list -m)"
unexpected="$(go list ./... | awk -v module="$module" '
	$0 == module "/activity" ||
	$0 == module "/workflow" ||
	$0 == module "/worker" ||
	$0 == module "/cmd" ||
	index($0, module "/cmd/") == 1 ||
	index($0, module "/internal/") == 1 { next }
	{ print }
')"

if [ -n "$unexpected" ]; then
	printf 'unexpected public package(s):\n%s\n' "$unexpected" >&2
	exit 1
fi

for public_package in activity workflow worker; do
	go list "$module/$public_package" >/dev/null
done
