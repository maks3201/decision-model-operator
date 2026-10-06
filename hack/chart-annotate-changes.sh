#!/usr/bin/env bash
# Append an `artifacthub.io/changes` annotation to a chart's Chart.yaml, built
# from the given release's CHANGELOG section via hack/chart-changes.sh.
#
# Usage:   hack/chart-annotate-changes.sh <version> <chart-yaml-path> [changelog-path]
# Example: hack/chart-annotate-changes.sh 0.3.0 charts/decision-model-operator/Chart.yaml
#
# Intended for the release packaging step: it mutates Chart.yaml in place right
# before `helm package`, so run it on a throwaway checkout (the release job) and
# do NOT commit the result — Chart.yaml in git stays free of per-release changes.
# A no-op (prints a notice, leaves Chart.yaml untouched) when the version has no
# CHANGELOG entries, so a release with an empty section still packages cleanly.
set -euo pipefail

version="${1:-}"
chart_yaml="${2:-}"
changelog="${3:-CHANGELOG.md}"

if [[ -z "$version" || -z "$chart_yaml" ]]; then
	echo "usage: $0 <version> <chart-yaml-path> [changelog-path]" >&2
	exit 2
fi
if [[ ! -f "$chart_yaml" ]]; then
	echo "Chart.yaml not found: $chart_yaml" >&2
	exit 1
fi

here="$(cd "$(dirname "$0")" && pwd)"
changes="$("$here/chart-changes.sh" "$version" "$changelog")"

if [[ -z "$changes" ]]; then
	echo "no CHANGELOG entries for $version; leaving Chart.yaml unchanged" >&2
	exit 0
fi

if grep -q 'artifacthub.io/changes:' "$chart_yaml"; then
	echo "artifacthub.io/changes already present in $chart_yaml; leaving it unchanged" >&2
	exit 0
fi

# Append under the existing top-level `annotations:` map. The annotation value is
# a literal YAML block (same style as artifacthub.io/images): key indented 2
# spaces, block content indented 4.
{
	printf '  artifacthub.io/changes: |\n'
	# Indent each line of the generated YAML by 4 spaces.
	awk '{ print "    " $0 }' <<<"$changes"
} >>"$chart_yaml"

echo "added artifacthub.io/changes for $version to $chart_yaml" >&2
