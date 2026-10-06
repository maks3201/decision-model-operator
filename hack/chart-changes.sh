#!/usr/bin/env bash
# Print the Artifact Hub `artifacthub.io/changes` YAML for one release, derived
# from that release's section in CHANGELOG.md (release-please format).
#
# Usage:   hack/chart-changes.sh <version> [changelog-path]
# Example: hack/chart-changes.sh 0.3.0
#
# Output is the YAML list for the annotation value, e.g.
#   - kind: added
#     description: "download model weights from a Hugging Face mirror"
#     links:
#       - name: GitHub commit
#         url: https://github.com/.../commit/003292f
#
# The release packaging step injects this into a COPY of Chart.yaml before
# `helm package`, so Chart.yaml in git stays clean.
#
# Mapping release-please headings -> Artifact Hub change kinds:
#   Features                 -> added
#   Bug Fixes                -> fixed
#   Performance Improvements -> changed
#   Reverts                  -> changed
#   Documentation            -> changed
#   Security                 -> security
# Any other heading is treated as "changed".
set -euo pipefail

version="${1:-}"
changelog="${2:-CHANGELOG.md}"

if [[ -z "$version" ]]; then
	echo "usage: $0 <version> [changelog-path]" >&2
	exit 2
fi
if [[ ! -f "$changelog" ]]; then
	echo "changelog not found: $changelog" >&2
	exit 1
fi

# Strip a leading "v" so both "v0.3.0" and "0.3.0" work.
version="${version#v}"

awk -v want="$version" '
	function flush_kind() { kind = "" }

	# Section heading: "## [0.3.0](url) (date)" or "## 0.3.0 (date)".
	/^## / {
		line = $0
		sub(/^## /, "", line)
		# Pull the version token: either "[x.y.z]" or a bare "x.y.z".
		if (match(line, /^\[[^]]+\]/)) {
			ver = substr(line, 2, RLENGTH - 2)
		} else {
			split(line, a, " ")
			ver = a[1]
		}
		sub(/^v/, "", ver)
		in_section = (ver == want)
		flush_kind()
		next
	}

	# Only parse inside the wanted section.
	!in_section { next }

	# Subsection heading: "### Features", "### Bug Fixes", ...
	/^### / {
		h = $0
		sub(/^### /, "", h)
		if (h == "Features")                      kind = "added"
		else if (h == "Bug Fixes")                kind = "fixed"
		else if (h == "Performance Improvements") kind = "changed"
		else if (h == "Reverts")                  kind = "changed"
		else if (h == "Documentation")            kind = "changed"
		else if (h == "Security")                 kind = "security"
		else                                      kind = "changed"
		next
	}

	# Change bullet: "* **scope:** description ([abc1234](url))" or
	# "description ([#6](issue-url)) ([abc1234](commit-url))".
	/^\* / {
		if (kind == "") next
		entry = $0
		sub(/^\* /, "", entry)

		# Collect every markdown link in the bullet; prefer a PR/issue link for
		# the annotation, else fall back to the commit link.
		pr_url = ""
		commit_url = ""
		rest = entry
		while (match(rest, /\]\(https:\/\/[^) ]+\)/)) {
			u = substr(rest, RSTART + 2, RLENGTH - 3)
			if (u ~ /\/pull\/|\/issues\//) pr_url = u
			else                           commit_url = u
			rest = substr(rest, RSTART + RLENGTH)
		}
		url = (pr_url != "") ? pr_url : commit_url

		# Description = the bullet with ALL trailing "([..](..))" link groups removed.
		desc = entry
		while (sub(/[ ]*\(\[[^]]*\]\(https:\/\/[^)]*\)\)[ ]*$/, "", desc)) { }
		# Drop the bold markdown around a conventional-commit scope: "**scope:** ".
		gsub(/\*\*/, "", desc)
		# Trim surrounding whitespace.
		gsub(/^[ \t]+|[ \t]+$/, "", desc)
		# Escape double quotes for the YAML double-quoted scalar.
		gsub(/"/, "\\\"", desc)

		printf "- kind: %s\n", kind
		printf "  description: \"%s\"\n", desc
		if (url != "") {
			name = (url ~ /\/pull\/|\/issues\//) ? "GitHub PR" : "GitHub commit"
			printf "  links:\n"
			printf "    - name: %s\n", name
			printf "      url: %s\n", url
		}
	}
' "$changelog"
