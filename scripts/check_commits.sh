#!/usr/bin/env bash
# usage: scripts/check_commits.sh [base-ref] (default: the branch point from main)
base="${1:-$(git merge-base HEAD main)}"
git log --format='%s' "$base"..HEAD | awk 'NF>9 {print "TOO LONG (" NF " words): " $0; bad=1} END {exit bad}'
