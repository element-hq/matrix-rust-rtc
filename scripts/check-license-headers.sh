#!/usr/bin/env bash
# Copyright 2026 Element Creations Ltd.
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
# Please see LICENSE in the repository root for full details.

# Verifies that every source file carries the dual-license SPDX header, and
# with --fix rewrites the ones that do not.
#
# `--fix` is what performed the original AGPL-only -> dual-license sweep. It
# replaces a leading comment block only when that block mentions the licence,
# so a file whose first lines are an ordinary doc comment is left alone.
set -euo pipefail

cd "$(dirname "$0")/.."

COPYRIGHT="Copyright 2026 Element Creations Ltd."
SPDX="SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial"
POINTER="Please see LICENSE in the repository root for full details."

# Upstream Gradle (Apache-2.0; `gradlew` carries its own SPDX tag, and the
# wrapper properties are rewritten by `gradle wrapper` on every upgrade) and the
# uniffi-generated Swift, which every codegen run overwrites.
is_exempt() {
    case "$1" in
    mobile/android/gradlew | \
        mobile/android/gradle/wrapper/gradle-wrapper.jar | \
        mobile/android/gradle/wrapper/gradle-wrapper.properties | \
        Sources/MatrixRtc/MatrixRtc.swift) return 0 ;;
    *) return 1 ;;
    esac
}

# Files that need a header, and how the header is spelled in each.
#   hash  - # comments
#   slash - // comments
#   block - /* */ comments
#   html  - <!-- --> comments
comment_style() {
    case "$1" in
    *.rs | *.kt | *.swift | *.gradle) echo slash ;;
    *.ts | *.mjs | *.js | *.css) echo block ;;
    *.toml | *.yml | *.yaml | *.sh | *.conf | *.properties) echo hash ;;
    *.html) echo html ;;
    Makefile | */Makefile) echo hash ;;
    *) echo "" ;;
    esac
}

# Each form ends with a blank line, so the header is always separated from the
# body; the normaliser in fix_file collapses it if the body supplies one too.
render_header() {
    case "$1" in
    slash) printf '// %s\n//\n// %s\n// %s\n\n' "$COPYRIGHT" "$SPDX" "$POINTER" ;;
    hash) printf '# %s\n#\n# %s\n# %s\n\n' "$COPYRIGHT" "$SPDX" "$POINTER" ;;
    block) printf '/*\n%s\n\n%s\n%s\n*/\n\n' "$COPYRIGHT" "$SPDX" "$POINTER" ;;
    html) printf '<!--\n%s\n\n%s\n%s\n-->\n\n' "$COPYRIGHT" "$SPDX" "$POINTER" ;;
    esac
}

# A first line that must stay first: shebang, Swift's tools version, or an HTML
# doctype. Putting the header above any of these breaks the file.
is_prologue() {
    case "$1" in
    '#!'* | '// swift-tools-version'*) return 0 ;;
    '<!doctype'* | '<!DOCTYPE'*) return 0 ;;
    *) return 1 ;;
    esac
}

# Removes the old licence notice from the head of a file, line by line rather
# than by discarding the whole leading comment.
#
# Whole-block stripping loses real content: `demo/backend/nginx/interop.conf`
# follows its notice with a descriptive comment in the same run of `#` lines,
# and `web/demo/index.html` keeps the page description inside the very same
# `<!-- -->` block as the notice. Matching individual boilerplate lines keeps
# both. Only the leading comment region is considered, so prose further down
# that happens to mention a licence is never touched.
strip_existing() {
    awk '
        function is_boilerplate(s) {
            gsub(/^[[:space:]]*(#|\/\/|\/\*|<!--|\*)[[:space:]]?/, "", s)
            gsub(/(\*\/|-->)[[:space:]]*$/, "", s)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", s)
            if (s == "") return 1
            return s ~ /^Copyright [0-9]/ ||
                   s ~ /This file is part of matrix-rust-rtc/ ||
                   s ~ /matrix-rust-rtc is free software/ ||
                   s ~ /matrix-rust-rtc is distributed in the hope/ ||
                   s ~ /under the terms of the GNU Affero/ ||
                   s ~ /it under the terms of the GNU/ ||
                   s ~ /the Free Software Foundation/ ||
                   s ~ /Free Software Foundation, either version/ ||
                   s ~ /\(at your option\) any later version/ ||
                   s ~ /any later version\./ ||
                   s ~ /but WITHOUT ANY WARRANTY/ ||
                   s ~ /MERCHANTABILITY or FITNESS/ ||
                   s ~ /GNU Affero General Public License/ ||
                   s ~ /You should have received a copy/ ||
                   s ~ /along with matrix-rust-rtc/ ||
                   s ~ /gnu\.org\/licenses/ ||
                   s ~ /^SPDX-License-Identifier:/ ||
                   s ~ /^Please see LICENSE/ ||
                   s ~ /released under the GNU Affero/ ||
                   # Continuation of a notice wrapped across lines, e.g.
                   # "...released under the GNU Affero" /
                   # "General Public License v3.0 or later; see the repository root."
                   s ~ /^General Public License/ ||
                   s ~ /see the repository root/
        }
        BEGIN { in_head = 1 }
        in_head && NR == 1 && (/^#!/ || /^\/\/ swift-tools-version/ || /^<!([Dd][Oo][Cc][Tt][Yy][Pp][Ee])/) {
            print; next
        }
        # The leading comment region ends at the first line that is neither a
        # comment, a blank, nor part of an open block comment.
        in_head {
            if (/^[[:space:]]*(\/\*|<!--)/) in_block = 1
            if (in_block) {
                if (is_boilerplate($0) && $0 !~ /^[[:space:]]*(\/\*|<!--)/ && $0 !~ /(\*\/|-->)[[:space:]]*$/) next
                if (/(\*\/|-->)[[:space:]]*$/) in_block = 0
                print; next
            }
            if (/^[[:space:]]*(#|\/\/)/) {
                if (is_boilerplate($0)) next
                print; next
            }
            if ($0 ~ /^[[:space:]]*$/) { print; next }
            in_head = 0
        }
        { print }
    ' "$1"
}

fix_file() {
    local file="$1" style="$2" tmp first
    tmp="$(mktemp)"
    first="$(head -n 1 "$file")"

    if is_prologue "$first"; then
        {
            printf '%s\n' "$first"
            render_header "$style"
            strip_existing "$file" | tail -n +2
        } >"$tmp"
    else
        {
            render_header "$style"
            strip_existing "$file"
        } >"$tmp"
    fi

    # A block comment whose entire contents were the old notice is left as an
    # empty `/* */` shell; drop it. Then collapse runs of blank lines at the
    # seam and write back through the original inode, so the file keeps its
    # mode (the shell scripts here are executable and `mv` would drop that).
    awk '
        /^[[:space:]]*(\/\*|<!--)[[:space:]]*$/ { pending = $0; blanks = ""; next }
        pending != "" && /^[[:space:]]*$/ { blanks = blanks $0 "\n"; next }
        pending != "" && /^[[:space:]]*(\*\/|-->)[[:space:]]*$/ { pending = ""; blanks = ""; next }
        pending != "" { printf "%s\n%s", pending, blanks; pending = ""; blanks = "" }
        { print }
        END { if (pending != "") printf "%s\n%s", pending, blanks }
    ' "$tmp" >"$tmp.deshell"
    awk 'NR==1 {print; prev_blank=0; next}
         { if ($0 ~ /^[[:space:]]*$/) { if (prev_blank) next; prev_blank=1 } else prev_blank=0; print }' \
        "$tmp.deshell" >"$tmp.norm"
    rm -f "$tmp.deshell"
    cat "$tmp.norm" >"$file"
    rm -f "$tmp" "$tmp.norm"
}

FIX=0
[ "${1:-}" = "--fix" ] && FIX=1

missing=0
fixed=0
while IFS= read -r file; do
    is_exempt "$file" && continue
    style="$(comment_style "$file")"
    [ -z "$style" ] && continue
    [ -f "$file" ] || continue

    if head -n 10 "$file" | grep -qF "$SPDX"; then
        continue
    fi

    if [ "$FIX" = 1 ]; then
        fix_file "$file" "$style"
        fixed=$((fixed + 1))
    else
        echo "missing license header: $file"
        missing=$((missing + 1))
    fi
done < <(git ls-files)

if [ "$FIX" = 1 ]; then
    echo "rewrote $fixed file(s)"
    exit 0
fi

if [ "$missing" -gt 0 ]; then
    echo
    echo "$missing file(s) missing the dual-license header. Run: $0 --fix"
    exit 1
fi

echo "all source files carry the dual-license header"
