#!/usr/bin/env bash
set -euo pipefail

report_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_dir="$report_dir/sources"
generated_dir="$report_dir/generated"
mermaid_dir="$generated_dir/mermaid"
mkdir -p "$generated_dir"
mkdir -p "$mermaid_dir"
find "$mermaid_dir" -maxdepth 1 -type f -delete

for source in "$source_dir"/*.md; do
    name=$(basename "$source" .md)
    html="$generated_dir/$name.html"
    prepared="$generated_dir/$name.rendered.md"
    title=$(sed -n '1s/^# //p' "$source")

    python3 "$report_dir/prepare_mermaid.py" \
        "$source" "$prepared" "$mermaid_dir" "$name"

    {
        printf '%s\n' '<!doctype html>' '<html lang="en">' '<head>' '<meta charset="utf-8">'
        printf '<title>%s</title>\n' "$title"
        printf '%s\n' '<style>'
        sed -n '1,$p' "$report_dir/style.css"
        printf '%s\n' '</style>' '</head>' '<body>'
        cmark --unsafe --smart "$prepared"
        printf '%s\n' '</body>' '</html>'
    } > "$html"
    rm -f -- "$prepared"
done

for html in "$generated_dir"/*.html; do
    name=$(basename "$html" .html)
    rm -f -- "$report_dir/$name.pdf"
    python3 "$report_dir/render_reports.py" "$html" "$report_dir/$name.pdf"
done

expected=$(find "$source_dir" -maxdepth 1 -name '*.md' | wc -l)
actual=$(find "$report_dir" -maxdepth 1 -name '*.pdf' | wc -l)
minimum_pdf_words=2300
minimum_pdf_pages=10

if [[ "$expected" -ne "$actual" ]]; then
    printf 'Expected %s PDFs but found %s\n' "$expected" "$actual" >&2
    exit 1
fi

for pdf in "$report_dir"/*.pdf; do
    qpdf --check "$pdf" >/dev/null
    pdfinfo "$pdf" >/dev/null
    if [[ $(pdftotext "$pdf" - | wc -w) -lt "$minimum_pdf_words" ]]; then
        printf 'PDF has fewer than %s extractable words: %s\n' "$minimum_pdf_words" "$pdf" >&2
        exit 1
    fi
    if [[ $(pdfinfo "$pdf" | awk '/^Pages:/ {print $2}') -lt "$minimum_pdf_pages" ]]; then
        printf 'PDF has fewer than %s pages: %s\n' "$minimum_pdf_pages" "$pdf" >&2
        exit 1
    fi
done

printf 'Built and validated %s reports in %s\n' "$actual" "$report_dir"
