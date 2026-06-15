"""
md2html.py - Minimal Markdown-to-HTML converter for relatorio.md.
Handles: ATX headings, fenced code blocks, GFM tables, unordered/ordered
lists, blockquotes, bold (**), inline code (`), horizontal rules (---),
and paragraphs. Escapes HTML special chars in all text/code.

Usage:
    python tests/md2html.py <input.md> <output_body.html>
    (or import and call convert(md_text) -> html_body_string)
"""

import re
import sys
import html


# ---------------------------------------------------------------------------
# Inline rendering
# ---------------------------------------------------------------------------

def _inline(text: str) -> str:
    """Render inline Markdown within a single line of text."""
    # Escape HTML first, then apply markup (bold, inline code).
    # We need to be careful: escape *before* inserting tags.
    # Strategy: tokenise segments between backtick spans, escape prose, then bold.

    result = []
    # Split on inline code spans (`...`) first so we don't escape inside them.
    parts = re.split(r'(`[^`]+`)', text)
    for part in parts:
        if part.startswith('`') and part.endswith('`') and len(part) > 1:
            inner = html.escape(part[1:-1])
            result.append(f'<code>{inner}</code>')
        else:
            escaped = html.escape(part)
            # Bold: **text**
            escaped = re.sub(r'\*\*(.+?)\*\*', lambda m: f'<strong>{m.group(1)}</strong>', escaped)
            result.append(escaped)
    return ''.join(result)


# ---------------------------------------------------------------------------
# Block-level state machine
# ---------------------------------------------------------------------------

def convert(md: str) -> str:
    lines = md.splitlines()
    out = []

    i = 0
    n = len(lines)

    # Paragraph accumulator
    para_lines: list[str] = []

    def flush_para():
        if not para_lines:
            return
        content = ' '.join(_inline(l) for l in para_lines if l.strip())
        if content:
            out.append(f'<p>{content}</p>')
        para_lines.clear()

    while i < n:
        line = lines[i]
        raw = line  # keep original for fence detection

        # ------------------------------------------------------------------
        # Fenced code block  ```[lang]
        # ------------------------------------------------------------------
        if line.startswith('```'):
            flush_para()
            lang = line[3:].strip()
            code_lines = []
            i += 1
            while i < n and not lines[i].startswith('```'):
                code_lines.append(html.escape(lines[i]))
                i += 1
            # skip closing ```
            code_body = '\n'.join(code_lines)
            cls = f' class="language-{html.escape(lang)}"' if lang else ''
            out.append(f'<pre><code{cls}>{code_body}</code></pre>')
            i += 1
            continue

        # ------------------------------------------------------------------
        # ATX headings  # ... ######
        # ------------------------------------------------------------------
        m = re.match(r'^(#{1,6})\s+(.*)', line)
        if m:
            flush_para()
            level = len(m.group(1))
            content = _inline(m.group(2).rstrip())
            out.append(f'<h{level}>{content}</h{level}>')
            i += 1
            continue

        # ------------------------------------------------------------------
        # Horizontal rule  --- or *** or ___  (standalone)
        # ------------------------------------------------------------------
        if re.match(r'^[-*_]{3,}\s*$', line):
            flush_para()
            out.append('<hr>')
            i += 1
            continue

        # ------------------------------------------------------------------
        # GFM table  |...|
        # ------------------------------------------------------------------
        if '|' in line and i + 1 < n and re.match(r'^\|?[\s\-|:]+\|', lines[i + 1]):
            flush_para()
            header_cells = [c.strip() for c in line.strip().strip('|').split('|')]
            i += 1  # skip separator line
            i += 1
            rows = []
            while i < n and '|' in lines[i]:
                cells = [c.strip() for c in lines[i].strip().strip('|').split('|')]
                rows.append(cells)
                i += 1
            # build table
            th = ''.join(f'<th>{_inline(c)}</th>' for c in header_cells)
            out.append('<table>')
            out.append(f'<thead><tr>{th}</tr></thead>')
            out.append('<tbody>')
            for row in rows:
                td = ''.join(f'<td>{_inline(c)}</td>' for c in row)
                out.append(f'<tr>{td}</tr>')
            out.append('</tbody></table>')
            continue

        # ------------------------------------------------------------------
        # Blockquote  > ...
        # ------------------------------------------------------------------
        if line.startswith('>'):
            flush_para()
            bq_lines = []
            while i < n and lines[i].startswith('>'):
                bq_lines.append(_inline(lines[i][1:].strip()))
                i += 1
            inner = ' '.join(bq_lines)
            out.append(f'<blockquote><p>{inner}</p></blockquote>')
            continue

        # ------------------------------------------------------------------
        # Unordered list  - or *
        # ------------------------------------------------------------------
        if re.match(r'^(\s*)[-*]\s+', line):
            flush_para()
            out.append('<ul>')
            while i < n and re.match(r'^(\s*)[-*]\s+', lines[i]):
                content = re.sub(r'^(\s*)[-*]\s+', '', lines[i])
                out.append(f'<li>{_inline(content)}</li>')
                i += 1
            out.append('</ul>')
            continue

        # ------------------------------------------------------------------
        # Ordered list  1. ...
        # ------------------------------------------------------------------
        if re.match(r'^\d+\.\s+', line):
            flush_para()
            out.append('<ol>')
            while i < n and re.match(r'^\d+\.\s+', lines[i]):
                content = re.sub(r'^\d+\.\s+', '', lines[i])
                out.append(f'<li>{_inline(content)}</li>')
                i += 1
            out.append('</ol>')
            continue

        # ------------------------------------------------------------------
        # Blank line -> flush paragraph
        # ------------------------------------------------------------------
        if not line.strip():
            flush_para()
            i += 1
            continue

        # ------------------------------------------------------------------
        # Plain paragraph text
        # ------------------------------------------------------------------
        para_lines.append(line)
        i += 1

    flush_para()
    return '\n'.join(out)


# ---------------------------------------------------------------------------
# HTML wrapper (same skeleton as existing relatorio.html)
# ---------------------------------------------------------------------------

CSS = """\
* { box-sizing: border-box; }
body {
    font-family: Arial, Helvetica, sans-serif;
    font-size: 11pt;
    line-height: 1.6;
    color: #1a1a1a;
    max-width: 860px;
    margin: 0 auto;
    padding: 24mm 20mm;
}
h1 { font-size: 1.7em; border-bottom: 2px solid #333; padding-bottom: .3em; margin-top: 1em; }
h2 { font-size: 1.35em; border-bottom: 1px solid #ccc; padding-bottom: .2em; margin-top: 1.4em; }
h3 { font-size: 1.1em; margin-top: 1.2em; }
h4 { font-size: 1em; margin-top: 1em; }
p { margin: .6em 0; text-align: justify; }
pre {
    background: #f4f4f4;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 10px 14px;
    overflow-x: auto;
    font-size: 9pt;
    line-height: 1.45;
    page-break-inside: avoid;
}
code {
    font-family: Consolas, "Courier New", monospace;
    background: #f0f0f0;
    padding: 1px 4px;
    border-radius: 3px;
    font-size: 9pt;
}
pre code {
    background: none;
    padding: 0;
    border-radius: 0;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin: .8em 0;
    font-size: 10pt;
    page-break-inside: avoid;
}
th, td {
    border: 1px solid #bbb;
    padding: 5px 9px;
    text-align: left;
}
th { background: #e8e8e8; font-weight: bold; }
tr:nth-child(even) { background: #f9f9f9; }
ul, ol { padding-left: 1.6em; margin: .5em 0; }
li { margin: .2em 0; }
hr { border: none; border-top: 1px solid #ccc; margin: 1.4em 0; }
blockquote {
    border-left: 4px solid #aaa;
    margin: .8em 0;
    padding: .2em 1em;
    color: #555;
    background: #fafafa;
}
a { color: #1a5276; }
@media print {
    body { padding: 10mm 12mm; max-width: 100%; }
    pre, table { page-break-inside: avoid; }
}
"""

SKELETON = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relatorio</title>
<style>
{css}
</style>
</head>
<body>
{body}
</body>
</html>
"""


def wrap(body: str) -> str:
    return SKELETON.format(css=CSS, body=body)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('usage: python md2html.py input.md output.html')
        sys.exit(1)
    src, dst = sys.argv[1], sys.argv[2]
    with open(src, encoding='utf-8') as f:
        md_text = f.read()
    html_body = convert(md_text)
    full_html = wrap(html_body)
    with open(dst, 'w', encoding='utf-8') as f:
        f.write(full_html)
    print(f'written {dst}')
