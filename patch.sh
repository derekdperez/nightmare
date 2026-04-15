python3 - <<'PY'
from pathlib import Path

p = Path("server.py")
text = p.read_text()

if "from reporting.server_pages import render_dashboard_html" not in text:
    marker = "from reporting.extractor_reports import"
    if marker in text:
        text = text.replace(marker, "from reporting.server_pages import render_dashboard_html\n" + marker)
    else:
        text = "from reporting.server_pages import render_dashboard_html\n" + text

text = text.replace(
    "self._write_text(self._render_dashboard_html(), content_type=\"text/html; charset=utf-8\")",
    "self._write_text(render_dashboard_html(), content_type=\"text/html; charset=utf-8\")"
)

lines = text.splitlines()
out = []
skip = False
for line in lines:
    if line.startswith("def _render_dashboard_html(self) -> str:"):
        skip = True
        continue
    if skip:
        if line.startswith("def ") and not line.startswith("def _render_dashboard_html"):
            skip = False
            out.append(line)
        elif line.startswith("class "):
            skip = False
            out.append(line)
        else:
            continue
    else:
        out.append(line)

p.write_text("\n".join(out) + "\n")
print("patched server.py")
PY