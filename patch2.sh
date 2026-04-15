python3 - <<'PY'
from pathlib import Path

server = Path("server.py")
sp = Path("reporting/server_pages.py")

server_text = server.read_text()
sp_text = sp.read_text()

if "def render_workers_html() -> str:" not in sp_text:
    sp_text += '\n\ndef render_workers_html() -> str:\n    return render_template("worker_control.html.j2")\n'
    sp.write_text(sp_text)

if "from reporting.server_pages import render_dashboard_html, render_workers_html" not in server_text:
    if "from reporting.server_pages import render_dashboard_html" in server_text:
        server_text = server_text.replace(
            "from reporting.server_pages import render_dashboard_html",
            "from reporting.server_pages import render_dashboard_html, render_workers_html",
        )
    else:
        server_text = "from reporting.server_pages import render_dashboard_html, render_workers_html\n" + server_text

server_text = server_text.replace(
    'self._write_text(self._render_workers_html(), content_type="text/html; charset=utf-8")',
    'self._write_text(render_workers_html(), content_type="text/html; charset=utf-8")'
)

server.write_text(server_text)
print("patched server.py and reporting/server_pages.py")
PY