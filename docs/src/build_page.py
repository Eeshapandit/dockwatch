"""Build docs/index.html from the template + the projected basemap.

The page loads docs/data.json at runtime (written by export_page_data.py),
so you only need to rerun this if you change the template.

Run:  python docs/src/build_page.py
"""
from pathlib import Path

SRC = Path(__file__).parent
page = (SRC / "template.html").read_text()
page = page.replace("/*BASEMAP*/", (SRC / "basemap.json").read_text()).replace("/*DATA*/", "null")
(SRC.parent / "index.html").write_text(page)
print("wrote docs/index.html")
