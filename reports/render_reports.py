#!/usr/bin/env python3
"""Small, dependency-light HTML-to-PDF renderer for the study reports.

The project intentionally keeps Markdown as the source of truth.  cmark turns
that Markdown into simple HTML; this file turns the limited HTML vocabulary
used by the reports into a readable ReportLab document.  It is deliberately
not a general browser engine.
"""

from __future__ import annotations

import html
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    KeepTogether,
    ListFlowable,
    ListItem,
    PageTemplate,
    Paragraph,
    Preformatted,
    Spacer,
    Table,
    TableStyle,
)


class Node:
    def __init__(self, tag: str = "root", attrs: dict[str, str] | None = None):
        self.tag = tag
        self.attrs = attrs or {}
        self.children: list[Node | str] = []


class TreeParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Node()
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = Node(tag, {key: value or "" for key, value in attrs})
        self.stack[-1].children.append(node)
        if tag not in {"br", "hr", "meta", "link", "img", "input"}:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                self.stack = self.stack[:index]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


def text_of(node: Node | str) -> str:
    if isinstance(node, str):
        return node
    return "".join(text_of(child) for child in node.children)


def inline_markup(node: Node | str) -> str:
    """Convert the inline subset to ReportLab's Paragraph markup."""
    if isinstance(node, str):
        return html.escape(node, quote=False)
    inner = "".join(inline_markup(child) for child in node.children)
    if node.tag in {"strong", "b"}:
        return f"<b>{inner}</b>"
    if node.tag in {"em", "i"}:
        return f"<i>{inner}</i>"
    if node.tag == "code":
        return f'<font name="Courier" size="8.5" color="#173f5f">{inner}</font>'
    if node.tag == "br":
        return "<br/>"
    if node.tag == "a":
        return inner
    return inner


def clean_pre(value: str) -> str:
    return html.unescape(value).replace("\u00a0", " ").strip("\n")


def find_tag(node: Node, tag: str) -> Node | None:
    if node.tag == tag:
        return node
    for child in node.children:
        if isinstance(child, Node):
            found = find_tag(child, tag)
            if found is not None:
                return found
    return None


def descendants(node: Node, tag: str) -> list[Node]:
    found: list[Node] = []
    for child in node.children:
        if isinstance(child, Node):
            if child.tag == tag:
                found.append(child)
            found.extend(descendants(child, tag))
    return found


class ReportRenderer:
    def __init__(self, title: str):
        self.title = title
        base = getSampleStyleSheet()
        self.styles = {
            "h1": ParagraphStyle("ReportH1", parent=base["Title"], fontName="Helvetica-Bold", fontSize=22, leading=25, textColor=colors.HexColor("#173f5f"), spaceAfter=10, keepWithNext=True),
            "h2": ParagraphStyle("ReportH2", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=14, leading=17, textColor=colors.HexColor("#173f5f"), spaceBefore=12, spaceAfter=5, keepWithNext=True),
            "h3": ParagraphStyle("ReportH3", parent=base["Heading3"], fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=colors.HexColor("#285d75"), spaceBefore=9, spaceAfter=3, keepWithNext=True),
            "body": ParagraphStyle("ReportBody", parent=base["BodyText"], fontName="Helvetica", fontSize=9.4, leading=13.4, textColor=colors.HexColor("#172033"), spaceAfter=6, alignment=TA_LEFT),
            "small": ParagraphStyle("ReportSmall", parent=base["BodyText"], fontName="Helvetica", fontSize=8.2, leading=10.5, textColor=colors.HexColor("#52606d"), spaceAfter=4),
            "quote": ParagraphStyle("ReportQuote", parent=base["BodyText"], fontName="Helvetica-Oblique", fontSize=9.2, leading=12.5, textColor=colors.HexColor("#173f5f"), leftIndent=7, rightIndent=7, spaceAfter=4),
            "table": ParagraphStyle("ReportTable", parent=base["BodyText"], fontName="Helvetica", fontSize=7.8, leading=10, textColor=colors.HexColor("#172033"), spaceAfter=0),
            "table_head": ParagraphStyle("ReportTableHead", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=7.8, leading=10, textColor=colors.white, spaceAfter=0),
            "code": ParagraphStyle("ReportCode", parent=base["Code"], fontName="Courier", fontSize=7.3, leading=9.2, textColor=colors.HexColor("#f4fbfd"), leftIndent=0, rightIndent=0, spaceAfter=0),
        }

    def paragraph(self, node: Node, style: str = "body") -> Paragraph:
        return Paragraph(inline_markup(node).strip(), self.styles[style])

    def list_flowable(self, node: Node, ordered: bool) -> ListFlowable:
        items = []
        for child in node.children:
            if isinstance(child, Node) and child.tag == "li":
                # Most report lists are one paragraph; rendering the whole
                # item at once preserves inline <code>/<strong> markup.
                item = Paragraph(inline_markup(child).strip(), self.styles["body"])
                items.append(ListItem([item], leftIndent=10))
        return ListFlowable(items, bulletType="1" if ordered else "bullet", start="1" if ordered else "circle", leftIndent=14, bulletFontName="Helvetica", bulletFontSize=8)

    def table_flowable(self, node: Node) -> Table:
        rows: list[list[Paragraph]] = []
        header = False
        for tr in descendants(node, "tr"):
            cells = descendants(tr, "th") + descendants(tr, "td")
            if not cells:
                continue
            if any(c.tag == "th" for c in cells):
                header = True
            rows.append([Paragraph(inline_markup(cell).strip(), self.styles["table_head" if cell.tag == "th" else "table"]) for cell in cells])
        if not rows:
            return Table([[""]])
        widths = [None] * max(len(row) for row in rows)
        table = Table(rows, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
        commands = [
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#c7d5df")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
        if header:
            commands.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#173f5f")))
        for row in range(1 if header else 0, len(rows), 2):
            commands.append(("BACKGROUND", (0, row), (-1, row), colors.HexColor("#f3f8fa")))
        table.setStyle(TableStyle(commands))
        return table

    def pre_flowable(self, node: Node):
        value = clean_pre(text_of(node))
        style_class = node.attrs.get("class", "")
        if "diagram" in style_class:
            fg, bg = colors.HexColor("#f4fbfd"), colors.HexColor("#173f5f")
        elif "code-microscope" in style_class:
            fg, bg = colors.HexColor("#173f5f"), colors.HexColor("#f7fafc")
        else:
            fg, bg = colors.HexColor("#172033"), colors.HexColor("#f2f4f7")
        style = ParagraphStyle("PreBlock", parent=self.styles["code"], textColor=fg, fontName="Courier", fontSize=7.2, leading=9.1)
        block = Preformatted(value, style, maxLineLength=105)
        table = Table([[block]], colWidths=[None], hAlign="LEFT")
        table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), bg), ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#9bc2d2")), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 7), ("BOTTOMPADDING", (0, 0), (-1, -1), 7)]))
        return table

    def flowables(self, nodes: list[Node | str]) -> list:
        result = []
        for node in nodes:
            if isinstance(node, str):
                if node.strip():
                    result.append(Paragraph(inline_markup(node).strip(), self.styles["body"]))
                continue
            if node.tag in {"h1", "h2", "h3"}:
                result.append(self.paragraph(node, node.tag))
                if node.tag == "h1":
                    result.append(HRFlowable(width="100%", thickness=2.2, color=colors.HexColor("#2f80a3"), spaceAfter=7))
            elif node.tag == "p":
                result.append(self.paragraph(node))
            elif node.tag == "blockquote":
                box = Table([[Paragraph(inline_markup(node).strip(), self.styles["quote"])]], colWidths=[None])
                box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6f8")), ("LINEBEFORE", (0, 0), (0, -1), 4, colors.HexColor("#2f80a3")), ("LEFTPADDING", (0, 0), (-1, -1), 9), ("RIGHTPADDING", (0, 0), (-1, -1), 9), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
                result.append(box)
            elif node.tag == "pre":
                result.append(self.pre_flowable(node))
                result.append(Spacer(1, 5))
            elif node.tag in {"ul", "ol"}:
                result.append(self.list_flowable(node, node.tag == "ol"))
                result.append(Spacer(1, 3))
            elif node.tag == "table":
                result.append(self.table_flowable(node))
                result.append(Spacer(1, 5))
            elif node.tag == "hr":
                result.append(HRFlowable(width="100%", thickness=0.7, color=colors.HexColor("#bed3df"), spaceBefore=6, spaceAfter=8))
            elif node.tag == "div":
                class_name = node.attrs.get("class", "")
                body = [x for x in self.flowables(node.children) if x is not None]
                if "callout" in class_name:
                    box = Table([[body]], colWidths=[None])
                    box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fff7ed")), ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#9bc2d2")), ("LINEBEFORE", (0, 0), (0, -1), 5, colors.HexColor("#f28e2b")), ("LEFTPADDING", (0, 0), (-1, -1), 9), ("RIGHTPADDING", (0, 0), (-1, -1), 9), ("TOPPADDING", (0, 0), (-1, -1), 7), ("BOTTOMPADDING", (0, 0), (-1, -1), 7)]))
                    result.append(box)
                else:
                    result.extend(body)
            elif node.tag in {"thead", "tbody", "tr", "td", "th", "span"}:
                result.extend(self.flowables(node.children))
        return result

    def build(self, html_path: Path, pdf_path: Path) -> None:
        parser = TreeParser()
        parser.feed(html_path.read_text(encoding="utf-8"))
        body = find_tag(parser.root, "body") or parser.root
        flow = self.flowables(body.children)
        doc = BaseDocTemplate(str(pdf_path), pagesize=A4, leftMargin=17 * mm, rightMargin=17 * mm, topMargin=16 * mm, bottomMargin=17 * mm, title=self.title, author="CoreDump study reports")
        frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")

        def footer(canvas, document):
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor("#bed3df"))
            canvas.line(doc.leftMargin, 11 * mm, A4[0] - doc.rightMargin, 11 * mm)
            canvas.setFont("Helvetica", 7.5)
            canvas.setFillColor(colors.HexColor("#52606d"))
            canvas.drawString(doc.leftMargin, 7 * mm, "CoreDump student study report")
            canvas.drawRightString(A4[0] - doc.rightMargin, 7 * mm, f"{document.page}")
            canvas.restoreState()

        doc.addPageTemplates([PageTemplate(id="report", frames=[frame], onPage=footer)])
        doc.build(flow)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: render_reports.py INPUT.html OUTPUT.pdf", file=sys.stderr)
        return 2
    source = Path(sys.argv[1])
    target = Path(sys.argv[2])
    title_match = re.search(r"<title>(.*?)</title>", source.read_text(encoding="utf-8"), re.S)
    title = html.unescape(title_match.group(1).strip()) if title_match else source.stem
    ReportRenderer(title).build(source, target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
