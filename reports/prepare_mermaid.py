#!/usr/bin/env python3
"""Render Mermaid fences and replace them with PDF-friendly image elements."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


FENCE = re.compile(r"```mermaid\s*\n(.*?)\n```", re.DOTALL)


def main() -> int:
    if len(sys.argv) != 5:
        print(
            "usage: prepare_mermaid.py SOURCE.md OUTPUT.md IMAGE_DIR REPORT_NAME",
            file=sys.stderr,
        )
        return 2

    source = Path(sys.argv[1])
    output = Path(sys.argv[2])
    image_dir = Path(sys.argv[3])
    report_name = sys.argv[4]
    image_dir.mkdir(parents=True, exist_ok=True)

    text = source.read_text(encoding="utf-8")
    counter = 0

    def render(match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        stem = f"{report_name}-{counter:02d}"
        mermaid_path = image_dir / f"{stem}.mmd"
        image_path = image_dir / f"{stem}.png"
        mermaid_path.write_text(match.group(1).strip() + "\n", encoding="utf-8")
        subprocess.run(
            [
                "mmdc",
                "-i",
                str(mermaid_path),
                "-o",
                str(image_path),
                "-t",
                "neutral",
                "-b",
                "white",
                "-w",
                "1100",
                "-H",
                "700",
                "-q",
            ],
            check=True,
        )
        mermaid_path.unlink()
        relative = image_path.relative_to(output.parent)
        return (
            '<div class="mermaid-image">'
            f'<img src="{relative.as_posix()}" alt="Rendered Mermaid diagram {counter}" />'
            "</div>"
        )

    transformed = FENCE.sub(render, text)
    output.write_text(transformed, encoding="utf-8")
    print(f"Rendered {counter} Mermaid diagrams for {report_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
