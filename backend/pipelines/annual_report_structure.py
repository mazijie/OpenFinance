"""Section paths shared by parsing, Silver materialization and chunking."""

from __future__ import annotations

import re
from uuid import NAMESPACE_URL, uuid5


def heading_level(title: str) -> int:
    if re.match(r"^第[一二三四五六七八九十百0-9]+[章节]", title):
        return 1
    if re.match(r"^[一二三四五六七八九十百]+[、.]", title):
        return 2
    if re.match(r"^[（(][一二三四五六七八九十百0-9]+[）)]", title):
        return 3
    if re.match(r"^\d+[、.．]", title):
        return 4
    return 1


def annotate_sections(pages: list[dict], *, report_id: str, parse_run_id: str) -> list[dict]:
    """Assign region paths and produce a nested, page-addressable section tree."""
    if not pages:
        return []
    sections: list[dict] = []
    stack: list[dict] = []
    for page in pages:
        for region in page["regions"]:
            if region["region_type"] == "heading" and region["clean_text"].strip():
                title = re.sub(r"\s+", " ", region["clean_text"]).strip()
                level = heading_level(title)
                while stack and stack[-1]["level"] >= level:
                    stack.pop()
                parent = stack[-1] if stack else None
                path = f"{parent['section_path']}/{title}" if parent else title
                section = {
                    "section_id": str(uuid5(NAMESPACE_URL, f"{parse_run_id}:section:{region['region_id']}")),
                    "report_id": report_id, "parse_run_id": parse_run_id,
                    "section_title": title, "section_path": path, "level": level,
                    "parent_section_id": parent["section_id"] if parent else None,
                    "start_page": page["page_number"], "end_page": page["page_number"],
                    "section_order": len(sections) + 1,
                }
                sections.append(section)
                stack.append(section)
            if not sections:
                section = {
                    "section_id": str(uuid5(NAMESPACE_URL, f"{parse_run_id}:section:body")),
                    "report_id": report_id, "parse_run_id": parse_run_id,
                    "section_title": "报告正文", "section_path": "报告正文", "level": 1,
                    "parent_section_id": None, "start_page": page["page_number"],
                    "end_page": page["page_number"], "section_order": 1,
                }
                sections.append(section)
                stack.append(section)
            region["section_path"] = stack[-1]["section_path"] if stack else "报告正文"
            for ancestor in stack:
                ancestor["end_page"] = page["page_number"]
    return sections
