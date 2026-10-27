"""Prints a version's section of CHANGELOG.md, for its GitHub release notes.

Usage: python release_notes.py VERSION

Exits with an error if there's no section for the version, or it's empty, so that a version is
never released without notes.
"""

import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[2] / "CHANGELOG.md"


def release_notes(version, changelog):
    # Each section starts with a "## <version>" heading, and runs until the next one. The heading
    # may also bracket the version, prefix it with "v", or follow it with a date
    for section in re.split(r"^## +", changelog, flags=re.MULTILINE)[1:]:
        heading, _, notes = section.partition("\n")
        words = heading.split()
        if words and words[0].strip("[]").lstrip("v") == version:
            return notes.strip()
    return None


if __name__ == "__main__":
    version = sys.argv[1]
    notes = release_notes(version, CHANGELOG.read_text(encoding="utf-8"))
    if not notes:
        sys.exit(
            f'CHANGELOG.md has no notes for {version}. Add them under a "## {version}" heading '
            '(e.g., by renaming "## Unreleased")'
        )
    print(notes)
