"""Packages the plugin as a zip that WeKit's Python plugin manager can import."""

import json
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = ROOT / "plugin"


def main() -> None:
    manifest = json.loads((PLUGIN_DIR / "plugin.json").read_text(encoding="utf-8"))
    output = ROOT / "dist" / f"{manifest['id']}-{manifest['version']}.zip"
    output.parent.mkdir(exist_ok=True)

    files = [PLUGIN_DIR / "plugin.json", *sorted(PLUGIN_DIR.glob("*.py"))]
    # Optional and private, so it's only included when it exists.
    blocked_words = PLUGIN_DIR / "blocked_words.txt"
    if blocked_words.exists():
        files.append(blocked_words)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, file.name)

    print(output)


if __name__ == "__main__":
    main()
