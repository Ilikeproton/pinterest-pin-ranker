from pathlib import Path
import sys

from PIL import Image


ICON_SIZES = [
    (16, 16),
    (20, 20),
    (24, 24),
    (32, 32),
    (40, 40),
    (48, 48),
    (64, 64),
    (128, 128),
    (256, 256),
]


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: make_windows_icon.py <src.png> <dst.ico>", file=sys.stderr)
        return 2

    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    dst.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(src) as image:
        image = image.convert("RGBA")
        image.save(dst, format="ICO", sizes=ICON_SIZES)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
