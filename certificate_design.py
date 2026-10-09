"""Bounded certificate artwork validation and shared print rendering."""

from __future__ import annotations

import math
from io import BytesIO
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

MAX_ASSET_BYTES = 20 * 1024 * 1024
MAX_ASSET_PIXELS = 16_000_000
PAGE_SIZES_MM = {"A4": (210.0, 297.0), "LETTER": (215.9, 279.4)}


def image_metadata(content: bytes) -> dict[str, Any]:
    if not content or len(content) > MAX_ASSET_BYTES:
        raise ValueError("image must be non-empty and at most 20 MiB")
    try:
        with Image.open(BytesIO(content)) as image:
            if image.format not in {"PNG", "JPEG"}:
                raise ValueError("only PNG and JPEG images are supported")
            if image.width * image.height > MAX_ASSET_PIXELS:
                raise ValueError("image exceeds 16 million pixels")
            metadata = {
                "mediaType": "image/png"
                if image.format == "PNG"
                else "image/jpeg",
                "widthPx": image.width,
                "heightPx": image.height,
            }
            image.verify()
        return metadata
    except (OSError, SyntaxError, Image.DecompressionBombError) as exc:
        raise ValueError("image content is invalid or unsafe") from exc


def render_pdf(
    spec: dict[str, Any],
    values: dict[str, str],
    background: bytes | None,
    signature: bytes | None,
    *,
    preview: bool = False,
) -> bytes:
    print_spec = spec["printSpec"]
    page = str(print_spec.get("page", "A4")).upper()
    orientation = str(print_spec.get("orientation", "PORTRAIT")).upper()
    if page not in PAGE_SIZES_MM or orientation not in {
        "PORTRAIT",
        "LANDSCAPE",
    }:
        raise ValueError("choose A4 or LETTER and PORTRAIT or LANDSCAPE")
    dpi = float(print_spec.get("dpi", 300))
    maximum = 300 if preview else 600
    if not math.isfinite(dpi) or not 150 <= dpi <= maximum:
        raise ValueError(f"resolution must be between 150 and {maximum} DPI")
    width_mm, height_mm = PAGE_SIZES_MM[page]
    if orientation == "LANDSCAPE":
        width_mm, height_mm = height_mm, width_mm
    size = (
        math.ceil(width_mm / 25.4 * dpi),
        math.ceil(height_mm / 25.4 * dpi),
    )
    if background:
        image_metadata(background)
        with Image.open(BytesIO(background)) as image:
            canvas = ImageOps.exif_transpose(image).convert("RGB").resize(size)
    else:
        canvas = Image.new("RGB", size, "white")
    signature_image = None
    if signature:
        image_metadata(signature)
        with Image.open(BytesIO(signature)) as image:
            signature_image = ImageOps.exif_transpose(image).convert("RGBA")
    draw = ImageDraw.Draw(canvas)
    for element in spec["elements"]:
        x, y, width, height = (
            int(float(element[field]) * dimension)
            for field, dimension in (
                ("x", canvas.width),
                ("y", canvas.height),
                ("width", canvas.width),
                ("height", canvas.height),
            )
        )
        if element["kind"] == "MANAGER_SIGNATURE" and signature_image:
            image = signature_image.copy()
            image.thumbnail((max(1, width), max(1, height)))
            canvas.paste(
                image,
                (
                    x + (width - image.width) // 2,
                    y + (height - image.height) // 2,
                ),
                image,
            )
            continue
        text = str(values.get(element["kind"], element.get("label", "")))[:200]
        font_size = max(10, int(height * 0.55))
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", font_size)
        except OSError:
            try:
                font = ImageFont.load_default(size=font_size)
            except TypeError:  # Compatibility with Pillow 10.0.
                font = ImageFont.load_default()
        bounds = draw.textbbox((0, 0), text, font=font)
        # Shrink long names to the positioned field instead of overflowing it.
        if bounds[2] - bounds[0] > width and hasattr(font, "font_variant"):
            font = font.font_variant(
                size=max(8, int(font_size * width / (bounds[2] - bounds[0])))
            )
            bounds = draw.textbbox((0, 0), text, font=font)
        draw.text(
            (
                x + max(0, (width - bounds[2] + bounds[0]) // 2),
                y + max(0, (height - bounds[3] + bounds[1]) // 2) - bounds[1],
            ),
            text,
            fill="black",
            font=font,
        )
    if preview:
        draw.text((20, 20), "PREVIEW / MOCK DATA", fill="#a33a32")
    output = BytesIO()
    canvas.save(output, format="PDF", resolution=dpi)
    return output.getvalue()
