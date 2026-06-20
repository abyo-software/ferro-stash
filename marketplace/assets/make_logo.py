#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
"""Generate FerroStash marketplace logos, reusing the shared abyo curled-cat mark.

Brand family (see ../../../../s4, ../../../../abyo-software, sibling ferrosca):
the copper-bronze curled-cat mark is the shared corporate element, and every
product reuses it framed by a product-specific motif. The cat mark is extracted
from the top half of the S4 square logo (the same source the FerroSCA / FerroDruid
logos use); if that source is unavailable we fall back to the corporate icon_base.

  FerroStash's motif = a stream / pipeline glyph: several "log line" streams flow
  in from the left, funnel down through a copper funnel, and exit as a single
  routed arrow on the right -- i.e. the input -> filter -> output pipeline. It is
  paired with the FerroStash wordmark and the log-pipeline tagline.

Name in dark slate, tagline in copper letterspaced caps -- matching the S4 /
FerroSCA / abyo siblings.

Output: marketplace/assets/ferro-stash-logo-square.png (900x900 RGB on white,
the AWS Marketplace product logo). Transparent + 640 + wide variants are also
produced for completeness.
"""
import os

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
# Shared corporate cat mark source (same as the FerroSCA / FerroDruid logos).
SRC = os.path.normpath(os.path.join(HERE, "..", "..", "..", "..", "s4", "assets",
                                    "marketplace", "s4-logo-square.png"))
# Fallback: the corporate icon if the s4 sibling checkout is absent.
SRC_FALLBACK = os.path.normpath(os.path.join(HERE, "..", "..", "..", "..",
                                             "abyo-software", "icon_base.png"))
OUT = HERE
PREFIX = "ferro-stash-logo"

COPPER_DARK = (90, 46, 26)
COPPER = (164, 86, 48)
COPPER_LIGHT = (200, 127, 79)
SLATE = (58, 52, 60)
WHITE = (255, 255, 255)

QS_BOLD = "/usr/share/fonts/truetype/quicksand/Quicksand-Bold.ttf"
QS_MED = "/usr/share/fonts/truetype/quicksand/Quicksand-Medium.ttf"

NAME = "FerroStash"
TAGLINE = "RUST-NATIVE LOG PIPELINE"


def _mark_source():
    return SRC if os.path.exists(SRC) else SRC_FALLBACK


def extract_mark():
    """Crop the cat spiral mark (above the 'S4' text) and key white->transparent."""
    src = _mark_source()
    im = Image.open(src).convert("RGBA")
    W, H = im.size
    px = im.load()
    # The s4 square logo has the cat in the top ~half; icon_base is the cat only.
    top_limit = int(H * 0.52) if src == SRC else H
    minx, miny, maxx, maxy = W, H, 0, 0
    for y in range(0, top_limit):
        for x in range(0, W):
            r, g, b, a = px[x, y]
            if a > 10 and not (r > 244 and g > 244 and b > 244):
                minx, miny = min(minx, x), min(miny, y)
                maxx, maxy = max(maxx, x), max(maxy, y)
    if minx >= maxx or miny >= maxy:
        minx, miny, maxx, maxy = 0, 0, W, top_limit
    pad = 6
    mark = im.crop((max(0, minx - pad), max(0, miny - pad),
                    min(W, maxx + pad), min(H, maxy + pad)))
    mp = mark.load()
    mw, mh = mark.size
    for y in range(mh):
        for x in range(mw):
            r, g, b, a = mp[x, y]
            lum = (r + g + b) / 3
            if lum > 250:
                mp[x, y] = (r, g, b, 0)
            elif lum > 220:
                mp[x, y] = (r, g, b, int(255 * (250 - lum) / 30))
    return mark


def draw_pipeline(draw, cx, cy, w, h, stroke, light, width):
    """Draw a stream/pipeline glyph: log-line streams -> funnel -> routed arrow.

    Centred on (cx, cy); spans roughly w x h. Drawn as copper strokes so it
    reads as the input -> filter -> output data pipeline.
    """
    half_w = w / 2
    left = cx - half_w
    right = cx + half_w
    # 1) Three incoming "log line" streams on the left, stacked + decreasing
    #    width, suggesting many event sources.
    line_x0 = left
    funnel_mouth_x = cx - w * 0.12
    spread = h * 0.30
    for i, dy in enumerate((-spread, 0.0, spread)):
        y = cy + dy
        x_end = funnel_mouth_x - (i == 1) * 0  # all reach the funnel mouth
        draw.line([(line_x0 + (abs(dy) * 0.15), y), (x_end, y)],
                  fill=light, width=width, joint="curve")
        # a little "packet" tick at the stream head
        draw.ellipse([line_x0 - width, y - width, line_x0 + width, y + width],
                     fill=stroke)

    # 2) The funnel: two converging lines from the mouth to a narrow throat.
    throat_x = cx + w * 0.10
    throat_half = h * 0.10
    top_mouth = cy - spread - h * 0.06
    bot_mouth = cy + spread + h * 0.06
    draw.line([(funnel_mouth_x, top_mouth), (throat_x, cy - throat_half)],
              fill=stroke, width=width, joint="curve")
    draw.line([(funnel_mouth_x, bot_mouth), (throat_x, cy + throat_half)],
              fill=stroke, width=width, joint="curve")
    # short throat walls
    draw.line([(throat_x, cy - throat_half), (cx + w * 0.20, cy - throat_half)],
              fill=stroke, width=width, joint="curve")
    draw.line([(throat_x, cy + throat_half), (cx + w * 0.20, cy + throat_half)],
              fill=stroke, width=width, joint="curve")

    # 3) The routed output: a single arrow exiting right.
    arr_tail = cx + w * 0.20
    arr_tip = right
    draw.line([(arr_tail, cy), (arr_tip, cy)], fill=stroke, width=width,
              joint="curve")
    ah = h * 0.16
    draw.line([(arr_tip - ah, cy - ah), (arr_tip, cy)], fill=stroke,
              width=width, joint="curve")
    draw.line([(arr_tip - ah, cy + ah), (arr_tip, cy)], fill=stroke,
              width=width, joint="curve")


def letterspaced(draw, xy, text, font, fill, spacing, anchor_center_x=None):
    widths = [draw.textlength(c, font=font) for c in text]
    total = sum(widths) + spacing * (len(text) - 1)
    x = (anchor_center_x - total / 2) if anchor_center_x is not None else xy[0]
    y = xy[1]
    for c, w in zip(text, widths):
        draw.text((x, y), c, font=font, fill=fill)
        x += w + spacing
    return total


def fit_font(draw, text, path, start, max_w):
    """Largest font size <= start whose rendered text width fits max_w."""
    size = start
    while size > 10:
        f = ImageFont.truetype(path, size)
        if draw.textlength(text, font=f) <= max_w:
            return f
        size -= 2
    return ImageFont.truetype(path, 10)


def build_square(transparent: bool):
    S = 900
    bg = (0, 0, 0, 0) if transparent else (*WHITE, 255)
    canvas = Image.new("RGBA", (S, S), bg)

    # Cat mark, centred in the upper portion of the canvas.
    mark = extract_mark()
    target_h = 270
    scale = target_h / mark.height
    mw, mh = int(mark.width * scale), int(mark.height * scale)
    mark = mark.resize((mw, mh), Image.LANCZOS)
    mx = (S - mw) // 2
    my = 95
    canvas.alpha_composite(mark, (mx, my))

    # Pipeline glyph below the mark (supersampled for crisp strokes).
    SS = 3
    art = Image.new("RGBA", (S * SS, S * SS), (0, 0, 0, 0))
    adraw = ImageDraw.Draw(art)
    draw_pipeline(adraw, (S / 2) * SS, 470 * SS, 430 * SS, 150 * SS,
                  (*COPPER, 245), (*COPPER_LIGHT, 235), width=int(15 * SS))
    art = art.resize((S, S), Image.LANCZOS)
    canvas.alpha_composite(art)

    draw = ImageDraw.Draw(canvas)
    name_font = fit_font(draw, NAME, QS_BOLD, 150, S - 120)
    nw = draw.textlength(NAME, font=name_font)
    draw.text(((S - nw) / 2, 590), NAME, font=name_font, fill=(*SLATE, 255))

    tag_font = fit_font(draw, TAGLINE, QS_MED, 35, S - 100)
    letterspaced(draw, (0, 770), TAGLINE, tag_font, (*COPPER, 255),
                 spacing=6, anchor_center_x=S / 2)

    suffix = "square-transparent" if transparent else "square"
    out = canvas if transparent else canvas.convert("RGB")
    out.save(f"{OUT}/{PREFIX}-{suffix}.png")
    return canvas


def build_wide():
    W, H = 1540, 480
    canvas = Image.new("RGBA", (W, H), (*WHITE, 255))

    mark = extract_mark()
    target_h = 210
    scale = target_h / mark.height
    mw, mh = int(mark.width * scale), int(mark.height * scale)
    mark = mark.resize((mw, mh), Image.LANCZOS)
    mx, my = 230 - mw // 2, (H // 2) - mh // 2 - 30
    canvas.alpha_composite(mark, (mx, my))

    SS = 3
    art = Image.new("RGBA", (W * SS, H * SS), (0, 0, 0, 0))
    adraw = ImageDraw.Draw(art)
    draw_pipeline(adraw, 230 * SS, (H - 110) * SS, 320 * SS, 110 * SS,
                  (*COPPER, 245), (*COPPER_LIGHT, 235), width=int(12 * SS))
    art = art.resize((W, H), Image.LANCZOS)
    canvas.alpha_composite(art)

    draw = ImageDraw.Draw(canvas)
    tx = 470
    name_font = fit_font(draw, NAME, QS_BOLD, 150, W - tx - 60)
    draw.text((tx, 120), NAME, font=name_font, fill=(*SLATE, 255))
    tag_font = fit_font(draw, TAGLINE, QS_MED, 36, W - tx - 60)
    letterspaced(draw, (tx + 6, 300), TAGLINE, tag_font, (*COPPER, 255), spacing=5)
    canvas.convert("RGB").save(f"{OUT}/{PREFIX}-wide.png")
    return canvas


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    sq = build_square(False)
    build_square(True)
    build_wide()
    # AWS Marketplace product logo caps at 640x640 PNG -- ship a downscaled copy.
    sq.resize((640, 640), Image.LANCZOS).convert("RGB").save(f"{OUT}/{PREFIX}-640.png")
    print("logos written to", OUT)
