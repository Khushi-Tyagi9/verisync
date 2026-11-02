"""WCAG contrast of the colour tokens in both themes, read straight from the CSS."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CSS = (Path(__file__).resolve().parents[2] / "dashboard" / "assets" / "app.css").read_text(encoding="utf-8")


def _tokens(block: str) -> dict[str, str]:
    return dict(re.findall(r"(--[a-z0-9-]+):\s*(#[0-9a-fA-F]{6})", block))


def _themes():
    light_block = CSS[CSS.index(":root {"):CSS.index("@media (prefers-color-scheme: dark)")]
    dark_start = CSS.index("@media (prefers-color-scheme: dark)")
    dark_block = CSS[dark_start:CSS.index("* { box-sizing", dark_start)]
    light = _tokens(light_block)
    dark = {**light, **_tokens(dark_block)}
    return {"light": light, "dark": dark}


def _lum(hex_color: str) -> float:
    rgb = [int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def ratio(a: str, b: str) -> float:
    hi, lo = sorted((_lum(a), _lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


TEXT_PAIRS = [
    ("--ink", "--bg"), ("--ink", "--panel"), ("--ink", "--panel-2"),
    ("--muted", "--bg"), ("--muted", "--panel"), ("--muted", "--panel-2"),
    ("--accent", "--bg"), ("--accent", "--panel"),
    ("--on-accent", "--accent"), ("--on-accent", "--accent-hover"),
    ("--notice-ink", "--notice-bg"), ("--err-ink", "--err-bg"),
    ("--v-auto-ink", "--v-auto-bg"), ("--v-ok-ink", "--v-ok-bg"), ("--v-risk-ink", "--v-risk-bg"),
    ("--v-disp-ink", "--v-disp-bg"), ("--v-corr-ink", "--v-corr-bg"), ("--v-unres-ink", "--v-unres-bg"),
    # verdict colours used as text on the plain panel (review queue reasons, "as expected")
    ("--v-auto-ink", "--panel"), ("--v-disp-ink", "--panel"), ("--err-ink", "--panel"),
    ("--bg", "--ink"),   # selected chip: page background text on ink fill
]


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("fg,bg", TEXT_PAIRS)
def test_text_pairs_meet_wcag_aa(theme, fg, bg):
    t = _themes()[theme]
    assert ratio(t[fg], t[bg]) >= 4.5, f"{theme}: {fg} on {bg} = {ratio(t[fg], t[bg]):.2f}"


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_focus_ring_and_borders_are_visible_against_surfaces(theme):
    t = _themes()[theme]
    for surface in ("--bg", "--panel"):
        assert ratio(t["--focus"], t[surface]) >= 3.0
        assert ratio(t["--rule-strong"], t[surface]) >= 3.0, (theme, surface)


def test_dark_theme_redefines_every_colour_token():
    start = CSS.index("@media (prefers-color-scheme: dark)")
    dark_only = _tokens(CSS[start:CSS.index("* { box-sizing", start)])
    assert set(dark_only) == set(_themes()["light"])


def test_the_contrast_function_can_fail():
    assert ratio("#777777", "#808080") < 1.2
    assert ratio("#000000", "#ffffff") == pytest.approx(21.0)
