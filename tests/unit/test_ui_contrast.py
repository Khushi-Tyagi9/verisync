"""Colour tokens: the exact values specified for both themes, defined once, and
WCAG AA (4.5:1) for every text/background pair the page uses."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

CSS = (Path(__file__).resolve().parents[2] / "dashboard" / "assets" / "app.css").read_text(encoding="utf-8")


def _tokens(block: str) -> dict[str, str]:
    return {k: v.upper() for k, v in re.findall(r"(--[a-z0-9-]+):\s*(#[0-9a-fA-F]{6})", block)}


def _blocks():
    light_start = CSS.index(":root {")
    dark_start = CSS.index("@media (prefers-color-scheme: dark)")
    dark_end = CSS.index("/* ---- base", dark_start)
    return CSS[light_start:dark_start], CSS[dark_start:dark_end]


def _themes():
    light_block, dark_block = _blocks()
    light = _tokens(light_block)
    return {"light": light, "dark": {**light, **_tokens(dark_block)}}


def _lum(hex_color: str) -> float:
    rgb = [int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def ratio(a: str, b: str) -> float:
    hi, lo = sorted((_lum(a), _lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


SPEC = {
    "light": {"--page": "#FAFAFA", "--surface": "#FFFFFF", "--border": "#E5E7EB",
              "--text": "#111827", "--muted": "#6B7280", "--accent": "#2F4BFF"},
    "dark": {"--page": "#0B0D12", "--surface": "#12151C", "--border": "#232733",
             "--text": "#E6E8EE", "--muted": "#9AA1B2", "--accent": "#7C8CFF"},
}


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_tokens_have_exactly_the_specified_values(theme):
    t = _themes()[theme]
    for name, value in SPEC[theme].items():
        assert t[name] == value, (theme, name, t[name])


def test_each_token_is_defined_once_per_theme():
    light_block, dark_block = _blocks()
    for block in (light_block, dark_block):
        names = re.findall(r"(--[a-z0-9-]+):\s*#", block)
        assert len(names) == len(set(names)), [n for n in names if names.count(n) > 1]
    # and nowhere else in the stylesheet
    rest = CSS.replace(light_block, "").replace(dark_block, "")
    assert not re.findall(r"--[a-z0-9-]+:\s*#[0-9a-fA-F]{6}", rest)


def test_dark_theme_redefines_every_colour_token():
    light_block, dark_block = _blocks()
    assert set(_tokens(dark_block)) == set(_tokens(light_block))


TEXT_PAIRS = [
    ("--text", "--page"), ("--text", "--surface"),
    ("--muted", "--page"), ("--muted", "--surface"),
    ("--accent", "--page"), ("--accent", "--surface"),          # links
    ("--on-accent", "--accent"),                                # primary button label
    ("--pill-grey-fg", "--pill-grey-bg"), ("--pill-blue-fg", "--pill-blue-bg"),
    ("--pill-red-fg", "--pill-red-bg"), ("--pill-amber-fg", "--pill-amber-bg"),
    ("--pill-purple-fg", "--pill-purple-bg"),
    ("--pill-red-fg", "--surface"),                             # error text, "unexpected"
    ("--text", "--pill-red-bg"),                                # alert body text
]


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("fg,bg", TEXT_PAIRS)
def test_text_pairs_meet_wcag_aa(theme, fg, bg):
    t = _themes()[theme]
    assert ratio(t[fg], t[bg]) >= 4.5, f"{theme}: {fg} on {bg} = {ratio(t[fg], t[bg]):.2f}"


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_focus_ring_is_visible_against_both_surfaces(theme):
    t = _themes()[theme]
    for surface in ("--page", "--surface"):
        assert ratio(t["--focus"], t[surface]) >= 3.0


def test_the_contrast_function_can_fail():
    assert ratio("#777777", "#808080") < 1.2
    assert ratio("#000000", "#FFFFFF") == pytest.approx(21.0)
