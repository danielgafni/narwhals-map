#!/usr/bin/env -S uv run --script --quiet
# /// script
# dependencies = ["nox", "nox-uv"]
# ///

import nox
import nox_uv

nox.options.default_venv_backend = "uv"


@nox_uv.session(
    python=["3.10", "3.11", "3.12", "3.13", "3.14"],
    uv_groups=["test"],
    uv_all_extras=True,
)
def test(s: nox.Session) -> None:
    s.run("python", "-m", "pytest")
