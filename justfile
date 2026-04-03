lint:
    uv run ruff format
    uv run ruff check
    uv run pyrefly check


ruff:
    uv run ruff format
    uv run ruff check --fix


release bump="":
    #!/usr/bin/env bash
    set -euo pipefail
    uv version --bump {{bump}}
    version="v$(uv version --short)"
    echo "__version__ = \"$(uv version --short)\"" > src/narwhals_map/_version.py

nox:
    uv run nox
