from __future__ import annotations

import os
from pathlib import Path

import uvicorn


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    os.chdir(repo_root)
    current_path = os.environ.get("PYTHONPATH", "").strip()
    os.environ["PYTHONPATH"] = (
        str(repo_root) if not current_path else f"{repo_root}{os.pathsep}{current_path}"
    )

    # OneDrive/network-like folders can be unstable with native FS events.
    os.environ.setdefault("WATCHFILES_FORCE_POLLING", "true")

    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        app_dir=str(repo_root),
        reload_dirs=[str(repo_root / "app")],
    )


if __name__ == "__main__":
    main()
