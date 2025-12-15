import os
from pathlib import Path
from typing import Literal

enabled_file_extensions = Literal["csv"]


def keep_only_files(path: str | Path, file_ext: enabled_file_extensions):
    print("executou")
    dest_path = Path(path)
    for item in dest_path.rglob("*"):
        if item.is_file() and item.suffix.lower() != f".{file_ext}":
            print(item.stem)
            os.remove(item)
