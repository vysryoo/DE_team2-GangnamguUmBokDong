"""유니코드 정규화 형태와 무관하게 파일명을 매칭하는 glob."""

from __future__ import annotations

import fnmatch
import unicodedata
from pathlib import Path


def glob_normalized(csv_dir: Path, pattern: str) -> list[Path]:
    """유니코드 정규화 형태와 무관하게 파일명을 매칭해 정렬된 목록으로 반환한다.

    macOS는 파일명을 NFD로 저장해 NFC 한글 패턴이 `Path.glob()`에서 에러 없이 0건
    매칭된다. 양쪽을 NFC로 맞춘 뒤 fnmatch로 비교한다.
    """
    normalized_pattern = unicodedata.normalize("NFC", pattern)
    return sorted(
        path
        for path in csv_dir.iterdir()
        if path.is_file()
        and fnmatch.fnmatch(unicodedata.normalize("NFC", path.name), normalized_pattern)
    )
