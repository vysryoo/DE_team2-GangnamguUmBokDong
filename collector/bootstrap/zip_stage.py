"""대형 원본 ZIP에서 부트스트랩에 필요한 CSV만 골라 스트리밍 복사한다.

CSV 본문은 해석하지 않고 중앙 디렉터리의 파일명과 크기만 본다. 수십 GB 원본을 통째로
풀지 않으려는 것이다.

    python -m bootstrap.zip_stage --zip ../data/archive.zip \
        --bootstrap-dir ../data/bootstrap --population-dir ../data/population \
        --from 2025-01-01 --to 2025-12-31 --rental-context-before-days 35 --dry-run

ASOS는 파일이 정확히 하나인지만 확인하고 행 날짜 제한은 bootstrap 날짜 필터가 맡는다.
대여 context 옵션은 경계 밖 트립을 위해 **대여 월 파일만** 추가한다.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import sys
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

_ZIP_UTF8_FLAG = 0x800
_COPY_BUFFER_BYTES = 1024 * 1024

_RENTAL_RE = re.compile(
    r"^서울특별시 공공자전거 대여이력 정보_(?P<yymm>\d{4})(?: \(\d+\))?\.csv$"
)
_STATION_RE = re.compile(r"^data_(?P<yymm>\d{4})\.csv$")
_ASOS_RE = re.compile(r"^OBS_ASOS_TIM_.+\.csv$")
_POPULATION_RE = re.compile(r"^250_LOCAL_RESD_(?P<ymd>\d{8})\.csv$")


class StageError(RuntimeError):
    """선택 또는 안전 검증에 실패했음을 나타낸다."""


@dataclass(frozen=True)
class Member:
    """안전 검증과 이름 정규화를 마친 ZIP 멤버를 나타낸다."""

    info: zipfile.ZipInfo
    normalized_name: str
    parts: tuple[str, ...]

    @property
    def basename(self) -> str:
        """정규화된 마지막 경로 조각을 반환한다."""
        return self.parts[-1]


@dataclass(frozen=True)
class Selection:
    """논리 입력 하나와 최종 출력 경로의 대응을 나타낸다."""

    logical_name: str
    member: Member
    destination: Path


@dataclass(frozen=True)
class StageSummary:
    """선택한 파일 수와 중앙 디렉터리 기준 크기를 담는다."""

    file_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    dry_run: bool


def _normalize_member_name(info: zipfile.ZipInfo) -> str:
    """ZIP 멤버명을 가능한 경우 UTF-8로 복구한 뒤 NFC로 정규화한다.

    macOS에서 만든 일부 ZIP은 UTF-8 플래그 없이 NFD UTF-8 바이트를 기록한다.
    ``zipfile``은 이를 CP437로 해석하므로 원래 바이트로 되돌린 뒤 UTF-8 디코딩을
    시도한다. 바이트가 UTF-8이 아니면 정상 CP437 이름일 수 있으므로 원문을
    보존한다.
    """
    name = info.filename
    if not info.flag_bits & _ZIP_UTF8_FLAG:
        try:
            name = name.encode("cp437").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return unicodedata.normalize("NFC", name)


def _validated_parts(info: zipfile.ZipInfo, normalized_name: str) -> tuple[str, ...]:
    """멤버 경로가 일반 상대 경로인지 검증하고 조각을 반환한다.

    raises:
        StageError: 절대 경로, 상위 경로 이동, 역슬래시, 심볼릭 링크 또는 기타
            특수 파일 형식일 때.
    """
    if not normalized_name or "\x00" in normalized_name:
        raise StageError("ZIP에 비어 있거나 NUL을 포함한 멤버명이 있다")
    if "\\" in normalized_name:
        raise StageError(f"ZIP 멤버 경로에 역슬래시가 있다: {normalized_name!r}")
    if normalized_name.startswith("/") or re.match(r"^[A-Za-z]:", normalized_name):
        raise StageError(f"ZIP 멤버가 절대 경로를 사용한다: {normalized_name!r}")

    raw_parts = normalized_name.split("/")
    if info.is_dir() and raw_parts[-1] == "":
        raw_parts.pop()
    if not raw_parts or any(part in {"", ".", ".."} for part in raw_parts):
        raise StageError(
            f"ZIP 멤버가 안전하지 않은 경로를 사용한다: {normalized_name!r}"
        )

    unix_mode = info.external_attr >> 16
    file_type = stat.S_IFMT(unix_mode)
    if stat.S_ISLNK(unix_mode):
        raise StageError(f"ZIP 멤버가 심볼릭 링크다: {normalized_name!r}")
    if info.create_system == 3 and file_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
        raise StageError(f"ZIP 멤버가 지원하지 않는 특수 파일이다: {normalized_name!r}")
    return tuple(raw_parts)


def _members(archive: zipfile.ZipFile) -> list[Member]:
    """중앙 디렉터리를 읽어 모든 일반 파일의 안전한 메타데이터를 반환한다."""
    members: list[Member] = []
    for info in archive.infolist():
        normalized_name = _normalize_member_name(info)
        parts = _validated_parts(info, normalized_name)
        if not info.is_dir():
            members.append(
                Member(info=info, normalized_name=normalized_name, parts=parts)
            )
    return members


def _has_path(m: Member, *expected: str) -> bool:
    """멤버의 부모 경로에 연속된 경로 조각이 있는지 반환한다."""
    parents = m.parts[:-1]
    width = len(expected)
    return any(
        parents[index : index + width] == expected
        for index in range(len(parents) - width + 1)
    )


def _days(first: date, last: date) -> list[date]:
    """양 끝을 포함한 날짜 목록을 반환한다."""
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def _months(first: date, last: date) -> list[str]:
    """날짜 범위와 겹치는 달을 YYMM 오름차순으로 반환한다."""
    result: list[str] = []
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        result.append(f"{year % 100:02d}{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return result


def _find_member(
    members: list[Member],
    logical_name: str,
    parent_path: tuple[str, ...],
    pattern: re.Pattern[str],
    key: str | None = None,
) -> Member:
    """부모 경로와 파일명 패턴이 맞는 멤버가 정확히 하나인지 확인해 반환한다.

    `key`를 주면 패턴의 첫 그룹이 그 값과 같은 멤버만 고른다.
    """
    candidates = [
        member
        for member in members
        if _has_path(member, *parent_path)
        and (match := pattern.fullmatch(member.basename)) is not None
        and (key is None or match.group(1) == key)
    ]
    if len(candidates) != 1:
        raise StageError(
            f"{logical_name} 멤버가 정확히 하나여야 한다: matches={len(candidates)}"
        )
    return candidates[0]


def _validate_range(
    first: date, last: date, rental_context_before_days: int, rental_context_after_days: int
) -> None:
    """요청 기간과 대여 context 일수가 staging 가능한 범위인지 확인한다."""
    if first > last:
        raise StageError(f"--from({first})이 --to({last})보다 뒤다")
    if first.year != last.year:
        raise StageError(
            "ASOS 출력이 연도별 하나이므로 --from과 --to는 같은 연도여야 한다"
        )
    if rental_context_before_days < 0 or rental_context_after_days < 0:
        raise StageError("대여 컨텍스트 일수는 0 이상이어야 한다")


def _select(
    members: list[Member],
    bootstrap_dir: Path,
    population_dir: Path,
    first: date,
    last: date,
    rental_context_before_days: int = 0,
    rental_context_after_days: int = 0,
) -> list[Selection]:
    """요청 기간과 대여 컨텍스트의 논리 파일을 선택해 출력 계획을 만든다."""
    _validate_range(first, last, rental_context_before_days, rental_context_after_days)
    selections: list[Selection] = []
    context_months = _months(
        first - timedelta(days=rental_context_before_days),
        last + timedelta(days=rental_context_after_days),
    )
    for yymm in context_months:
        name = f"rental:{yymm}"
        rental = _find_member(members, name, ("raw",), _RENTAL_RE, yymm)
        selections.append(Selection(name, rental, bootstrap_dir / rental.basename))

    for yymm in _months(first, last):
        name = f"station:{yymm}"
        station = _find_member(members, name, ("raw_station",), _STATION_RE, yymm)
        destination = bootstrap_dir / f"대여소별 공공자전거 대여가능 수량_{yymm}.csv"
        selections.append(Selection(name, station, destination))

    asos = _find_member(members, "weather:ASOS", ("raw_forecast",), _ASOS_RE)
    destination = bootstrap_dir / f"weather_realtime_{first.year}.csv"
    selections.append(Selection("weather:ASOS", asos, destination))

    for day in _days(first, last):
        ymd = day.strftime("%Y%m%d")
        name = f"population:{ymd}"
        path = ("raw_people", "250m", "resd")
        population = _find_member(members, name, path, _POPULATION_RE, ymd)
        selections.append(Selection(name, population, population_dir / population.basename))

    _validate_destinations(selections)
    return selections


def _validate_destinations(selections: list[Selection]) -> None:
    """선택 결과가 같은 출력 경로를 둘 이상 가리키지 않는지 확인한다."""
    seen: dict[str, str] = {}
    for selection in selections:
        key = os.path.abspath(selection.destination).casefold()
        previous = seen.get(key)
        if previous is not None:
            raise StageError(
                f"출력 경로가 충돌한다: {previous}, {selection.logical_name} -> "
                f"{selection.destination}"
            )
        seen[key] = selection.logical_name


def _guard_existing(selections: list[Selection], force: bool) -> None:
    """기존 출력이 있고 force가 아니면 쓰기를 거부한다."""
    if force:
        return
    existing = [
        selection.destination
        for selection in selections
        if selection.destination.exists()
    ]
    if existing:
        raise StageError(f"출력 파일이 이미 존재한다(--force 필요): {existing[0]}")


def _copy_entry(
    archive: zipfile.ZipFile,
    selection: Selection,
    temporary_path: Path,
) -> None:
    """ZIP 멤버 하나를 임시 파일로 스트리밍 복사하고 크기를 검증한다."""
    with (
        archive.open(selection.member.info, "r") as source,
        temporary_path.open("wb") as target,
    ):
        shutil.copyfileobj(source, target, length=_COPY_BUFFER_BYTES)
        target.flush()
        os.fsync(target.fileno())
    copied = temporary_path.stat().st_size
    if copied != selection.member.info.file_size:
        raise StageError(
            f"복사 크기가 중앙 디렉터리와 다르다: {selection.logical_name} "
            f"expected={selection.member.info.file_size} actual={copied}"
        )


def _extract(archive: zipfile.ZipFile, selections: list[Selection]) -> None:
    """모든 파일을 임시 경로에 준비한 뒤 각 목적지로 원자적으로 교체한다."""
    staged: list[tuple[Path, Path]] = []
    try:
        for selection in selections:
            selection.destination.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                dir=selection.destination.parent,
                prefix=f".{selection.destination.name}.",
                suffix=".part",
            )
            os.close(descriptor)
            temporary_path = Path(temporary_name)
            staged.append((temporary_path, selection.destination))
            _copy_entry(archive, selection, temporary_path)

        for temporary_path, destination in staged:
            os.replace(temporary_path, destination)
    finally:
        for temporary_path, _ in staged:
            temporary_path.unlink(missing_ok=True)


def stage(
    zip_path: Path,
    bootstrap_dir: Path,
    population_dir: Path,
    first: date,
    last: date,
    *,
    dry_run: bool = False,
    force: bool = False,
    rental_context_before_days: int = 0,
    rental_context_after_days: int = 0,
) -> StageSummary:
    """원본 ZIP에서 요청 기간의 CSV만 선택해 준비하고 요약을 반환한다.

    dry-run에서도 중앙 디렉터리, 안전성, 정확한 매칭, 출력 충돌과 기존 파일을
    모두 검증하지만 디렉터리나 파일은 만들지 않는다.

    args:
        zip_path: 원본 ZIP 경로.
        bootstrap_dir: 대여·재고·날씨 CSV 출력 디렉터리.
        population_dir: 일별 거주인구 CSV 출력 디렉터리.
        first: 포함할 첫 날짜.
        last: 포함할 마지막 날짜.
        dry_run: 참이면 쓰지 않고 선택 결과만 검증한다.
        force: 참이면 기존 출력 파일을 원자적으로 교체한다.
        rental_context_before_days: 시작일 전 대여 이력을 추가할 일수.
        rental_context_after_days: 종료일 후 대여 이력을 추가할 일수.
    returns:
        선택 파일 수와 압축 전후 합계 크기.
    raises:
        StageError: 안전 검증, 정확한 선택 또는 출력 조건이 맞지 않을 때.
        OSError: ZIP 또는 출력 파일을 읽고 쓰지 못할 때.
        zipfile.BadZipFile: 입력이 올바른 ZIP이 아닐 때.
    """
    if not zip_path.is_file():
        raise StageError(f"ZIP 경로가 파일이 아니다: {zip_path}")
    with zipfile.ZipFile(zip_path) as archive:
        selections = _select(
            _members(archive),
            bootstrap_dir,
            population_dir,
            first,
            last,
            rental_context_before_days,
            rental_context_after_days,
        )
        _guard_existing(selections, force)
        if not dry_run:
            _extract(archive, selections)

    return StageSummary(
        file_count=len(selections),
        uncompressed_bytes=sum(item.member.info.file_size for item in selections),
        compressed_bytes=sum(item.member.info.compress_size for item in selections),
        dry_run=dry_run,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        prog="python -m bootstrap.zip_stage",
        description="대형 archive ZIP에서 요청 기간의 부트스트랩 CSV만 안전하게 준비한다.",
    )
    parser.add_argument("--zip", required=True, type=Path, help="원본 ZIP 경로")
    parser.add_argument(
        "--bootstrap-dir", required=True, type=Path, help="대여·재고·날씨 출력 디렉터리"
    )
    parser.add_argument(
        "--population-dir", required=True, type=Path, help="거주인구 출력 디렉터리"
    )
    parser.add_argument(
        "--from", dest="from_date", required=True, help="시작 날짜 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to", dest="to_date", required=True, help="끝 날짜, 포함 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="중앙 목록만 검증하고 쓰지 않는다"
    )
    parser.add_argument(
        "--force", action="store_true", help="기존 출력 파일을 교체한다"
    )
    parser.add_argument(
        "--rental-context-before-days",
        type=int,
        default=0,
        help="시작일 전 대여 이력만 추가할 일수",
    )
    parser.add_argument(
        "--rental-context-after-days",
        type=int,
        default=0,
        help="종료일 후 대여 이력만 추가할 일수",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """선택 staging을 실행하고 파일 수와 중앙 디렉터리 기준 크기를 출력한다."""
    args = parse_args(argv)
    try:
        summary = stage(
            args.zip,
            args.bootstrap_dir,
            args.population_dir,
            date.fromisoformat(args.from_date),
            date.fromisoformat(args.to_date),
            dry_run=args.dry_run,
            force=args.force,
            rental_context_before_days=args.rental_context_before_days,
            rental_context_after_days=args.rental_context_after_days,
        )
    except (StageError, OSError, ValueError, zipfile.BadZipFile) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    mode = "dry-run" if summary.dry_run else "staged"
    print(
        f"mode={mode} files={summary.file_count} "
        f"uncompressed_bytes={summary.uncompressed_bytes} "
        f"compressed_bytes={summary.compressed_bytes}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
