"""데이터 레이크 계층의 객체 키 규칙.

여러 모듈이 같은 계층을 읽고 쓰므로 경로 규칙을 한 곳에 둔다. `s3.py`는 버킷과
직렬화만 다루는 제네릭 입출력이고, "어떤 키에 무엇이 있는가"는 이 모듈이 정한다.
한 모듈만 쓰는 내부 키(bronze, quarantine, GC manifest 등)는 그 모듈이 갖는다.

`dt=`/`hh=` 파티션은 KST 기준이다. 예외는 source snapshot manifest로, 이 계층만
UTC 파티션을 쓴다. 시각 인자는 모두 timezone-aware여야 한다.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import NamedTuple

from .kst import KST
from .sources import LIVING_POPULATION_GRID

_SOURCE_SNAPSHOT_MANIFEST_KEY = re.compile(
    r"\Asource_snapshot_manifest/(?P<source_id>[a-z][a-z0-9_]*)/"
    r"dt=(?P<partition_day>\d{4}-\d{2}-\d{2})/"
    r"hh=(?P<partition_hour>\d{2})/"
    r"logical=(?P<logical>\d{8}T\d{12}Z)/"
    r"revision=(?P<revision>\d{10})\.json\Z"
)


class SourceSnapshotManifestKey(NamedTuple):
    """Source snapshot manifest key에서 읽은 identity다."""

    source_id: str
    logical_dttm: datetime
    revision_no: int


def _as_kst(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"키 시각은 timezone-aware여야 한다: {value!r}")
    return value.astimezone(KST)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"키 시각은 timezone-aware여야 한다: {value!r}")
    return value.astimezone(UTC)


def _window_path(source_id: str, window_start: datetime) -> str:
    local = _as_kst(window_start)
    return f"{source_id}/dt={local:%Y-%m-%d}/hh={local:%H}/{local:%H%M}"


def silver_prefix(source_id: str) -> str:
    """해당 소스의 Silver 객체 전체가 모이는 prefix를 만든다."""
    return f"silver/{source_id}/"


def silver_day_prefix(source_id: str, day: date) -> str:
    """해당 KST 날짜의 Silver 객체들이 모이는 prefix를 만든다.

    args:
        source_id: 소스 id
        day: KST 날짜
    returns:
        `silver/{source_id}/dt=YYYY-MM-DD/`
    """
    return f"{silver_prefix(source_id)}dt={day:%Y-%m-%d}/"


def silver_window_key(source_id: str, window_start: datetime) -> str:
    """윈도우당 파일 하나로 덮어쓰는 mutable Silver parquet 키를 만든다.

    args:
        source_id: 소스 id
        window_start: 윈도우 시작 시각
    returns:
        `silver/{source_id}/dt=YYYY-MM-DD/hh=HH/HHMM.parquet`
    """
    return f"silver/{_window_path(source_id, window_start)}.parquet"


def silver_content_key(source_id: str, window_start: datetime, sha256: str) -> str:
    """Collector가 쓰는 content-addressed immutable Silver parquet 키를 만든다.

    args:
        source_id: 소스 id
        window_start: 윈도우 시작 시각
        sha256: parquet bytes의 lowercase SHA-256
    returns:
        `silver/{source_id}/dt=YYYY-MM-DD/hh=HH/HHMM/sha256={sha256}.parquet`
    """
    return f"silver/{_window_path(source_id, window_start)}/sha256={sha256}.parquet"


def nowcast_key(day: date) -> str:
    """Nowcaster가 만드는 일자별 생활인구 추정치 parquet 키를 만든다.

    args:
        day: 추정 대상 KST 날짜
    returns:
        `silver/living_population_grid/dt=YYYY-MM-DD/hh=00/nowcast.parquet`
    """
    return f"{silver_day_prefix(LIVING_POPULATION_GRID, day)}hh=00/nowcast.parquet"


def window_manifest_key(source_id: str, window_start: datetime) -> str:
    """실행 진단용 mutable window manifest 키를 만든다.

    args:
        source_id: 소스 id
        window_start: 윈도우 시작 시각
    returns:
        `_manifest/{source_id}/dt=YYYY-MM-DD/hh=HH/HHMM.json`
    """
    return f"_manifest/{_window_path(source_id, window_start)}.json"


def window_manifest_prefix(source_id: str, day: date, hour: int | None = None) -> str:
    """KST 날짜(및 선택적으로 시)의 window manifest prefix를 만든다.

    args:
        source_id: 소스 id
        day: KST 날짜
        hour: KST 시(0~23). 생략하면 하루 전체
    returns:
        `_manifest/{source_id}/dt=YYYY-MM-DD/[hh=HH/]`
    """
    prefix = f"_manifest/{source_id}/dt={day:%Y-%m-%d}/"
    if hour is None:
        return prefix
    return f"{prefix}hh={hour:02d}/"


def source_snapshot_day_prefix(source_id: str, utc_day: date) -> str:
    """UTC 날짜의 source snapshot manifest prefix를 만든다.

    args:
        source_id: 소스 id
        utc_day: UTC 날짜
    returns:
        `source_snapshot_manifest/{source_id}/dt=YYYY-MM-DD/`
    """
    return f"source_snapshot_manifest/{source_id}/dt={utc_day:%Y-%m-%d}/"


def source_snapshot_hour_prefix(source_id: str, logical_dttm: datetime) -> str:
    """논리 시각이 속한 UTC 시간의 source snapshot manifest prefix를 만든다.

    args:
        source_id: 소스 id
        logical_dttm: 논리 시각
    returns:
        `source_snapshot_manifest/{source_id}/dt=YYYY-MM-DD/hh=HH/` (UTC)
    """
    utc = _as_utc(logical_dttm)
    return f"{source_snapshot_day_prefix(source_id, utc.date())}hh={utc:%H}/"


def source_snapshot_logical_prefix(source_id: str, logical_dttm: datetime) -> str:
    """논리 window 하나의 source snapshot manifest revision들이 모이는 prefix를 만든다.

    args:
        source_id: 소스 id
        logical_dttm: 논리 시각
    returns:
        `source_snapshot_manifest/.../logical=YYYYMMDDTHHMMSSffffffZ/` (UTC)
    """
    utc = _as_utc(logical_dttm)
    return (
        f"{source_snapshot_hour_prefix(source_id, utc)}"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
    )


def source_snapshot_manifest_key(
    source_id: str, logical_dttm: datetime, revision_no: int
) -> str:
    """Source snapshot correction revision의 immutable manifest 키를 만든다.

    args:
        source_id: 소스 id
        logical_dttm: 논리 시각
        revision_no: 0 이상의 correction 순번
    returns:
        `source_snapshot_manifest/.../logical=.../revision=NNNNNNNNNN.json`
    raises:
        ValueError: revision_no가 음수일 때
    """
    if revision_no < 0:
        raise ValueError(f"revision_no는 0 이상이어야 한다: {revision_no}")
    return (
        f"{source_snapshot_logical_prefix(source_id, logical_dttm)}"
        f"revision={revision_no:010d}.json"
    )


def parse_source_snapshot_manifest_key(key: str) -> SourceSnapshotManifestKey:
    """Source snapshot manifest 키를 파싱하고 UTC 파티션과 논리 시각의 일치를 검증한다.

    args:
        key: 버킷을 뺀 객체 키
    returns:
        source id, UTC 논리 시각, revision 순번
    raises:
        ValueError: canonical 키 형식이 아니거나 파티션과 논리 시각이 다를 때
    """
    matched = _SOURCE_SNAPSHOT_MANIFEST_KEY.fullmatch(key)
    if matched is None:
        raise ValueError(f"source snapshot manifest 키 형식이 아니다: {key}")
    try:
        logical = datetime.strptime(
            matched.group("logical"), "%Y%m%dT%H%M%S%fZ"
        ).replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError(f"source snapshot manifest 논리 시각이 잘못됐다: {key}") from exc
    if (
        matched.group("partition_day") != f"{logical:%Y-%m-%d}"
        or matched.group("partition_hour") != f"{logical:%H}"
    ):
        raise ValueError(f"source snapshot manifest UTC 파티션과 논리 시각이 다르다: {key}")
    return SourceSnapshotManifestKey(
        source_id=matched.group("source_id"),
        logical_dttm=logical,
        revision_no=int(matched.group("revision")),
    )


def archive_key(source_id: str, day: date) -> str:
    """archive 객체 키를 만든다.

    silver와 달리 `hh=` 파티션이 없고 날짜당 파일 하나다. archive는 "하루치"가 단위다.

    args:
        source_id: 소스 id
        day: 대상 날짜
    returns:
        `archive/{source_id}/dt=YYYY-MM-DD.parquet`
    """
    return f"archive/{source_id}/dt={day:%Y-%m-%d}.parquet"


def archive_prefix(source_id: str) -> str:
    """해당 소스의 archive 객체들이 모이는 prefix."""
    return f"archive/{source_id}/"
