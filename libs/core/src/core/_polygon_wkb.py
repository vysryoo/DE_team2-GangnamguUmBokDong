"""외부 geometry 의존성 없이 2D Polygon WKB의 구조·면적·위상을 검증한다.

`libs/core`는 shapely에 의존하지 않으므로 POI Master 계약 검증에 필요한 최소한의
WKB 파싱, shoelace 면적, OGC 단순 ring·hole 포함 판정을 직접 구현한다.
"""

from __future__ import annotations

import math
import struct
from itertools import pairwise


class PolygonWkbError(ValueError):
    """Polygon WKB 구조나 위상이 계약을 위반했다."""


_Ring = tuple[tuple[float, float], ...]


def parse_polygon_wkb(payload: bytes | None) -> tuple[_Ring, ...]:
    """외부 geometry 의존성 없이 2D Polygon WKB 구조를 검증하고 ring 좌표를 반환한다."""
    if payload is None or len(payload) < 9:
        raise PolygonWkbError("Polygon WKB bytes가 없거나 너무 짧습니다.")
    byte_order = payload[0]
    if byte_order not in {0, 1}:
        raise PolygonWkbError("Polygon WKB byte order가 잘못됐습니다.")
    endian = "<" if byte_order == 1 else ">"
    geometry_type = struct.unpack_from(f"{endian}I", payload, 1)[0]
    if geometry_type != 3:
        raise PolygonWkbError("Geometry는 SRID/Z/M 없는 2D Polygon이어야 합니다.")
    ring_count = struct.unpack_from(f"{endian}I", payload, 5)[0]
    if ring_count < 1:
        raise PolygonWkbError("Polygon에는 ring이 하나 이상 있어야 합니다.")

    offset = 9
    rings: list[_Ring] = []
    for _ring_index in range(ring_count):
        if offset + 4 > len(payload):
            raise PolygonWkbError("Polygon ring header가 잘렸습니다.")
        point_count = struct.unpack_from(f"{endian}I", payload, offset)[0]
        offset += 4
        point_bytes = point_count * 16
        if point_count < 4 or offset + point_bytes > len(payload):
            raise PolygonWkbError("Polygon ring point 수 또는 길이가 잘못됐습니다.")
        points = tuple(
            struct.unpack_from(f"{endian}dd", payload, point_offset)
            for point_offset in range(offset, offset + point_bytes, 16)
        )
        if not all(math.isfinite(coordinate) for point in points for coordinate in point):
            raise PolygonWkbError("Polygon 좌표는 유한해야 합니다.")
        if points[0] != points[-1] or len(set(points[:-1])) < 3:
            raise PolygonWkbError(
                "Polygon ring은 닫혀 있고 서로 다른 점이 세 개 이상이어야 합니다."
            )
        rings.append(points)
        offset += point_bytes
    if offset != len(payload):
        raise PolygonWkbError("Polygon WKB 뒤에 불필요한 bytes가 있습니다.")
    return tuple(rings)


def polygon_area(rings: tuple[_Ring, ...]) -> float:
    """Exterior 면적에서 hole 면적을 뺀 Polygon 면적을 shoelace 공식으로 계산한다."""
    ring_areas: list[float] = []
    for points in rings:
        origin_x, origin_y = points[0]
        twice_area = math.fsum(
            (first[0] - origin_x) * (second[1] - origin_y)
            - (second[0] - origin_x) * (first[1] - origin_y)
            for first, second in pairwise(points)
        )
        if twice_area == 0:
            raise PolygonWkbError("Polygon ring 면적은 0보다 커야 합니다.")
        ring_areas.append(abs(twice_area) / 2.0)
    polygon_area = ring_areas[0] - math.fsum(ring_areas[1:])
    if not math.isfinite(polygon_area) or polygon_area <= 0:
        raise PolygonWkbError("Polygon의 ring 면적 합계는 0보다 커야 합니다.")
    return polygon_area


def validate_polygon_rings(rings: tuple[_Ring, ...]) -> None:
    """Polygon exterior와 hole이 OGC의 단순 ring·포함 불변식을 지키는지 검사한다."""
    normalized_rings = tuple(
        _collapse_consecutive_points(ring) for ring in rings
    )
    for ring in normalized_rings:
        _validate_simple_ring(ring)

    exterior = normalized_rings[0]
    holes = normalized_rings[1:]
    for hole in holes:
        hole_locations = tuple(
            (
                _point_inside_ring(point, exterior),
                _point_on_ring(point, exterior),
            )
            for point in hole[:-1]
        )
        if (
            _rings_have_invalid_contact(exterior, hole)
            or not any(inside for inside, _on_boundary in hole_locations)
            or not all(inside or on_boundary for inside, on_boundary in hole_locations)
        ):
            raise PolygonWkbError(
                "Polygon hole은 exterior 안에 있고 경계를 교차하면 안 됩니다."
            )
    for first_index, first_hole in enumerate(holes):
        for second_hole in holes[first_index + 1 :]:
            if (
                _rings_have_invalid_contact(first_hole, second_hole)
                or any(
                    _point_inside_ring(point, second_hole)
                    for point in first_hole[:-1]
                )
                or any(
                    _point_inside_ring(point, first_hole)
                    for point in second_hole[:-1]
                )
            ):
                raise PolygonWkbError(
                    "Polygon hole은 서로 교차·겹치거나 포함할 수 없습니다."
                )


def _collapse_consecutive_points(
    ring: tuple[tuple[float, float], ...],
) -> tuple[tuple[float, float], ...]:
    """위상을 바꾸지 않는 연속 중복 좌표를 검증용 ring에서 제거한다."""
    return tuple(
        point
        for index, point in enumerate(ring)
        if index == 0 or point != ring[index - 1]
    )


def _validate_simple_ring(ring: tuple[tuple[float, float], ...]) -> None:
    """Ring에 길이 0인 edge나 비인접 edge 교차가 없는지 검사한다."""
    segments = tuple(pairwise(ring))
    if any(start == end for start, end in segments):
        raise PolygonWkbError("Polygon ring에 길이 0인 edge가 있습니다.")
    last_index = len(segments) - 1
    for first_index, first_segment in enumerate(segments):
        for second_index in range(first_index + 1, len(segments)):
            if second_index == first_index + 1 or (
                first_index == 0 and second_index == last_index
            ):
                continue
            if _segments_intersect(first_segment, segments[second_index]):
                raise PolygonWkbError(
                    "Polygon ring의 비인접 edge가 교차하거나 맞닿습니다."
                )


def _rings_have_invalid_contact(
    first: tuple[tuple[float, float], ...],
    second: tuple[tuple[float, float], ...],
) -> bool:
    """두 ring이 교차·겹치거나 둘 이상의 점에서 접하는지 반환한다."""
    contacts: set[tuple[float, float]] = set()
    for first_segment in pairwise(first):
        for second_segment in pairwise(second):
            segment_contacts = _segment_contacts(first_segment, second_segment)
            if segment_contacts is None:
                return True
            contacts.update(segment_contacts)
            if len(contacts) > 1:
                return True
    return False


def _segment_contacts(
    first: tuple[tuple[float, float], tuple[float, float]],
    second: tuple[tuple[float, float], tuple[float, float]],
) -> set[tuple[float, float]] | None:
    """두 선분의 끝점 접촉을 반환하고 교차·선 겹침이면 None을 반환한다."""
    first_start, first_end = first
    second_start, second_end = second
    orientations = _orientations(first, second)
    if (
        orientations[0] * orientations[1] < 0
        and orientations[2] * orientations[3] < 0
    ):
        return None
    contacts = {
        point
        for point in (first_start, first_end, second_start, second_end)
        if _orientation(first_start, first_end, point) == 0
        and _point_on_segment(point, first_start, first_end)
        and _orientation(second_start, second_end, point) == 0
        and _point_on_segment(point, second_start, second_end)
    }
    if len(contacts) > 1:
        return None
    return contacts


def _segments_intersect(
    first: tuple[tuple[float, float], tuple[float, float]],
    second: tuple[tuple[float, float], tuple[float, float]],
) -> bool:
    """두 닫힌 선분이 교차·접촉·중첩하는지 반환한다."""
    first_start, first_end = first
    second_start, second_end = second
    orientations = _orientations(first, second)
    if orientations[0] == 0 and _point_on_segment(
        second_start, first_start, first_end
    ):
        return True
    if orientations[1] == 0 and _point_on_segment(
        second_end, first_start, first_end
    ):
        return True
    if orientations[2] == 0 and _point_on_segment(
        first_start, second_start, second_end
    ):
        return True
    if orientations[3] == 0 and _point_on_segment(
        first_end, second_start, second_end
    ):
        return True
    return orientations[0] != orientations[1] and orientations[2] != orientations[3]


def _orientations(
    first: tuple[tuple[float, float], tuple[float, float]],
    second: tuple[tuple[float, float], tuple[float, float]],
) -> tuple[int, int, int, int]:
    """각 선분에 대한 상대 선분 양 끝점의 방향 4개를 반환한다."""
    first_start, first_end = first
    second_start, second_end = second
    return (
        _orientation(first_start, first_end, second_start),
        _orientation(first_start, first_end, second_end),
        _orientation(second_start, second_end, first_start),
        _orientation(second_start, second_end, first_end),
    )


def _orientation(
    first: tuple[float, float],
    second: tuple[float, float],
    third: tuple[float, float],
) -> int:
    """세 점의 방향을 반시계 1, 시계 -1, 일직선 0으로 반환한다."""
    cross = (second[0] - first[0]) * (third[1] - first[1]) - (
        second[1] - first[1]
    ) * (third[0] - first[0])
    return (cross > 0) - (cross < 0)


def _point_on_segment(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> bool:
    """일직선인 점이 닫힌 선분의 bounding box 안에 있는지 반환한다."""
    return (
        min(start[0], end[0]) <= point[0] <= max(start[0], end[0])
        and min(start[1], end[1]) <= point[1] <= max(start[1], end[1])
    )


def _point_inside_ring(
    point: tuple[float, float],
    ring: tuple[tuple[float, float], ...],
) -> bool:
    """점이 ring 경계가 아닌 내부에 있는지 ray casting으로 판정한다."""
    inside = False
    for start, end in pairwise(ring):
        if _orientation(start, end, point) == 0 and _point_on_segment(
            point, start, end
        ):
            return False
        if (start[1] > point[1]) != (end[1] > point[1]):
            crossing_x = start[0] + (
                (point[1] - start[1]) * (end[0] - start[0])
                / (end[1] - start[1])
            )
            if crossing_x > point[0]:
                inside = not inside
    return inside


def _point_on_ring(
    point: tuple[float, float],
    ring: tuple[tuple[float, float], ...],
) -> bool:
    """점이 ring의 어느 닫힌 선분 위에 있는지 반환한다."""
    return any(
        _orientation(start, end, point) == 0
        and _point_on_segment(point, start, end)
        for start, end in pairwise(ring)
    )
