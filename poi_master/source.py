"""서울 열린데이터광장 페이지에서 POI Master 첨부파일을 발견하고 내려받는다."""

from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser

import httpx

DEFAULT_DATASET_PAGE_URL = (
    "https://data.seoul.go.kr/dataList/OA-21285/A/1/datasetView.do"
)
"""서울시 실시간 도시데이터의 공식 데이터셋 페이지다."""

DOWNLOAD_URL = (
    "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?useCache=false"
)
"""데이터셋 페이지의 첨부파일 다운로드 endpoint다."""

_INFO_ID = "OA-21285"
_INFO_SEQUENCE = "2"
_LIST_FILENAME = re.compile(
    r"서울시\s+주요\s+(?P<count>[1-9][0-9]*)장소\s+목록\.xlsx\Z"
)
_AREAS_FILENAME = re.compile(
    r"서울시\s+주요\s+(?P<count>[1-9][0-9]*)장소\s+영역\.zip\Z"
)
_DOWNLOAD_SEQUENCE = re.compile(r"downloadFile\s*\(\s*['\"](\d+)['\"]\s*\)")
_MODIFIED_DATE = re.compile(r"(?<![0-9])(\d{4}\.\d{2}\.\d{2}\.)(?![0-9])")
_ZIP_SIGNATURES = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")


class PoiSourceError(RuntimeError):
    """서울시 POI 첨부 발견 또는 다운로드 계약이 깨졌을 때 발생한다."""


@dataclass(frozen=True, slots=True)
class Attachment:
    """공식 페이지에서 발견한 첨부파일 한 개의 식별 정보를 보관한다."""

    role: str
    filename: str
    sequence: str
    modified_date: str
    declared_place_count: int

    def __post_init__(self) -> None:
        """역할별 파일명과 파일명이 선언한 장소 수가 정확히 같은지 검증한다."""
        pattern = {"list": _LIST_FILENAME, "areas": _AREAS_FILENAME}.get(self.role)
        matched = pattern.fullmatch(self.filename) if pattern is not None else None
        if matched is None or int(matched.group("count")) != self.declared_place_count:
            raise PoiSourceError(
                "POI 첨부 역할·파일명·선언 장소 수가 일치하지 않습니다: "
                f"role={self.role!r}, filename={self.filename!r}, "
                f"declared_place_count={self.declared_place_count!r}"
            )


@dataclass(frozen=True, slots=True)
class SourceAssets:
    """한 번의 확인에서 함께 내려받은 목록 XLSX와 영역 ZIP을 보관한다."""

    list_attachment: Attachment
    areas_attachment: Attachment
    list_bytes: bytes
    areas_bytes: bytes
    page_url: str = DEFAULT_DATASET_PAGE_URL


@dataclass(frozen=True, slots=True)
class _ParsedRow:
    """HTML 표의 한 행에서 모은 셀 텍스트와 다운로드 sequence를 보관한다."""

    cells: tuple[str, ...]
    sequences: tuple[str, ...]


class _AttachmentTableParser(HTMLParser):
    """첨부 표의 행 경계를 유지하며 텍스트와 downloadFile 호출을 수집한다."""

    def __init__(self) -> None:
        """빈 행 수집기를 초기화한다."""
        super().__init__(convert_charrefs=True)
        self.rows: list[_ParsedRow] = []
        self._in_row = False
        self._cell_depth = 0
        self._cell_text: list[str] = []
        self._cells: list[str] = []
        self._sequences: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """행·셀 시작을 추적하고 속성 안의 다운로드 sequence를 찾는다."""
        lowered = tag.casefold()
        if lowered == "tr":
            self._in_row = True
            self._cell_depth = 0
            self._cell_text = []
            self._cells = []
            self._sequences = []
        if not self._in_row:
            return
        if lowered in {"td", "th"}:
            self._cell_depth += 1
            if self._cell_depth == 1:
                self._cell_text = []
        for _name, raw_value in attrs:
            if raw_value is None:
                continue
            self._sequences.extend(_DOWNLOAD_SEQUENCE.findall(raw_value))

    def handle_data(self, data: str) -> None:
        """현재 셀 안의 가시 텍스트 조각을 수집한다."""
        if self._in_row and self._cell_depth > 0:
            self._cell_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        """셀과 행이 끝날 때 정규화한 결과를 확정한다."""
        lowered = tag.casefold()
        if not self._in_row:
            return
        if lowered in {"td", "th"} and self._cell_depth > 0:
            self._cell_depth -= 1
            if self._cell_depth == 0:
                text = " ".join("".join(self._cell_text).split())
                self._cells.append(text)
                self._cell_text = []
        if lowered == "tr":
            self.rows.append(
                _ParsedRow(cells=tuple(self._cells), sequences=tuple(self._sequences))
            )
            self._in_row = False


def parse_attachments(page_html: str) -> tuple[Attachment, Attachment]:
    """공식 페이지 HTML에서 목록 XLSX와 영역 ZIP을 파일명 기준으로 찾는다.

    행 번호나 현재 sequence 값은 서울시가 파일을 다시 올리면 바뀔 수 있으므로
    사용하지 않는다. 장소 수가 늘어 파일명의 숫자가 바뀌어도 역할을 찾을 수 있게
    파일명 패턴과 같은 행의 ``downloadFile('<seq>')``를 결합한다.
    """
    if not page_html.strip():
        raise PoiSourceError("서울시 데이터셋 페이지 HTML이 비어 있습니다.")
    parser = _AttachmentTableParser()
    parser.feed(page_html)

    found: dict[str, list[Attachment]] = {"list": [], "areas": []}
    for row in parser.rows:
        filenames: list[tuple[str, str, int]] = []
        for cell in row.cells:
            list_match = _LIST_FILENAME.fullmatch(cell)
            if list_match is not None:
                filenames.append(("list", cell, int(list_match.group("count"))))
            areas_match = _AREAS_FILENAME.fullmatch(cell)
            if areas_match is not None:
                filenames.append(("areas", cell, int(areas_match.group("count"))))
        if not filenames:
            continue
        sequences = tuple(dict.fromkeys(row.sequences))
        if len(sequences) != 1:
            raise PoiSourceError(
                "POI 첨부 행의 downloadFile sequence가 하나가 아닙니다: "
                f"cells={row.cells}, sequences={sequences}"
            )
        row_text = " ".join(row.cells)
        dates = tuple(dict.fromkeys(_MODIFIED_DATE.findall(row_text)))
        if len(dates) != 1:
            raise PoiSourceError(
                f"POI 첨부 행의 수정일을 하나로 판별할 수 없습니다: cells={row.cells}"
            )
        for role, filename, declared_place_count in filenames:
            found[role].append(
                Attachment(
                    role=role,
                    filename=filename,
                    sequence=sequences[0],
                    modified_date=dates[0],
                    declared_place_count=declared_place_count,
                )
            )

    for role in ("list", "areas"):
        if len(found[role]) != 1:
            raise PoiSourceError(
                f"서울시 POI {role} 첨부를 정확히 하나 찾지 못했습니다: "
                f"count={len(found[role])}"
            )
    list_attachment = found["list"][0]
    areas_attachment = found["areas"][0]
    if list_attachment.declared_place_count != areas_attachment.declared_place_count:
        raise PoiSourceError(
            "POI 목록과 영역 파일명이 선언한 장소 수가 다릅니다: "
            f"list={list_attachment.declared_place_count}, "
            f"areas={areas_attachment.declared_place_count}"
        )
    return list_attachment, areas_attachment


def _require_zip_payload(payload: bytes, attachment: Attachment) -> None:
    """XLSX와 ZIP이 공통으로 사용하는 ZIP container signature를 검증한다."""
    if not payload.startswith(_ZIP_SIGNATURES):
        raise PoiSourceError(
            f"서울시 {attachment.filename} 응답이 ZIP 계열 파일이 아닙니다: "
            f"prefix={payload[:32]!r}"
        )


def _download_attachment(client: httpx.Client, attachment: Attachment) -> bytes:
    """페이지에서 얻은 sequence로 첨부파일 한 개를 내려받고 signature를 확인한다."""
    try:
        response = client.post(
            DOWNLOAD_URL,
            data={
                "infId": _INFO_ID,
                "seq": attachment.sequence,
                "infSeq": _INFO_SEQUENCE,
            },
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise PoiSourceError(
            f"서울시 POI 첨부 다운로드에 실패했습니다: {attachment.filename}"
        ) from exc
    payload = response.content
    _require_zip_payload(payload, attachment)
    return payload


def fetch_source_assets(
    client: httpx.Client,
    *,
    page_url: str = DEFAULT_DATASET_PAGE_URL,
) -> SourceAssets:
    """공식 페이지를 읽어 그 시점의 목록과 영역 첨부를 모두 내려받는다."""
    try:
        response = client.get(page_url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise PoiSourceError("서울시 POI 데이터셋 페이지를 읽지 못했습니다.") from exc
    list_attachment, areas_attachment = parse_attachments(response.text)
    return SourceAssets(
        list_attachment=list_attachment,
        areas_attachment=areas_attachment,
        list_bytes=_download_attachment(client, list_attachment),
        areas_bytes=_download_attachment(client, areas_attachment),
        page_url=page_url,
    )
