"""GC 삭제 대상을 manifest에 먼저 남긴 뒤 지우고 결과를 확인한다.

과거 manifest가 삭제된 object를 계속 가리킬 수 있어, 무엇을 왜 지웠는지 GC manifest로
남긴다. 삭제 중 멈춰도 `prepared` 기록으로 지우려던 대상을 알 수 있다.
"""

from __future__ import annotations

from datetime import datetime

from core.kst import KST
from core.s3 import delete_objects, list_keys, write_json


def delete_with_manifest(
    gc_key: str,
    gc_manifest: dict,
    keys: list[str],
    list_prefix: str,
    *,
    completed: dict,
) -> None:
    """삭제 대상을 `prepared`로 기록하고 지운 뒤 완료 상태를 기록한다.

    Args:
        gc_key: GC manifest key
        gc_manifest: 두 기록에 공통으로 넣을 필드
        keys: 삭제할 object key. 비어 있으면 완료 기록만 남김
        list_prefix: 삭제 뒤 남은 object를 확인할 prefix
        completed: 완료 기록에 덧붙일 필드. `status`를 포함함
    Raises:
        RuntimeError: 삭제 뒤에도 object가 남았을 때
    """
    if keys:
        write_json(gc_key, {**gc_manifest, "status": "prepared"})
        delete_objects(keys)
        # S3 DeleteObjects는 key별 실패를 예외 없이 응답 본문에만 담으므로 다시 나열해 확인함
        remaining = set(keys) & set(list_keys(list_prefix))
        if remaining:
            raise RuntimeError(
                f"GC 삭제 확인에 실패했다: prefix={list_prefix} remaining={sorted(remaining)[:5]}"
            )
    write_json(
        gc_key,
        {**gc_manifest, **completed, "completed_at": datetime.now(KST).isoformat()},
    )
