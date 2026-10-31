"""월간 재학습이 쓸 챔피언 프로필과 그 피처마트의 갱신 필요 여부를 S3에서 판정한다."""

from __future__ import annotations

from datetime import UTC, datetime

from orchestration.aws_infra_task import MODELS_PREFIX, read_s3_json


def champion_profile_name(model_name: str) -> str | None:
    """현재 챔피언 모델이 학습에 쓴 프로필 이름을 읽는다.

    Args:
        model_name: ``rental`` 또는 ``return``
    Returns:
        챔피언 포인터와 프로필 JSON을 따라 읽은 프로필 이름. 챔피언이 없으면 None
    """
    pointer = read_s3_json(f"{MODELS_PREFIX}/champion/{model_name}.json")
    archive_prefix = (pointer or {}).get("archive_prefix")
    if not archive_prefix:
        return None
    payload = read_s3_json(f"{archive_prefix}/{model_name}_profile.json")
    return (payload or {}).get("profile_name")


def is_feature_mart_fresh(profile: str, model_name: str, max_age_hours: float = 24.0) -> bool:
    """프로필의 공통·모델 전용 피처마트가 모두 최근에 같은 시각까지 갱신됐는지 판정한다.

    Args:
        profile: 프로필 이름
        model_name: ``rental`` 또는 ``return``
        max_age_hours: 이보다 오래된 watermark는 갱신이 필요한 것으로 봄
    Returns:
        두 watermark가 모두 ``max_age_hours`` 안이고 ``max_hour_ts``가 같으면 True.
        판정 중 오류가 나면 False
    """
    try:
        profile_data = (
            read_s3_json(f"profiles/{profile}.json")
            or read_s3_json(f"{MODELS_PREFIX}/profiles/{profile}.json")
            or {}
        )
        combo_id, anchor_tick = _extract_profile_feature_params(profile_data)
        base_wm = read_s3_json(f"processed/features/{combo_id}/_watermark.json") or {}
        model_wm = read_s3_json(
            f"processed/features/{combo_id}/training_anchor_a{anchor_tick}/"
            f"_multi_horizon_{model_name}_watermark.json"
        ) or {}
        now = datetime.now(UTC)
        is_recent = all(
            wm.get("updated_at")
            and (now - datetime.fromisoformat(wm["updated_at"])).total_seconds() <= max_age_hours * 3600
            for wm in (base_wm, model_wm)
        )
        return is_recent and base_wm.get("max_hour_ts") == model_wm.get("max_hour_ts")
    except Exception:  # noqa: BLE001 - 판정에 실패하면 피처마트를 다시 만듦
        return False


def _first_profile_value(profile_data: dict, keys: tuple[str, ...], default: int) -> int:
    return next((profile_data[key] for key in keys if profile_data.get(key)), default)


def _extract_profile_feature_params(profile_data: dict) -> tuple[str, int]:
    # 프로필 JSON은 대문자 키와 소문자 키가 섞여 있음
    window = _first_profile_value(
        profile_data, ("ROLLING_WINDOW_MINUTES", "rolling_window_minutes"), 60
    )
    embargo = _first_profile_value(
        profile_data, ("ROLLING_EMBARGO_MINUTES", "rolling_embargo_minutes"), 40
    )
    tick = _first_profile_value(
        profile_data, ("ROLLING_TICK_MINUTES", "rolling_tick_minutes"), 20
    )
    anchor_tick = _first_profile_value(
        profile_data,
        (
            "TRAIN_ANCHOR_TICK_MINUTES",
            "train_anchor_tick_minutes",
            "GRID_TICK_MINUTES",
            "grid_tick_minutes",
        ),
        20,
    )
    return f"w{window}_e{embargo}_t{tick}", int(anchor_tick)
