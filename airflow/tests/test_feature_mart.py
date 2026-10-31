"""월간 재학습의 챔피언 프로필 조회와 피처마트 갱신 필요 판정을 검증한다."""

from orchestration import feature_mart


def test_champion_profile_name_reads_pointer_then_profile_json(monkeypatch) -> None:
    """챔피언 포인터의 archive_prefix를 따라가 `{model}_profile.json`의 profile_name을 읽는다."""
    reads = {}

    def fake_read_s3_json(key):
        reads[key] = True
        if key == "models/champion/rental.json":
            return {"archive_prefix": "models/archive/2026-01-01/leaves127"}
        if key == "models/archive/2026-01-01/leaves127/rental_profile.json":
            return {"profile_name": "leaves127"}
        return None

    monkeypatch.setattr(feature_mart, "read_s3_json", fake_read_s3_json)

    assert feature_mart.champion_profile_name("rental") == "leaves127"
    assert "models/champion/rental.json" in reads


def test_champion_profile_name_none_when_no_champion_yet(monkeypatch) -> None:
    monkeypatch.setattr(feature_mart, "read_s3_json", lambda key: None)
    assert feature_mart.champion_profile_name("rental") is None


def test_extract_profile_feature_params_handles_uppercase_and_defaults() -> None:
    """프로필 대문자 키와 기본값(embargo=40)이 정확히 반영되는지 검증한다."""
    # 1. 대문자 계약 딕셔너리
    uppercase_profile = {
        "ROLLING_WINDOW_MINUTES": 60,
        "ROLLING_EMBARGO_MINUTES": 40,
        "ROLLING_TICK_MINUTES": 20,
        "TRAIN_ANCHOR_TICK_MINUTES": 20,
        "PEAK_ANCHOR_TICK_MINUTES": 20,
    }
    combo_id, anchor_tick = feature_mart._extract_profile_feature_params(uppercase_profile)
    assert combo_id == "w60_e40_t20"
    assert anchor_tick == 20

    # 2. 빈 프로필 전달 시 기본값 (w60_e40_t20, 20)
    combo_id_empty, anchor_tick_empty = feature_mart._extract_profile_feature_params({})
    assert combo_id_empty == "w60_e40_t20"
    assert anchor_tick_empty == 20

    # 3. 커스텀 프로필 (GRID_TICK_MINUTES fallback)
    custom_profile = {
        "ROLLING_WINDOW_MINUTES": 90,
        "ROLLING_EMBARGO_MINUTES": 45,
        "ROLLING_TICK_MINUTES": 15,
        "GRID_TICK_MINUTES": 15,
    }
    combo_id_custom, anchor_tick_custom = feature_mart._extract_profile_feature_params(custom_profile)
    assert combo_id_custom == "w90_e45_t15"
    assert anchor_tick_custom == 15


def test_extract_profile_feature_params_prioritizes_train_anchor_over_peak_anchor() -> None:
    """TRAIN_ANCHOR_TICK_MINUTES와 PEAK_ANCHOR_TICK_MINUTES가 다를 때 TRAIN이 우선하여 anchor_tick이 결정되는지 검증한다."""
    hybrid_profile = {
        "ROLLING_WINDOW_MINUTES": 60,
        "ROLLING_EMBARGO_MINUTES": 40,
        "ROLLING_TICK_MINUTES": 20,
        "TRAIN_ANCHOR_TICK_MINUTES": 60,  # 실제 산출물 디렉터리: training_anchor_a60
        "PEAK_ANCHOR_TICK_MINUTES": 20,   # adaptive 샘플링용 설정
    }
    combo_id, anchor_tick = feature_mart._extract_profile_feature_params(hybrid_profile)
    assert combo_id == "w60_e40_t20"
    assert anchor_tick == 60


def test_is_feature_mart_fresh_with_uppercase_profile_and_dedicated_watermark(monkeypatch) -> None:
    """대문자 프로필 및 모델 전용 워터마크 경로에서 신선도 검사가 정상 동작하는지 검증한다."""
    from datetime import UTC, datetime

    now_iso = datetime.now(UTC).isoformat()
    mock_s3 = {
        "profiles/default.json": {
            "ROLLING_WINDOW_MINUTES": 60,
            "ROLLING_EMBARGO_MINUTES": 40,
            "ROLLING_TICK_MINUTES": 20,
            "TRAIN_ANCHOR_TICK_MINUTES": 20,
        },
        "processed/features/w60_e40_t20/_watermark.json": {
            "max_hour_ts": "2026-08-28T00:00:00",
            "updated_at": now_iso,
        },
        "processed/features/w60_e40_t20/training_anchor_a20/_multi_horizon_rental_watermark.json": {
            "max_hour_ts": "2026-08-28T00:00:00",
            "updated_at": now_iso,
        },
    }

    monkeypatch.setattr(feature_mart, "read_s3_json", lambda key: mock_s3.get(key))

    # 최신 워터마크 -> True
    assert feature_mart.is_feature_mart_fresh("default", "rental") is True
    # return 전용 워터마크는 없음 -> False
    assert feature_mart.is_feature_mart_fresh("default", "return") is False


def test_is_feature_mart_fresh_when_train_and_peak_anchors_differ(monkeypatch) -> None:
    """TRAIN_ANCHOR(60)와 PEAK_ANCHOR(20)가 다를 때 training_anchor_a60 워터마크를 정확히 조회하는지 검증한다."""
    from datetime import UTC, datetime

    now_iso = datetime.now(UTC).isoformat()
    mock_s3 = {
        "profiles/sparse.json": {
            "ROLLING_WINDOW_MINUTES": 60,
            "ROLLING_EMBARGO_MINUTES": 40,
            "ROLLING_TICK_MINUTES": 20,
            "TRAIN_ANCHOR_TICK_MINUTES": 60,
            "PEAK_ANCHOR_TICK_MINUTES": 20,
        },
        "processed/features/w60_e40_t20/_watermark.json": {
            "max_hour_ts": "2026-08-28T00:00:00",
            "updated_at": now_iso,
        },
        "processed/features/w60_e40_t20/training_anchor_a60/_multi_horizon_rental_watermark.json": {
            "max_hour_ts": "2026-08-28T00:00:00",
            "updated_at": now_iso,
        },
    }

    monkeypatch.setattr(feature_mart, "read_s3_json", lambda key: mock_s3.get(key))

    # training_anchor_a60 경로를 올바르게 찾아 True 반환
    assert feature_mart.is_feature_mart_fresh("sparse", "rental") is True


