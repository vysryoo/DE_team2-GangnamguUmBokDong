"""core.s3의 S3 왕복(read/write)을 moto로 검증한다."""

import io

import pandas as pd
import pyarrow as pa
import pytest
from core import s3


def test_get_object_bytes_missing_key_returns_none():
    assert s3.get_object_bytes("no/such/key.json") is None


def test_put_then_get_object_bytes_round_trip():
    s3.put_object_bytes("some/key.bin", b"hello world")
    assert s3.get_object_bytes("some/key.bin") == b"hello world"


def test_put_then_get_object_metadata_without_reading_body():
    s3.put_object_bytes(
        "some/metadata.bin",
        b"payload",
        metadata={"source_window_start": "2026-08-20T10:05:00+09:00"},
    )

    assert s3.get_object_metadata("some/metadata.bin") == {
        "source_window_start": "2026-08-20T10:05:00+09:00"
    }
    assert s3.get_object_metadata("some/missing.bin") is None


def test_read_parquet_missing_key_returns_none():
    assert s3.read_parquet("no/such/table.parquet") is None


def test_write_then_read_parquet_round_trip():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet")

    pd.testing.assert_frame_equal(result, df)


def test_read_parquet_with_columns_filter():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [1.5, 2.5]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet", columns=["a", "c"])

    assert list(result.columns) == ["a", "c"]


def test_read_parquet_reads_spark_style_multi_part_directory():
    """Spark의 df.write.parquet(key)는 key 자체가 아니라 key를 prefix로 삼아
    part-00000-....parquet 여러 개 + _SUCCESS 마커를 쓴다 — read_parquet()가
    정확히 그 key의 단일 GET이 실패하면 prefix로 보고 파트 파일들을 찾아
    이어붙이는지 확인한다."""
    part1 = pd.DataFrame({"a": [1, 2]})
    part2 = pd.DataFrame({"a": [3, 4]})
    s3.put_object_bytes("out/table.parquet/_SUCCESS", b"")
    s3.write_parquet(part1, "out/table.parquet/part-00000.parquet")
    s3.write_parquet(part2, "out/table.parquet/part-00001.parquet")

    result = s3.read_parquet("out/table.parquet")

    assert sorted(result["a"].tolist()) == [1, 2, 3, 4]


def test_capture_object_reads_includes_parallel_multipart_bytes():
    """ContextVar를 worker로 전파해 실제 part bytes를 같은 capture에 모은다."""
    s3.write_parquet(pd.DataFrame({"a": [1]}), "captured/part-00000.parquet")
    s3.write_parquet(pd.DataFrame({"a": [2]}), "captured/part-00001.parquet")

    with s3.capture_object_reads() as capture:
        result = s3.read_parquet("captured")

    assert sorted(result["a"].tolist()) == [1, 2]
    assert [item.key for item in capture.objects] == [
        "captured/part-00000.parquet",
        "captured/part-00001.parquet",
    ]
    assert all(item.payload for item in capture.objects)


def test_capture_object_reads_rejects_same_key_drift(monkeypatch):
    """한 run 안에서 같은 mutable source key가 바뀌면 조용히 섞지 않는다."""
    payloads = iter((b"before", b"after"))

    class _Client:
        def get_object(self, **_kwargs):
            return {"Body": io.BytesIO(next(payloads))}

    monkeypatch.setattr(s3, "_client", lambda _timeout=None: _Client())

    with s3.capture_object_reads():
        assert s3.get_object_bytes("mutable.bin") == b"before"
        with pytest.raises(s3.S3InputDriftError, match="run 중 변경"):
            s3.get_object_bytes("mutable.bin")


def test_read_parquet_safely_promotes_compatible_multi_part_schemas():
    """증분 재생성 전후 part의 숫자 폭이 달라도 값 손실 없는 공통 타입이면 읽는다."""
    s3.write_parquet(pa.table({"capacity": pa.array([10], type=pa.int16())}), "mixed/part-00000.parquet")
    s3.write_parquet(pa.table({"capacity": pa.array([20.5], type=pa.float32())}), "mixed/part-00001.parquet")

    result = s3.read_parquet("mixed")

    assert result["capacity"].dtype.name == "float32"
    assert result["capacity"].tolist() == [10.0, 20.5]


def test_read_parquet_projects_added_column_and_fills_old_part_with_typed_null():
    """요청 컬럼이 옛 part에 없어도 나머지 part에서 타입을 얻어 NULL로 보충한다."""
    old = pa.table({"station_no": pa.array([1], type=pa.int16())})
    new = pa.table({
        "station_no": pa.array([2], type=pa.int16()),
        "new_feature": pa.array([1.5], type=pa.float32()),
    })
    s3.write_parquet(old, "additive/part-00000.parquet")
    s3.write_parquet(new, "additive/part-00001.parquet")

    result = s3.read_parquet("additive", columns=["new_feature", "station_no"])

    assert list(result.columns) == ["new_feature", "station_no"]
    assert result["new_feature"].dtype.name == "float32"
    assert pd.isna(result.loc[0, "new_feature"])
    assert result.loc[1, "new_feature"] == 1.5


def test_read_parquet_rejects_requested_column_missing_from_every_part():
    """projection 호환 처리가 모든 part에 없는 오타 컬럼을 빈 결과로 숨기지 않는다."""
    s3.write_parquet(pa.table({"station_no": [1]}), "missing/part-00000.parquet")
    s3.write_parquet(pa.table({"station_no": [2]}), "missing/part-00001.parquet")

    with pytest.raises(s3.ParquetSchemaMismatchError, match="어떤 Parquet part에도 없습니다"):
        s3.read_parquet("missing", columns=["unknown_feature"])


def test_read_parquet_rejects_incompatible_multi_part_schemas():
    """서로 다른 논리 타입을 문자열 등으로 임의 변환해 조용히 섞지 않는다."""
    s3.write_parquet(pa.table({"station_no": pa.array([1], type=pa.int16())}), "bad/part-00000.parquet")
    s3.write_parquet(pa.table({"station_no": pa.array(["ST-1"], type=pa.string())}), "bad/part-00001.parquet")

    with pytest.raises(s3.ParquetSchemaMismatchError, match="안전한 공통 타입"):
        s3.read_parquet("bad")


def test_concat_compatible_tables_rejects_precision_loss():
    """공통 타입이 존재해도 실제 정숫값을 float로 정확히 표현할 수 없으면 실패한다."""
    too_large_for_float64 = 2**53 + 1
    integer = pa.table({"value": pa.array([too_large_for_float64], type=pa.int64())})
    floating = pa.table({"value": pa.array([1.5], type=pa.float64())})

    with pytest.raises(s3.ParquetSchemaMismatchError, match="무손실 변환"):
        s3.concat_compatible_tables([integer, floating])


def test_concat_compatible_tables_unions_columns_and_normalizes_order():
    """컬럼 추가와 순서 변화는 첫 등장 순서의 union schema 및 typed NULL로 맞춘다."""
    old = pa.table({"station_no": [1], "capacity": [10]})
    new = pa.table({"capacity": [20], "minute": [5], "station_no": [2]})

    result = s3.concat_compatible_tables([old, new])

    assert result.column_names == ["station_no", "capacity", "minute"]
    assert result["station_no"].to_pylist() == [1, 2]
    assert result["minute"].to_pylist() == [None, 5]


def test_concat_compatible_tables_promotes_null_field_to_declared_type():
    """전량 결측 part의 Arrow null 타입은 실제 값이 있는 part의 선언 타입으로 맞춘다."""
    null_only = pa.table({"precip": pa.array([None], type=pa.null())})
    typed = pa.table({"precip": pa.array([1.5], type=pa.float32())})

    result = s3.concat_compatible_tables([null_only, typed])

    assert result.schema.field("precip").type == pa.float32()
    assert result["precip"].to_pylist() == [None, 1.5]


def test_concat_compatible_tables_exact_schema_uses_fast_path(monkeypatch):
    """정상 대용량 경로는 schema unify/cast 없이 기존 zero-copy concat을 유지한다."""
    first = pa.table({"station_no": pa.array([1], type=pa.int16())})
    second = pa.table({"station_no": pa.array([2], type=pa.int16())})

    def _unexpected_unify(*_args, **_kwargs):
        raise AssertionError("동일 스키마에서 unify_schemas를 호출하면 안 됨")

    monkeypatch.setattr(s3.pa, "unify_schemas", _unexpected_unify)

    result = s3.concat_compatible_tables([first, second])

    assert result["station_no"].to_pylist() == [1, 2]


def test_read_parquet_as_pandas_false_returns_pyarrow_table():
    df = pd.DataFrame({"a": [1, 2]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet", as_pandas=False)

    assert result.to_pandas()["a"].tolist() == [1, 2]


def test_read_parquet_date_range_reads_only_requested_partitions():
    """Spark의 partitionBy("date") 출력(key/date=YYYY-MM-DD/part-*.parquet, 파일
    내용엔 date 컬럼이 없음 — Hive 컨벤션)에서 date_range로 지정한 날짜만 나열/
    다운로드하고, 범위 밖 파티션은 아예 안 건드리는지 확인한다."""
    # Spark처럼 파일 내용엔 "date" 컬럼이 없다 — 파티션 폴더명에만 있음.
    s3.write_parquet(pd.DataFrame({"a": [1, 2]}), "mh/date=2025-11-01/part-00000.parquet")
    s3.write_parquet(pd.DataFrame({"a": [3]}), "mh/date=2025-11-02/part-00000.parquet")
    # 범위 밖 — 읽히면 안 된다.
    s3.write_parquet(pd.DataFrame({"a": [999]}), "mh/date=2025-12-01/part-00000.parquet")

    result = s3.read_parquet("mh", date_range=("2025-11-01", "2025-11-02"))

    assert sorted(result["a"].tolist()) == [1, 2, 3]
    assert sorted(result["date"].unique().tolist()) == ["2025-11-01", "2025-11-02"]


def test_read_parquet_date_range_with_columns_keeps_date_and_requested_order():
    s3.write_parquet(pd.DataFrame({"a": [1], "b": [10]}), "mh2/date=2025-11-01/part-00000.parquet")

    result = s3.read_parquet("mh2", columns=["date", "a"], date_range=("2025-11-01", "2025-11-01"))

    assert list(result.columns) == ["date", "a"]
    assert result["date"].tolist() == ["2025-11-01"]


def test_read_parquet_date_range_without_date_in_columns_omits_it_and_skips_building_it(monkeypatch):
    """columns에 date가 없으면 결과에서 빠지고, 버릴 date 문자열 배열도 만들지 않는다.

    `pa.array`는 pyarrow 내부에서도 불리므로 파티션 날짜만 반복한 호출만 센다.
    """
    s3.write_parquet(pd.DataFrame({"a": [1], "b": [10]}), "mh2b/date=2025-11-01/part-00000.parquet")

    date_array_calls = []
    real_pa_array = s3.pa.array

    def _spy(*args, **kwargs):
        if args and list(args[0]) == ["2025-11-01"]:
            date_array_calls.append(args[0])
        return real_pa_array(*args, **kwargs)

    monkeypatch.setattr(s3.pa, "array", _spy)

    result = s3.read_parquet("mh2b", columns=["a"], date_range=("2025-11-01", "2025-11-01"))

    assert list(result.columns) == ["a"]
    assert date_array_calls == []  # "date" 배열을 만들려고 호출된 적이 없어야 함


def test_read_parquet_date_range_missing_partitions_returns_none():
    assert s3.read_parquet("mh3", date_range=("2025-01-01", "2025-01-02")) is None


def test_read_parquet_date_range_as_pandas_false_returns_pyarrow_table():
    s3.write_parquet(pd.DataFrame({"a": [1]}), "mh4/date=2025-11-01/part-00000.parquet")

    result = s3.read_parquet("mh4", as_pandas=False, date_range=("2025-11-01", "2025-11-01"))

    assert result.to_pandas()["a"].tolist() == [1]


def test_read_parquet_dates_safely_promotes_partition_schema_drift():
    """date= 파티션을 가로질러도 공용 안전 결합 규칙과 date 복원이 함께 적용된다."""
    first = pa.table({"horizon": pa.array([1], type=pa.int8())})
    second = pa.table({"horizon": pa.array([12], type=pa.int16())})
    s3.write_parquet(first, "mh4b/date=2025-11-01/part-00000.parquet")
    s3.write_parquet(second, "mh4b/date=2025-11-02/part-00000.parquet")

    result = s3.read_parquet("mh4b", dates=["2025-11-01", "2025-11-02"])

    assert result["horizon"].dtype.name == "int16"
    assert result["horizon"].tolist() == [1, 12]
    assert result["date"].tolist() == ["2025-11-01", "2025-11-02"]


def test_read_parquet_dates_reads_only_the_listed_discontinuous_dates():
    """`dates=`는 `date_range`와 달리 연속 구간이 아니라 임의의 날짜 목록(예: 짝수날만)을
    받는다 — 목록에 없는, 심지어 그 사이에 낀 날짜도 안 읽는지 확인한다."""
    s3.write_parquet(pd.DataFrame({"a": [1]}), "mh5/date=2025-11-02/part-00000.parquet")
    s3.write_parquet(pd.DataFrame({"a": [2]}), "mh5/date=2025-11-03/part-00000.parquet")  # 목록에 없음 — 안 읽혀야 함
    s3.write_parquet(pd.DataFrame({"a": [3]}), "mh5/date=2025-11-04/part-00000.parquet")

    result = s3.read_parquet("mh5", dates=["2025-11-02", "2025-11-04"])

    assert sorted(result["a"].tolist()) == [1, 3]
    assert sorted(result["date"].unique().tolist()) == ["2025-11-02", "2025-11-04"]


def test_read_parquet_rejects_date_range_and_dates_together():
    with pytest.raises(ValueError, match="동시에 지정할 수 없습니다"):
        s3.read_parquet("mh6", date_range=("2025-11-01", "2025-11-02"), dates=["2025-11-01"])


def test_read_parquet_filters_applies_to_plain_object():
    """filters=는 date=/dates= 파티션 없이 파일 하나짜리 객체에도 적용돼야 한다."""
    df = pd.DataFrame({"horizon": list(range(1, 13)), "value": range(12)})
    s3.write_parquet(df, "plain/table.parquet")

    result = s3.read_parquet("plain/table.parquet", filters=[("horizon", "<=", 6)])

    assert sorted(result["horizon"].unique().tolist()) == [1, 2, 3, 4, 5, 6]


def test_read_parquet_filters_applies_within_each_date_partition():
    """date= 파티션 안에 여러 horizon이 섞여 있을 때, dates=로 못 줄이는 걸 filters=로 줄인다."""
    df = pd.DataFrame({"horizon": list(range(1, 13)), "value": range(12)})
    s3.write_parquet(df, "mh7/date=2025-11-01/part-00000.parquet")

    result = s3.read_parquet("mh7", dates=["2025-11-01"], filters=[("horizon", "<=", 6)])

    assert len(result) == 6
    assert sorted(result["horizon"].unique().tolist()) == [1, 2, 3, 4, 5, 6]


def test_read_parquet_dates_reports_progress_via_on_complete():
    """대량 학습 테이블 로드처럼 오래 걸리는 다중 파일 읽기의 진행 상황을 로깅하려면
    on_complete(완료 개수, 전체 개수)가 파일마다(날짜 경계 무관) 한 번씩 불려야 한다."""
    s3.write_parquet(pd.DataFrame({"a": [1]}), "mh8/date=2025-11-01/part-00000.parquet")
    s3.write_parquet(pd.DataFrame({"a": [2]}), "mh8/date=2025-11-02/part-00000.parquet")

    calls = []
    result = s3.read_parquet(
        "mh8", dates=["2025-11-01", "2025-11-02"], on_complete=lambda done, total: calls.append((done, total))
    )

    assert sorted(result["a"].tolist()) == [1, 2]
    assert len(calls) == 2
    assert all(total == 2 for _done, total in calls)
    assert sorted(done for done, _total in calls) == [1, 2]


def test_read_json_missing_key_returns_none():
    assert s3.read_json("no/such/file.json") is None


def test_write_then_read_json_round_trip():
    data = {"holidays_2025": ["2025-01-01", "2025-12-25"]}
    s3.write_json("some/file.json", data)

    assert s3.read_json("some/file.json") == data


def test_list_keys_returns_only_matching_prefix():
    s3.put_object_bytes("silver/station/a.parquet", b"1")
    s3.put_object_bytes("silver/station/b.parquet", b"2")
    s3.put_object_bytes("silver/rental/c.parquet", b"3")

    keys = s3.list_keys("silver/station/")

    assert sorted(keys) == ["silver/station/a.parquet", "silver/station/b.parquet"]


def test_list_keys_empty_prefix_returns_empty_list():
    assert s3.list_keys("nothing/here/") == []


def test_object_exists():
    assert s3.object_exists("some/key.bin") is False
    s3.put_object_bytes("some/key.bin", b"x")
    assert s3.object_exists("some/key.bin") is True


def test_delete_object():
    s3.put_object_bytes("some/key.bin", b"x")
    s3.delete_object("some/key.bin")
    assert s3.get_object_bytes("some/key.bin") is None


@pytest.mark.parametrize("n", [0, 3])
def test_delete_objects(n):
    keys = [f"batch/{i}.bin" for i in range(n)]
    for key in keys:
        s3.put_object_bytes(key, b"x")

    s3.delete_objects(keys)

    assert all(s3.get_object_bytes(key) is None for key in keys)


class _CapturingBoto3:
    """`boto3.Session().client()`에 넘어간 설정과 생성 횟수를 잡는 스텁."""

    def __init__(self):
        """Session namespace를 겸하는 빈 capture를 만든다."""
        self.session = self
        self.session_kwargs: dict | None = None
        self.kwargs: dict | None = None
        self.calls = 0

    def Session(self, **kwargs):
        """Session 설정을 기록하고 client factory 역할을 할 자신을 반환한다."""
        self.session_kwargs = kwargs
        return self

    def client(self, service_name, **kwargs):
        """Client 설정과 호출 수를 기록하고 새 identity 객체를 반환한다."""
        self.calls += 1
        self.kwargs = kwargs
        return object()


def test_client_is_reused_inside_process(monkeypatch):
    """같은 endpoint와 timeout의 S3 client는 process 안에서 한 번만 생성한다."""
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    first = s3._client()
    second = s3._client()

    assert first is second
    assert fake.calls == 1


def test_client_cache_separates_timeout_policy(monkeypatch):
    """기본 재시도와 짧은 fallback timeout은 서로 다른 client를 쓴다."""
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    default = s3._client()
    short = s3._client(0.5)

    assert default is not short
    assert fake.calls == 2


@pytest.mark.parametrize(
    ("name", "first", "second"),
    [
        ("AWS_DEFAULT_REGION", "us-east-1", "ap-northeast-2"),
        ("AWS_PROFILE", "first-profile", "second-profile"),
        ("AWS_ACCESS_KEY_ID", "first-key", "second-key"),
        ("AWS_SECRET_ACCESS_KEY", "first-secret", "second-secret"),
        ("AWS_SESSION_TOKEN", "first-token", "second-token"),
    ],
)
def test_client_cache_separates_effective_environment(
    monkeypatch, name, first, second
):
    """Region/profile/credential 설정이 바뀌면 기존 client를 재사용하지 않는다."""
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv(name, first)

    original = s3._client()
    monkeypatch.setenv(name, second)
    changed = s3._client()

    assert original is not changed
    assert fake.calls == 2


def test_client_uses_worker_sized_connection_pool(monkeypatch):
    """16개 read worker가 botocore 기본 pool 10개에 막히지 않게 맞춘다."""
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    s3._client()

    assert fake.kwargs["config"].max_pool_connections == 16


def test_client_applies_region_and_profile_to_session(monkeypatch):
    """명시한 profile/region은 새 boto3 Session의 실제 설정으로 전달한다."""
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_PROFILE", "serving-profile")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")

    s3._client()

    assert fake.session_kwargs == {
        "profile_name": "serving-profile",
        "region_name": "ap-northeast-2",
    }


def test_clear_client_cache_closes_every_client(monkeypatch):
    """Cache clear가 분리된 timeout client들의 HTTP pool을 모두 닫는다."""

    class _ClosableClient:
        """close 호출 여부만 기록하는 가짜 S3 client다."""

        def __init__(self):
            """열린 상태의 client를 만든다."""
            self.close_calls = 0

        def close(self):
            """Connection pool close 호출을 기록한다."""
            self.close_calls += 1

    class _ClosableBoto3(_CapturingBoto3):
        """호출마다 닫을 수 있는 client를 생성하는 capture다."""

        def __init__(self):
            """생성 client 목록을 함께 초기화한다."""
            super().__init__()
            self.clients = []

        def client(self, service_name, **kwargs):
            """새 closable client와 설정을 기록한다."""
            self.calls += 1
            self.kwargs = kwargs
            client = _ClosableClient()
            self.clients.append(client)
            return client

    fake = _ClosableBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    s3._client()
    s3._client(0.5)

    s3._clear_client_cache()

    assert [client.close_calls for client in fake.clients] == [1, 1]


@pytest.mark.parametrize("endpoint", [None, ""])
def test_client_without_endpoint_delegates_credentials_to_boto3_chain(monkeypatch, endpoint):
    # 환경 credential이 없을 때 자격증명을 명시적으로 넘기면 boto3가 EC2 instance
    # profile / EMR 실행 역할을 조회하지 않는다 — 운영 경로에는 인자가 없어야 한다.
    # 운영 compose는 S3_ENDPOINT_URL을 빈 문자열로 두므로 ""도 "없음"으로 취급해야 한다.
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    if endpoint is None:
        monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    else:
        monkeypatch.setenv("S3_ENDPOINT_URL", endpoint)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)

    s3._client()

    assert "aws_access_key_id" not in fake.kwargs
    assert "aws_secret_access_key" not in fake.kwargs
    assert "endpoint_url" not in fake.kwargs


def test_client_with_endpoint_passes_explicit_credentials(monkeypatch):
    # 로컬 MinIO 경로는 기존 동작 그대로 유지되어야 한다.
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "local-token")

    s3._client()

    assert fake.kwargs["endpoint_url"] == "http://minio:9000"
    assert fake.kwargs["aws_access_key_id"] == "local-key"
    assert fake.kwargs["aws_secret_access_key"] == "local-secret"
    assert fake.kwargs["aws_session_token"] == "local-token"


def test_client_with_endpoint_falls_back_to_minioadmin(monkeypatch):
    # .env를 셸에 로드하지 않고 모듈을 직접 실행하던 개발 경로를 깨뜨리지 않는다.
    fake = _CapturingBoto3()
    monkeypatch.setattr(s3, "boto3", fake)
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    s3._client()

    assert fake.kwargs["aws_access_key_id"] == "minioadmin"
    assert fake.kwargs["aws_secret_access_key"] == "minioadmin"
