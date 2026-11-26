from __future__ import annotations

import importlib
import io
import json
import sys
import tempfile
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

import pytest

# snowflake-snowpark-python is an optional dependency and not installed in the dev environment,
# so the snowflake modules used at runtime are replaced by fakes.


class FakeSnowflakeError(Exception):
    pass


class FakeSnowparkSQLException(Exception):  # noqa: N818
    pass


class FakeSnowflakeS3RestClient:
    def _get_bucket_accelerate_config(self, bucket_name):  # noqa: ARG002
        return True

    def transfer_accelerate_config(self, use_accelerate_endpoint=None):  # noqa: ARG002
        return True


def _fake_modules() -> dict[str, ModuleType]:
    snowflake = ModuleType("snowflake")
    connector = ModuleType("snowflake.connector")
    errors = ModuleType("snowflake.connector.errors")
    errors.Error = FakeSnowflakeError
    s3_storage_client = ModuleType("snowflake.connector.s3_storage_client")
    s3_storage_client.SnowflakeS3RestClient = FakeSnowflakeS3RestClient
    snowpark = ModuleType("snowflake.snowpark")
    snowpark_exceptions = ModuleType("snowflake.snowpark.exceptions")
    snowpark_exceptions.SnowparkSQLException = FakeSnowparkSQLException
    container_ops = ModuleType("transforms_container_ops_python")
    container_ops.io = mock.MagicMock()
    return {
        "snowflake": snowflake,
        "snowflake.connector": connector,
        "snowflake.connector.errors": errors,
        "snowflake.connector.s3_storage_client": s3_storage_client,
        "snowflake.snowpark": snowpark,
        "snowflake.snowpark.exceptions": snowpark_exceptions,
        "transforms_container_ops_python": container_ops,
    }


@pytest.fixture(scope="module")
def fsi_modules():
    with mock.patch.dict(sys.modules, _fake_modules()):
        fsi_prefixes = ("foundry_dev_tools.helpers.fast_snowflake_ingestion", "foundry_dev_tools._optional.snowflake")
        for name in [m for m in sys.modules if m.startswith(fsi_prefixes)]:
            del sys.modules[name]
        pkg = importlib.import_module("foundry_dev_tools.helpers.fast_snowflake_ingestion")
        yield SimpleNamespace(
            pkg=pkg,
            impl=sys.modules["foundry_dev_tools.helpers.fast_snowflake_ingestion._impl"],
            utils=sys.modules["foundry_dev_tools.helpers.fast_snowflake_ingestion.utils"],
            container_ops=sys.modules["transforms_container_ops_python"],
        )


@pytest.fixture(autouse=True)
def _reset_s3_client():
    original = dict(vars(FakeSnowflakeS3RestClient))
    yield
    for attr in ("_get_bucket_accelerate_config", "transfer_accelerate_config"):
        setattr(FakeSnowflakeS3RestClient, attr, original[attr])


@pytest.fixture()
def job_config(fsi_modules, mocker):
    cfg = mock.MagicMock()
    cfg.get_job_config_yaml.return_value = {
        "branch": "master",
        "job-rid": "ri.foundry.main.job.1",
        "job-spec": {
            "sourceProvenance": {"details": {"code": {"repositoryRid": "ri.stemma.main.repository.1"}}},
            "computationParameters": {
                "transforms": {
                    "containerizedApplicationConfiguration": {
                        "standalone": {
                            "jobConfiguration": {
                                "inputs": {"in": "ri.foundry.main.dataset.in"},
                                "outputs": {"out": "ri.foundry.main.dataset.out"},
                            }
                        }
                    }
                }
            },
        },
    }
    mocker.patch.object(fsi_modules.utils, "load_job_config", return_value=cfg)
    mocker.patch.object(
        fsi_modules.utils,
        "get_output_foundry_path_components",
        return_value=("ri.foundry.main.dataset.out", "ri.foundry.main.transaction.1", "master", ""),
    )
    return cfg


def session_mock() -> mock.MagicMock:
    session = mock.MagicMock()
    session.get_session_stage.return_value = "@SESSION_STAGE"
    session.get_current_database.return_value = "CUR_DB"
    session.get_current_schema.return_value = "CUR_SCHEMA"
    return session


def test_session_stage_and_query_tag(fsi_modules, job_config):
    session = session_mock()
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(
        session, mock.MagicMock(), custom_tags={"team": "it's"}, enable_streaming=False
    )

    tags = json.loads(session.query_tag)
    assert tags["team"] == "it's"
    assert tags["job_rid"] == "ri.foundry.main.job.1"
    assert tags["inputs"] == {"in": "ri.foundry.main.dataset.in"}
    # query tag is set via the escaping session property, not via raw SQL
    session.sql.assert_not_called()

    info = fsi.get_database_and_staging_info()
    assert info["remote_file_path"].startswith("@SESSION_STAGE/")
    assert info["use_shared_db"] is False
    assert info["is_using_fallback"] is False


def test_query_tag_truncated(fsi_modules, job_config, caplog):
    session = session_mock()
    long_value = "x" * 3000
    fsi_modules.pkg.FastSnowflakeIngestion(session, mock.MagicMock(), custom_tags={"long": long_value})

    assert len(session.query_tag) == fsi_modules.impl.QUERY_TAG_MAX_LENGTH
    assert session.query_tag.startswith('{"')
    assert long_value in caplog.text


def test_shared_db_stage(fsi_modules, job_config):
    session = session_mock()
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(
        session, mock.MagicMock(), use_shared_db=True, db_name="DB", schema_name="SCHEMA"
    )

    session.use_database.assert_called_once_with("DB")
    session.use_schema.assert_called_once_with("SCHEMA")
    info = fsi.get_database_and_staging_info()
    assert info["fully_qualified_stage_name"].startswith("DB.SCHEMA.TEMP_EXPORT_")
    assert info["temporary_stage_name"] == info["fully_qualified_stage_name"]
    assert info["remote_file_path"].startswith("@DB.SCHEMA.TEMP_EXPORT_")
    assert info["is_streaming"] is True
    session.sql.assert_called_once_with(
        f"CREATE OR REPLACE TEMPORARY STAGE {info['fully_qualified_stage_name']} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
    )


def test_shared_db_stage_without_streaming(fsi_modules, job_config):
    session = session_mock()
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(
        session, mock.MagicMock(), use_shared_db=True, db_name="DB", schema_name="SCHEMA", enable_streaming=False
    )

    info = fsi.get_database_and_staging_info()
    assert info["is_streaming"] is False
    session.sql.assert_called_once_with(f"CREATE OR REPLACE TEMPORARY STAGE {info['fully_qualified_stage_name']};")


def test_streaming_stage_in_current_schema(fsi_modules, job_config):
    session = session_mock()
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, mock.MagicMock())

    info = fsi.get_database_and_staging_info()
    assert info["is_streaming"] is True
    assert info["fully_qualified_stage_name"].startswith("CUR_DB.CUR_SCHEMA.TEMP_EXPORT_")
    assert "SNOWFLAKE_SSE" in session.sql.call_args.args[0]
    session.get_session_stage.assert_not_called()


def test_streaming_stage_refused_uses_session_stage(fsi_modules, job_config):
    session = session_mock()
    session.sql.return_value.collect.side_effect = FakeSnowparkSQLException("Insufficient privileges")
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, mock.MagicMock())

    info = fsi.get_database_and_staging_info()
    assert info["is_streaming"] is False
    assert info["fully_qualified_stage_name"] is None
    assert info["remote_file_path"].startswith("@SESSION_STAGE/")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"use_shared_db": True, "db_name": "DB"},
        {"use_shared_db": True, "schema_name": "SCHEMA"},
        {"fallback_db_name": "DB"},
        {"fallback_schema_name": "SCHEMA"},
    ],
)
def test_invalid_arguments(fsi_modules, job_config, kwargs):
    with pytest.raises(ValueError):  # noqa: PT011
        fsi_modules.pkg.FastSnowflakeIngestion(session_mock(), mock.MagicMock(), **kwargs)


def test_no_fallback_configured_raises_original_error(fsi_modules, job_config):
    session = session_mock()
    session.get_session_stage.side_effect = FakeSnowflakeError("no stage")
    with pytest.raises(FakeSnowflakeError, match="no stage"):
        fsi_modules.pkg.FastSnowflakeIngestion(session, mock.MagicMock(), enable_streaming=False)


@pytest.mark.parametrize("use_shared_db", [False, True])
def test_fallback_stage(fsi_modules, job_config, use_shared_db):
    session = session_mock()
    session.get_session_stage.side_effect = FakeSnowflakeError("no stage")
    # a failing CREATE STAGE raises SnowparkSQLException from collect(), not a connector Error
    session.sql.return_value.collect.side_effect = (
        [FakeSnowparkSQLException("no privilege"), mock.MagicMock()] if use_shared_db else None
    )
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(
        session,
        mock.MagicMock(),
        use_shared_db=use_shared_db,
        db_name="DB" if use_shared_db else None,
        schema_name="SCHEMA" if use_shared_db else None,
        fallback_db_name="FALLBACK_DB",
        fallback_schema_name="STAGING",
        enable_streaming=False,
    )

    info = fsi.get_database_and_staging_info()
    assert info["is_using_fallback"] is True
    assert info["db_name"] == "FALLBACK_DB"
    assert info["schema_name"] == "STAGING"
    assert info["fully_qualified_stage_name"].startswith("FALLBACK_DB.STAGING.TEMP_EXPORT_")


def test_fallback_disabled(fsi_modules, job_config):
    session = session_mock()
    session.get_session_stage.side_effect = FakeSnowflakeError("no stage")
    with pytest.raises(FakeSnowflakeError):
        fsi_modules.pkg.FastSnowflakeIngestion(
            session,
            mock.MagicMock(),
            fallback_db_name="FALLBACK_DB",
            fallback_schema_name="STAGING",
            enable_automatic_stage_fallback=False,
            enable_streaming=False,
        )


def test_s3_monkeypatch(fsi_modules, job_config):
    fsi_modules.pkg.FastSnowflakeIngestion(session_mock(), mock.MagicMock(), enable_s3_monkeypatch=False)
    assert FakeSnowflakeS3RestClient()._get_bucket_accelerate_config("bucket") is True

    fsi_modules.pkg.FastSnowflakeIngestion(session_mock(), mock.MagicMock())
    client = FakeSnowflakeS3RestClient()
    client.s3location = SimpleNamespace(bucket_name="bucket")
    client.region_name = "eu-central-1"
    assert client._get_bucket_accelerate_config("bucket") is False
    assert client.transfer_accelerate_config() is False
    assert client.endpoint == "https://bucket.s3.eu-central-1.amazonaws.com"


class FakeFilesystem:
    def __init__(self, written: dict):
        self.written = written

    def open(self, path, mode):
        buf = io.BytesIO() if "b" in mode else io.StringIO()
        buf.close = lambda: None
        self.written[path] = buf
        return buf


def test_save_snowpark_dataframe_to_foundry_output(fsi_modules, job_config, mocker):
    written = {}
    output = mock.MagicMock()
    output.filesystem.side_effect = lambda: FakeFilesystem(written)
    session = session_mock()
    session.session_id = 1234

    def fake_get(stage_location, target_directory, parallel):
        for i in range(3):
            Path(target_directory, f"data_{i}.parquet").write_bytes(b"x" * i)
        return []

    session.file.get.side_effect = fake_get
    mocker.patch.object(
        fsi_modules.impl,
        "get_output_foundry_path_components",
        return_value=("ri.foundry.main.dataset.out", "ri.foundry.main.transaction.1", "master", ""),
    )
    commit = fsi_modules.container_ops.io.commit_dataset_upload
    commit.reset_mock()
    df = mock.MagicMock()

    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, output, enable_streaming=False)
    assert fsi.save_snowpark_dataframe_to_foundry_output(df, cpu_cores=4, column_descriptions={"a": "b"})

    df.write.copy_into_location.assert_called_once()
    assert df.write.copy_into_location.call_args.kwargs["MAX_FILE_SIZE"] == 200 * 1024 * 1024
    assert sorted(written) == [
        "_snowflake_session_metadata/1234.txt",
        "data_0.parquet",
        "data_1.parquet",
        "data_2.parquet",
    ]
    assert written["data_2.parquet"].getvalue() == b"xx"
    commit.assert_called_once_with("ri.foundry.main.dataset.out", "master", "ri.foundry.main.transaction.1", {"a": "b"})
    assert fsi._temp_dir is None


def test_temp_dir_removed_on_failure(fsi_modules, job_config):
    session = session_mock()
    created = []

    def fake_get(stage_location, target_directory, parallel):
        created.append(target_directory)
        msg = "download failed"
        raise FakeSnowflakeError(msg)

    session.file.get.side_effect = fake_get
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, mock.MagicMock(), enable_streaming=False)
    with pytest.raises(FakeSnowflakeError):
        fsi.save_snowpark_dataframe_to_foundry_output(mock.MagicMock(), save_session_id=False)
    assert not Path(created[0]).exists()
    assert fsi._temp_dir is None


PARQUET = {"a.parquet": b"PAR1aaaaPAR1", "b.parquet": b"PAR1bbbbbbPAR1"}


def streaming_session(files: dict[str, bytes], copy_result: dict[str, bytes] | None = None):
    """Session whose stage lists one more of ``files`` per LIST, and a COPY job done after the second poll."""
    session = session_mock()
    session.session_id = 1234
    copy_result = files if copy_result is None else copy_result
    lists = [dict(list(files.items())[: i + 1]) for i in range(len(files))]

    def sql(query):
        result = mock.MagicMock()
        if query.startswith("LIST"):
            listed = lists.pop(0) if len(lists) > 1 else lists[0]
            result.collect.return_value = [
                {"name": f"temp_export_x/{name}", "size": len(data), "md5": str(hash(data))}
                for name, data in listed.items()
            ]
        elif query.startswith("SELECT column1, GET_PRESIGNED_URL"):
            names = [part.strip("()'") for part in query.split("FROM VALUES ")[1].split(",")]
            result.collect.return_value = [(name, f"https://bucket/{name}?sig") for name in names]
        return result

    session.sql.side_effect = sql
    job = mock.MagicMock()
    job.is_done.side_effect = [False] * (len(files) - 1) + [True]
    job.result.return_value = [
        SimpleNamespace(FILE_NAME=name, FILE_SIZE=len(data), ROW_COUNT=1) for name, data in copy_result.items()
    ]
    df = mock.MagicMock()
    df.write.copy_into_location.return_value = job
    return session, df


def fake_requests_get(contents: dict[str, bytes]):
    def get(url, stream, timeout):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.raw = io.BytesIO(contents[url.rsplit("/", 1)[1].split("?")[0]])
        return response

    return get


def test_streaming_save(fsi_modules, job_config, mocker):
    written = {}
    output = mock.MagicMock()
    output.filesystem.side_effect = lambda: FakeFilesystem(written)
    session, df = streaming_session(PARQUET)
    mocker.patch.object(fsi_modules.impl.requests, "get", side_effect=fake_requests_get(PARQUET))
    mocker.patch.object(fsi_modules.impl.time, "sleep")
    mocker.patch.object(
        fsi_modules.impl,
        "get_output_foundry_path_components",
        return_value=("ri.foundry.main.dataset.out", "ri.foundry.main.transaction.1", "master", ""),
    )
    commit = fsi_modules.container_ops.io.commit_dataset_upload
    commit.reset_mock()

    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, output)
    assert fsi.save_snowpark_dataframe_to_foundry_output(df, cpu_cores=4)

    assert df.write.copy_into_location.call_args.kwargs["block"] is False
    assert {name: buf.getvalue() for name, buf in written.items() if name.endswith(".parquet")} == PARQUET
    commit.assert_called_once()
    session.file.get.assert_not_called()


@pytest.mark.parametrize(
    ("contents", "copy_result", "match"),
    [
        ({**PARQUET, "b.parquet": b"encrypted!!!!!"}, None, "Invalid stream for b.parquet"),
        (PARQUET, {**PARQUET, "c.parquet": b"PAR1PAR1"}, "do not match the COPY INTO result"),
    ],
)
def test_streaming_check_fails_before_commit(fsi_modules, job_config, mocker, contents, copy_result, match):
    output = mock.MagicMock()
    output.filesystem.side_effect = lambda: FakeFilesystem({})
    session, df = streaming_session(PARQUET, copy_result)
    mocker.patch.object(fsi_modules.impl.requests, "get", side_effect=fake_requests_get(contents))
    mocker.patch.object(fsi_modules.impl.time, "sleep")
    commit = fsi_modules.container_ops.io.commit_dataset_upload
    commit.reset_mock()

    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session, output)
    with pytest.raises(RuntimeError, match=match):
        fsi.save_snowpark_dataframe_to_foundry_output(df, save_session_id=False)
    commit.assert_not_called()


def test_get_files_before_download_raises(fsi_modules, job_config):
    fsi = fsi_modules.pkg.FastSnowflakeIngestion(session_mock(), mock.MagicMock())
    with pytest.raises(RuntimeError):
        fsi.get_files_from_temp_dir()


def test_upload_file_destination_path(fsi_modules, tmp_path):
    written = {}
    output = mock.MagicMock()
    output.filesystem.side_effect = lambda: FakeFilesystem(written)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "a.parquet").write_bytes(b"1")

    fsi_modules.pkg.FastSnowflakeIngestion.upload_file(output, tmp_path / "sub" / "a.parquet", base_dir=tmp_path)
    assert list(written) == ["sub/a.parquet"]

    # without base_dir the first directory below the system temp directory is dropped
    written.clear()
    temp_dir = Path(tempfile.mkdtemp())
    try:
        (temp_dir / "b.parquet").write_bytes(b"1")
        fsi_modules.pkg.FastSnowflakeIngestion.upload_file(output, temp_dir / "b.parquet")
    finally:
        (temp_dir / "b.parquet").unlink()
        temp_dir.rmdir()
    assert list(written) == ["b.parquet"]


def test_prepare_job_info(fsi_modules, job_config):
    info = fsi_modules.utils.prepare_job_info(mock.MagicMock(), branch="ignored", extra=1)
    assert info == {
        "output_dataset_rid": "ri.foundry.main.dataset.out",
        "output_transaction_rid": "ri.foundry.main.transaction.1",
        "branch": "master",
        "job_rid": "ri.foundry.main.job.1",
        "sourceProvenance_details_code": {"repositoryRid": "ri.stemma.main.repository.1"},
        "inputs": {"in": "ri.foundry.main.dataset.in"},
        "outputs": {"out": "ri.foundry.main.dataset.out"},
        "extra": 1,
    }


def test_prepare_job_info_logs_failing_fields(fsi_modules, job_config, mocker):
    job_config.get_job_config_yaml.return_value = {"branch": "master", "job-spec": None}
    warning = mocker.patch.object(fsi_modules.utils.logger, "warning")

    info = fsi_modules.utils.prepare_job_info(mock.MagicMock())

    assert info["branch"] == "master"
    assert "inputs" not in info
    assert [c.args[1] for c in warning.call_args_list] == ["sourceProvenance_details_code", "inputs", "outputs"]


def test_prepare_job_info_keeps_custom_tags_without_job_config(fsi_modules, mocker):
    mocker.patch.object(
        fsi_modules.utils, "get_output_foundry_path_components", side_effect=ModuleNotFoundError("no job")
    )

    assert fsi_modules.utils.prepare_job_info(mock.MagicMock(), team="data") == {"team": "data"}


def test_load_job_config_outside_foundry(fsi_modules):
    with pytest.raises(ModuleNotFoundError, match="transforms library is not installed"):
        fsi_modules.utils.load_job_config()


def test_path_classes(fsi_modules):
    assert str(fsi_modules.pkg.TransactionPath("ds", "tx")) == "datasets/ds/transactions/tx"
    assert str(fsi_modules.pkg.TransactionPath("ds", "tx", "a/b")) == "datasets/ds/transactions/tx/a/b"
    assert fsi_modules.pkg.TransactionPath("ds", "tx").end_ref == "tx"
    assert str(fsi_modules.pkg.ViewPath("ds", "end")) == "datasets/ds/views/end/files"
    assert (
        str(fsi_modules.pkg.ViewPath("ds", "end", "start", "a"))
        == "datasets/ds/views/end/startTransactionRid/start/files/a"
    )
    path = fsi_modules.pkg.FoundryPathWithBranchId(fsi_modules.pkg.TransactionPath("ds", "tx"), "master")
    assert path.branch_id == "master"
    assert path.foundry_path.dataset_rid == "ds"


def test_optional_snowflake_missing():
    blocked = dict.fromkeys(
        ["snowflake", "snowflake.connector", "snowflake.snowpark", "foundry_dev_tools._optional.snowflake"]
    )
    with mock.patch.dict(sys.modules, blocked):
        del sys.modules["foundry_dev_tools._optional.snowflake"]
        optional = importlib.import_module("foundry_dev_tools._optional.snowflake")
        assert optional.snowflake_errors.__fake__ is True
        with pytest.raises(ImportError, match="snowflake-snowpark-python"):
            optional.snowflake_errors.Error  # noqa: B018
