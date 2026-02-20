import re

import polars as pl
import pytest

from foundry_dev_tools.errors.sql import (
    FoundrySqlQueryClientTimedOutError,
    FoundrySqlQueryFailedError,
    FoundrySqlSerializationFormatNotImplementedError,
)
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST


def test_poll_for_query_completion_timeout(mocker, test_context_mock):
    mocker.patch("time.sleep")  # we do not want to wait 500 ms in a test ;)

    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "queries/execute"),
        json={"queryId": "6941e90b-7a18-4ee9-8e82-63b64d7a7bde", "status": {"type": "running", "running": {}}},
    )

    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "foundry-sql-server", "")) + "queries/[^/]*/status"),
        json={"queryId": "6941e90b-7a18-4ee9-8e82-63b64d7a7bde", "status": {"type": "running", "running": {}}},
    )
    with pytest.raises(FoundrySqlQueryClientTimedOutError):
        test_context_mock.foundry_sql_server.query_foundry_sql(
            "SELECT * FROM `ri.foundry.main.dataset.249d2faf-4dcb-490b-9de5-3ae1160e8fbf`",
            timeout=0.001,
        )


def test_non_arrow_format_throws_exception(test_context_mock):
    response_data = b'S{"metadata":{"computedTime":1627978857810,"resultComputationMetadata":{"submitTimeMillis":1627978856578,"startTimeMillis":1627978856580,"finishResolveDataFrameTimeMillis":1627978857302,"finishTimeMillis":1627978857763,"estimatedResultSizeOnHeapInBytes":32296,"cacheType":"NO_CACHE","initialDataframeResolutionType":"BUILT_INITIAL_SPARK_SET_AND_DF","complexDataframeResolutionType":"NO_COMPLEX_TRANSFORMATIONS","setDescription":{"type":"sql","parentsByParam":{"/Global/Foundry Operations/Foundry Support/iris-167312679":{"type":"initialwithtransaction","identifier":"ri.foundry.main.dataset.14703427-09ab-4c9c-b036-6926b34d150b:master","transaction":"ri.foundry.main.transaction.0000003a-a437-fec8-95b1-0872dc6a0991","schemaId":"00000003-d596-797c-9663-e8970c07d296","viewMetadataId":null,"maybeStartTransaction":"ri.foundry.main.transaction.0000003a-a437-fec8-95b1-0872dc6a0991"}},"sqlQuery":"SELECT * FROM ${/Global/Foundry Operations/Foundry Support/iris-167312679}"},"queryMetricMetadata":{"queueSubmittedTimeMillis":1627978856263,"wasPunted":false},"traceId":"65a45518d71a3922","saveTraceId":"1ae3ca3cbb85ca39","latitudeQueryDescription":{"humanReadableDescription":null,"metadata":{"Skip the non-contour backend cache":"false","UserId":"3c8fbda5-686e-4fcb-ad52-d95e4281d99f","GraphId":"","NodeId":"","RefRid":"","SourceId":"foundry-sql-server"},"sourceMetadata":{"graphId":null,"nodeId":null,"refRid":null,"refParentRid":null,"sourceId":"foundry-sql-server","sparkReporterOwningRid":null,"resourceManagementAttributionRid":null,"userId":"3c8fbda5-686e-4fcb-ad52-d95e4281d99f","datasetRids":["ri.foundry.main.dataset.14703427-09ab-4c9c-b036-6926b34d150b"],"lsdHash":"01b919b1f5528a8895d161da41b4d55d","lsdHashWithMetadataScrubbed":"9c4501650954bcc765e2c8b6fd2920c5","lsdHashWithDatasetAndMetadataScrubbed":"38d9cc6b14c2d21ac287b53050ebc9aa","unresolvedLsdHash":null,"unresolvedLsdHashWithMetadataScrubbed":null,"unresolvedLsdHashWithDatasetAndMetadataScrubbed":null},"trimmedDatasetRids":[],"requestTraceId":"b2a7f6e4f8e68d97","requestTimeMillis":1627978856150},"backendMetricsTag":"foundrysqlserver-contour-backend-foundry-0","backendGroupMetricsTag":"foundry-sql-server-modules","groupProducerMetricsTag":"foundrysqlserver","numberOfTasks":null,"numberOfStages":null},"computedVersion":"9.206.0","warningMessage":null,"resultId":null,"rowCount":150,"columns":["id","is_setosa","species","sepal_length","sepal_width","petal_length","petal_width"],"columnTypes":[{"type":"INTEGER","name":"id","nullable":true,"customMetadata":{}},{"type":"INTEGER","name":"is_setosa","nullable":true,"customMetadata":{}},{"type":"STRING","name":"species","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"sepal_length","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"sepal_width","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"petal_length","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"petal_width","nullable":true,"customMetadata":{}}]},"rows":[[1,1,"setosa",5.1,3.5,1.4,0.2],[2,1,"setosa",4.9,3.0,1.4,0.2],[3,1,"setosa",4.7,3.2,1.3,0.2],[4,1,"setosa",4.6,3.1,1.5,0.2],[5,1,"setosa",5.0,3.6,1.4,0.2],[6,1,"setosa",5.0,3.4,1.5,0.2],[7,1,"setosa",4.4,2.9,1.4,0.2],[8,1,"setosa",5.4,3.7,1.5,0.2],[9,1,"setosa",4.8,3.4,1.6,0.2],[10,1,"setosa",5.8,4.0,1.2,0.2],[11,1,"setosa",5.4,3.4,1.7,0.2],[12,1,"setosa",4.6,3.6,1.0,0.2],[13,1,"setosa",4.8,3.4,1.9,0.2],[14,1,"setosa",5.0,3.0,1.6,0.2],[15,1,"setosa",5.2,3.5,1.5,0.2],[16,1,"setosa",5.2,3.4,1.4,0.2],[17,1,"setosa",4.7,3.2,1.6,0.2],[18,1,"setosa",4.8,3.1,1.6,0.2],[19,1,"setosa",5.5,4.2,1.4,0.2],[20,1,"setosa",4.9,3.1,1.5,0.2],[21,1,"setosa",5.0,3.2,1.2,0.2],[22,1,"setosa",5.5,3.5,1.3,0.2],[23,1,"setosa",4.4,3.0,1.3,0.2],[24,1,"setosa",5.1,3.4,1.5,0.2],[25,1,"setosa",4.4,3.2,1.3,0.2],[26,1,"setosa",5.1,3.8,1.6,0.2],[27,1,"setosa",4.6,3.2,1.4,0.2],[28,1,"setosa",5.3,3.7,1.5,0.2],[29,1,"setosa",5.0,3.3,1.4,0.2],[30,0,"versicolor",6.4,3.2,4.5,1.5],[31,0,"versicolor",6.9,3.1,4.9,1.5],[32,0,"versicolor",6.5,2.8,4.6,1.5],[33,0,"versicolor",5.9,3.0,4.2,1.5],[34,0,"versicolor",5.6,3.0,4.5,1.5],[35,0,"versicolor",6.2,2.2,4.5,1.5],[36,0,"versicolor",6.3,2.5,4.9,1.5],[37,0,"versicolor",6.0,2.9,4.5,1.5],[38,0,"versicolor",5.4,3.0,4.5,1.5],[39,0,"versicolor",6.7,3.1,4.7,1.5],[40,0,"virginica",6.0,2.2,5.0,1.5],[41,0,"virginica",6.3,2.8,5.1,1.5],[42,0,"versicolor",5.9,3.2,4.8,1.8],[43,0,"virginica",6.3,2.9,5.6,1.8],[44,0,"virginica",7.3,2.9,6.3,1.8],[45,0,"virginica",6.7,2.5,5.8,1.8],[46,0,"virginica",6.5,3.0,5.5,1.8],[47,0,"virginica",6.3,2.7,4.9,1.8],[48,0,"virginica",7.2,3.2,6.0,1.8],[49,0,"virginica",6.2,2.8,4.8,1.8],[50,0,"virginica",6.1,3.0,4.9,1.8],[51,0,"virginica",6.4,3.1,5.5,1.8],[52,0,"virginica",6.0,3.0,4.8,1.8],[53,0,"virginica",5.9,3.0,5.1,1.8],[54,0,"versicolor",5.5,2.3,4.0,1.3],[55,0,"versicolor",5.7,2.8,4.5,1.3],[56,0,"versicolor",6.6,2.9,4.6,1.3],[57,0,"versicolor",5.6,2.9,3.6,1.3],[58,0,"versicolor",6.1,2.8,4.0,1.3],[59,0,"versicolor",6.4,2.9,4.3,1.3],[60,0,"versicolor",6.3,2.3,4.4,1.3],[61,0,"versicolor",5.6,3.0,4.1,1.3],[62,0,"versicolor",5.5,2.5,4.0,1.3],[63,0,"versicolor",5.6,2.7,4.2,1.3],[64,0,"versicolor",5.7,2.9,4.2,1.3],[65,0,"versicolor",6.2,2.9,4.3,1.3],[66,0,"versicolor",5.7,2.8,4.1,1.3],[67,0,"versicolor",7.0,3.2,4.7,1.4],[68,0,"versicolor",5.2,2.7,3.9,1.4],[69,0,"versicolor",6.1,2.9,4.7,1.4],[70,0,"versicolor",6.7,3.1,4.4,1.4],[71,0,"versicolor",6.6,3.0,4.4,1.4],[72,0,"versicolor",6.8,2.8,4.8,1.4],[73,0,"versicolor",6.1,3.0,4.6,1.4],[74,0,"virginica",6.1,2.6,5.6,1.4],[75,0,"versicolor",4.9,2.4,3.3,1.0],[76,0,"versicolor",5.0,2.0,3.5,1.0],[77,0,"versicolor",6.0,2.2,4.0,1.0],[78,0,"versicolor",5.8,2.7,4.1,1.0],[79,0,"versicolor",5.7,2.6,3.5,1.0],[80,0,"versicolor",5.5,2.4,3.7,1.0],[81,0,"versicolor",5.0,2.3,3.3,1.0],[82,0,"virginica",6.4,3.2,5.3,2.3],[83,0,"virginica",7.7,2.6,6.9,2.3],[84,0,"virginica",6.9,3.2,5.7,2.3],[85,0,"virginica",7.7,3.0,6.1,2.3],[86,0,"virginica",6.9,3.1,5.1,2.3],[87,0,"virginica",6.8,3.2,5.9,2.3],[88,0,"virginica",6.7,3.0,5.2,2.3],[89,0,"virginica",6.2,3.4,5.4,2.3],[90,1,"setosa",5.4,3.9,1.7,0.4],[91,1,"setosa",5.7,4.4,1.5,0.4],[92,1,"setosa",5.4,3.9,1.3,0.4],[93,1,"setosa",5.1,3.7,1.5,0.4],[94,1,"setosa",5.0,3.4,1.6,0.4],[95,1,"setosa",5.4,3.4,1.5,0.4],[96,1,"setosa",5.1,3.8,1.9,0.4],[97,1,"setosa",4.6,3.4,1.4,0.3],[98,1,"setosa",5.1,3.5,1.4,0.3],[99,1,"setosa",5.7,3.8,1.7,0.3],[100,1,"setosa",5.1,3.8,1.5,0.3],[101,1,"setosa",5.0,3.5,1.3,0.3],[102,1,"setosa",4.5,2.3,1.3,0.3],[103,1,"setosa",4.8,3.0,1.4,0.3],[104,0,"virginica",6.5,3.2,5.1,2.0],[105,0,"virginica",5.7,2.5,5.0,2.0],[106,0,"virginica",5.6,2.8,4.9,2.0],[107,0,"virginica",7.7,2.8,6.7,2.0],[108,0,"virginica",7.9,3.8,6.4,2.0],[109,0,"virginica",6.5,3.0,5.2,2.0],[110,0,"versicolor",6.1,2.8,4.7,1.2],[111,0,"versicolor",5.8,2.7,3.9,1.2],[112,0,"versicolor",5.5,2.6,4.4,1.2],[113,0,"versicolor",5.8,2.6,4.0,1.2],[114,0,"versicolor",5.7,3.0,4.2,1.2],[115,0,"virginica",5.8,2.7,5.1,1.9],[116,0,"virginica",6.4,2.7,5.3,1.9],[117,0,"virginica",7.4,2.8,6.1,1.9],[118,0,"virginica",5.8,2.7,5.1,1.9],[119,0,"virginica",6.3,2.5,5.0,1.9],[120,0,"virginica",7.1,3.0,5.9,2.1],[121,0,"virginica",7.6,3.0,6.6,2.1],[122,0,"virginica",6.8,3.0,5.5,2.1],[123,0,"virginica",6.7,3.3,5.7,2.1],[124,0,"virginica",6.4,2.8,5.6,2.1],[125,0,"virginica",6.9,3.1,5.4,2.1],[126,1,"setosa",4.9,3.1,1.5,0.1],[127,1,"setosa",4.8,3.0,1.4,0.1],[128,1,"setosa",4.3,3.0,1.1,0.1],[129,1,"setosa",5.2,4.1,1.5,0.1],[130,1,"setosa",4.9,3.6,1.4,0.1],[131,0,"versicolor",6.3,3.3,4.7,1.6],[132,0,"versicolor",6.0,2.7,5.1,1.6],[133,0,"versicolor",6.0,3.4,4.5,1.6],[134,0,"virginica",7.2,3.0,5.8,1.6],[135,0,"versicolor",5.6,2.5,3.9,1.1],[136,0,"versicolor",5.5,2.4,3.8,1.1],[137,0,"versicolor",5.1,2.5,3.0,1.1],[138,0,"virginica",6.3,3.3,6.0,2.5],[139,0,"virginica",7.2,3.6,6.1,2.5],[140,0,"virginica",6.7,3.3,5.7,2.5],[141,0,"virginica",5.8,2.8,5.1,2.4],[142,0,"virginica",6.3,3.4,5.6,2.4],[143,0,"virginica",6.7,3.1,5.6,2.4],[144,0,"virginica",6.5,3.0,5.8,2.2],[145,0,"virginica",7.7,3.8,6.7,2.2],[146,0,"virginica",6.4,2.8,5.6,2.2],[147,0,"versicolor",6.7,3.0,5.0,1.7],[148,0,"virginica",4.9,2.5,4.5,1.7],[149,1,"setosa",5.0,3.5,1.6,0.6],[150,1,"setosa",5.1,3.3,1.7,0.5]]}'  # noqa: E501

    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "foundry-sql-server", "")) + "queries/[^/]*/results"),
        content=response_data,
    )
    with pytest.raises(FoundrySqlSerializationFormatNotImplementedError):
        test_context_mock.foundry_sql_server.read_fsql_query_results_arrow("queryid")


def test_read_arrow_optional_polars(test_context_mock):
    response_data = b'A\xff\xff\xff\xff\xb0\x01\x00\x00\x10\x00\x00\x00\x00\x00\n\x00\x0e\x00\x06\x00\r\x00\x08\x00\n\x00\x00\x00\x00\x00\x04\x00\x10\x00\x00\x00\x00\x01\n\x00\x0c\x00\x00\x00\x08\x00\x04\x00\n\x00\x00\x00\x08\x00\x00\x00,\x01\x00\x00\x01\x00\x00\x00\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\x08\x00\x00\x00\x00\x01\x00\x00\xf5\x00\x00\x00{"computedTime":1627977691184,"resultComputationMetadata":null,"computedVersion":null,"warningMessage":null,"resultId":null,"rowCount":null,"columns":["MATNR"],"columnTypes":[{"type":"STRING","name":"MATNR","nullable":true,"customMetadata":{}}]}\x00\x00\x00\x08\x00\x00\x00metadata\x00\x00\x00\x00\x01\x00\x00\x00\x18\x00\x00\x00\x00\x00\x12\x00\x18\x00\x14\x00\x13\x00\x12\x00\x0c\x00\x00\x00\x08\x00\x04\x00\x12\x00\x00\x00\x14\x00\x00\x00\x14\x00\x00\x00\x18\x00\x00\x00\x00\x00\x05\x01\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x05\x00\x00\x00MATNR\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x98\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x0e\x00\x15\x00\x10\x00\x04\x00\x0c\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x10\x00\x00\x00\x00\x03\n\x00\x18\x00\x0c\x00\x08\x00\x04\x00\n\x00\x00\x00\x14\x00\x00\x00H\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x0000062106-RNA-5UG\xff\xff\xff\xff\x00\x00\x00\x00E'  # noqa: E501
    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "foundry-sql-server", "")) + "queries/[^/]*/results"),
        content=response_data,
    )

    arrow_stream = test_context_mock.foundry_sql_server.read_fsql_query_results_arrow("queryId")
    pdf = arrow_stream.read_pandas()
    assert pdf.shape == (1, 1)

    arrow_stream = test_context_mock.foundry_sql_server.read_fsql_query_results_arrow("queryId")
    pa_table = arrow_stream.read_all()
    df = pl.from_arrow(pa_table)
    assert df.shape == (1, 1)


def test_exception(mocker, test_context_mock):
    mocker.patch("time.sleep")  # we do not want to wait 500 ms in a test ;)

    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "queries/execute"),
        json={"queryId": "6941e90b-7a18-4ee9-8e82-63b64d7a7bde", "status": {"type": "running", "running": {}}},
    )

    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "foundry-sql-server", "")) + "queries/[^/]*/status"),
        json={
            "status": {
                "type": "failed",
                "failed": {
                    "errorMessage": "org.apache.spark.sql.AnalysisException: ",
                    "failureReason": "FAILED_TO_EXECUTE",
                },
            }
        },
    )
    with pytest.raises(FoundrySqlQueryFailedError) as exception:
        test_context_mock.foundry_sql_server.query_foundry_sql(
            "SELECT notExistingColumn FROM `ri.foundry.main.dataset.249d2faf-4dcb-490b-9de5-3ae1160e8fbf`",
            timeout=0.001,
        )
    assert exception.value.error_message == "org.apache.spark.sql.AnalysisException: "


def test_exception_unknown_json(mocker, test_context_mock):
    mocker.patch("time.sleep")  # we do not want to wait 500 ms in a test ;)

    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "queries/execute"),
        json={"queryId": "6941e90b-7a18-4ee9-8e82-63b64d7a7bde", "status": {"type": "running", "running": {}}},
    )

    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(TEST_HOST.url, "foundry-sql-server", "")) + "queries/[^/]*/status"),
        json={
            "status": {
                "type": "failed",
                "unknownKey": {
                    "errorMessage": "org.apache.spark.sql.AnalysisException: ",
                    "failureReason": "FAILED_TO_EXECUTE",
                },
            }
        },
    )
    with pytest.raises(FoundrySqlQueryFailedError) as exception:
        test_context_mock.foundry_sql_server.query_foundry_sql(
            "SELECT notExistingColumn FROM `ri.foundry.main.dataset.249d2faf-4dcb-490b-9de5-3ae1160e8fbf`",
            timeout=0.001,
        )
    assert exception.value.error_message == ""


def test_v2_experimental_use_trino(mocker, test_context_mock):
    """Test that experimental_use_trino parameter modifies the query correctly."""
    import pandas as pd

    mocker.patch("time.sleep")  # we do not want to wait in tests

    # Mock the arrow stream reader to return a simple pandas DataFrame
    mock_arrow_reader = mocker.MagicMock()
    mock_arrow_reader.read_pandas.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mocker.patch.object(
        test_context_mock.foundry_sql_server_v2,
        "read_stream_results_arrow",
        return_value=mock_arrow_reader,
    )

    # Mock the api_query endpoint (initial query execution)
    query_matcher = mocker.MagicMock()
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/query"),
        json={"type": "running", "running": {"queryHandle": {"queryId": "test-query-id-123", "type": "foundry"}}},
        additional_matcher=query_matcher,
    )

    # Mock the api_status endpoint (poll for completion - returns ready immediately)
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/status"),
        json={
            "status": {
                "type": "ready",
                "ready": {"tickets": [{"tickets": ["eyJhbGc...mock-ticket-1", "eyJhbGc...mock-ticket-2"]}]},
            }
        },
    )

    # Test with experimental_use_trino=True
    df = test_context_mock.foundry_sql_server_v2.query_foundry_sql(
        "SELECT * FROM `ri.foundry.main.dataset.test-dataset`",
        experimental_use_trino=True,
    )

    # Verify the query was modified to include the Trino backend hint
    call_args = query_matcher.call_args_list[0]
    request = call_args[0][0]
    request_json = request.json()

    assert "SELECT /*+ backend(trino) */ * FROM" in request_json["querySpec"]["query"]
    assert df.shape[0] == 3

    # Reset for second test
    query_matcher.reset_mock()

    # Test with experimental_use_trino=False (default)
    df = test_context_mock.foundry_sql_server_v2.query_foundry_sql(
        "SELECT * FROM `ri.foundry.main.dataset.test-dataset`",
        experimental_use_trino=False,
    )

    # Verify the query was NOT modified
    call_args = query_matcher.call_args_list[0]
    request = call_args[0][0]
    request_json = request.json()

    assert request_json["querySpec"]["query"] == "SELECT * FROM `ri.foundry.main.dataset.test-dataset`"
    assert "backend(trino)" not in request_json["querySpec"]["query"]
    assert df.shape[0] == 3


def test_v2_poll_for_query_completion_timeout(mocker, test_context_mock):
    """Test that V2 query times out correctly when polling takes too long."""
    mocker.patch("time.sleep")  # we do not want to wait in tests

    # Mock the api_query endpoint (initial query execution)
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/query"),
        json={"type": "running", "running": {"queryHandle": {"queryId": "test-query-timeout-123", "type": "foundry"}}},
    )

    # Mock the api_status endpoint to always return running status
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/status"),
        json={"status": {"type": "running", "running": {}}},
    )

    with pytest.raises(FoundrySqlQueryClientTimedOutError):
        test_context_mock.foundry_sql_server_v2.query_foundry_sql(
            "SELECT * FROM `ri.foundry.main.dataset.test-dataset`",
            timeout=0.001,
        )


def test_v2_ansi_dialect_not_supported(test_context_mock):
    """Test that V2 client rejects ANSI SQL dialect."""
    with pytest.raises(TypeError, match="'ANSI' is not a valid option for dialect"):
        test_context_mock.foundry_sql_server_v2.query_foundry_sql(
            "SELECT * FROM `ri.foundry.main.dataset.test-dataset`",
            sql_dialect="ANSI",  # type: ignore[arg-type]
        )


def test_v2_invalid_compression_codec(test_context_mock):
    """Test that V2 client rejects invalid arrow compression codec."""
    with pytest.raises(TypeError, match="'INVALID' is not a valid option for arrow_compression_codec"):
        test_context_mock.foundry_sql_server_v2.query_foundry_sql(
            "SELECT * FROM `ri.foundry.main.dataset.test-dataset`",
            arrow_compression_codec="INVALID",  # type: ignore[arg-type]
        )


def test_v2_query_failed_error_details(mocker, test_context_mock):
    """Test that V2 error responses with rich parameters are properly extracted."""
    mocker.patch("time.sleep")

    # Mock the api_query endpoint (initial query execution)
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/query"),
        json={"type": "running", "running": {"queryHandle": {"queryId": "test-query-id", "type": "foundry"}}},
    )

    # Mock the api_status endpoint with V2 error structure containing rich parameters
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "foundry-sql-server", "sql-endpoint/v1/queries/status"),
        json={
            "status": {
                "type": "failed",
                "failed": {
                    "errorCode": "INVALID_ARGUMENT",
                    "errorName": "SqlQueryService:SqlSyntaxError",
                    "errorInstanceId": "c16cb2b7-01ec-42a9-9ee2-0e57e2aed4ba",
                    "parameters": {
                        "endLine": 1,
                        "endColumn": 15350,
                        "dialect": "SPARK",
                        "queryFragment": "",
                        "startColumn": 15340,
                        "startLine": 1,
                        "userFriendlyMessage": (
                            "From line 1, column 15340 to line 1, column 15350: "
                            "Column 'COLUMN_NAME' not found in table 'my_table'; did you mean 'column_name'?"
                        ),
                    },
                },
            }
        },
    )

    with pytest.raises(FoundrySqlQueryFailedError) as exception:
        test_context_mock.foundry_sql_server_v2.query_foundry_sql(
            "SELECT COLUMN_NAME FROM `ri.foundry.main.dataset.test-dataset`",
        )

    # Verify all error parameters are extracted and accessible
    assert exception.value.error_code == "INVALID_ARGUMENT"
    assert exception.value.error_name == "SqlQueryService:SqlSyntaxError"
    assert exception.value.error_instance_id == "c16cb2b7-01ec-42a9-9ee2-0e57e2aed4ba"

    # Verify parameters are converted from camelCase to snake_case and accessible
    assert exception.value.start_line == 1
    assert exception.value.end_line == 1
    assert exception.value.start_column == 15340
    assert exception.value.end_column == 15350
    assert exception.value.dialect == "SPARK"
    # query_fragment is in kwargs even if empty
    assert "query_fragment" in exception.value.kwargs

    # Verify userFriendlyMessage is used as the info text and accessible
    assert exception.value.user_friendly_message == (
        "From line 1, column 15340 to line 1, column 15350: "
        "Column 'COLUMN_NAME' not found in table 'my_table'; did you mean 'column_name'?"
    )

    # Verify the exception message string includes the user-friendly message
    exception_str = str(exception.value)
    assert "COLUMN_NAME" in exception_str
    assert "my_table" in exception_str
