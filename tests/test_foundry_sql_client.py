import pytest
from requests_mock import Mocker
from requests_mock.adapter import ANY

from foundry_dev_tools.foundry_api_client import (
    BranchNotFoundError,
    DatasetHasNoSchemaError,
    DatasetNotFoundError,
    FoundrySqlClient,
    FoundrySqlQueryClientTimedOutError,
    FoundrySqlQueryFailedError,
    FoundrySqlSerializationFormatNotImplementedError,
)


def mock_initiate_session(
    requests_mock: Mocker,
    session_id="96bbd062-b5c1-428d-95f3-739683955ff8",
    session_auth_token="Zu+eMIE1Unlr7vvgZ2kHkg==",
):
    requests_mock.post(
        url=ANY,
        status_code=200,
        json={"sessionId": session_id, "sessionAuthToken": session_auth_token},
    )


@pytest.fixture()
def sql_client(requests_mock: Mocker):
    mock_initiate_session(requests_mock)
    return FoundrySqlClient()


def test_session_is_established(
    requests_mock: Mocker,
):
    mock_initiate_session(
        requests_mock,
        "96bbd062-b5c1-428d-95f3-739683955ff8",
        "Zu+eMIE1Unlr7vvgZ2kHkg==",
    )

    client = FoundrySqlClient()
    assert client.session_auth_token == "Zu+eMIE1Unlr7vvgZ2kHkg=="
    assert client.session_id == "96bbd062-b5c1-428d-95f3-739683955ff8"
    assert client._headers["Session-Authorization"] == "Zu+eMIE1Unlr7vvgZ2kHkg=="


def test_prepare_and_execute(requests_mock: Mocker, sql_client):
    requests_mock.post(
        url=ANY,
        status_code=200,
        json={
            "jobId": "217984f8-8a30-45a8-ba08-0e4a8c3f2e7c",
            "status": {
                "type": "ready",
                "ready": {"jobId": "217984f8-8a30-45a8-ba08-0e4a8c3f2e7c"},
            },
            "statementId": "de928220-61c1-4aa7-acd0-91e5b9d4ae95",
        },
    )
    response_dict = sql_client._prepare_and_execute("SELECT * FROM `blub`")
    assert response_dict == {
        "jobId": "217984f8-8a30-45a8-ba08-0e4a8c3f2e7c",
        "status": {
            "type": "ready",
            "ready": {"jobId": "217984f8-8a30-45a8-ba08-0e4a8c3f2e7c"},
        },
        "statementId": "de928220-61c1-4aa7-acd0-91e5b9d4ae95",
    }


def test_poll_for_query_completion(requests_mock: Mocker, sql_client):
    requests_mock.post(
        url=ANY,
        status_code=200,
        json={
            "status": {
                "type": "ready",
                "ready": {"jobId": "2c1d6597-77ea-41ac-9651-b86e19799800"},
            }
        },
    )

    sql_client._poll_for_query_completion(
        {
            "jobId": "2c1d6597-77ea-41ac-9651-b86e19799800",
            "status": {
                "type": "running",
                "running": {"jobId": "2c1d6597-77ea-41ac-9651-b86e19799800"},
            },
            "statementId": "57977172-8a57-4f62-91d1-094c35c0a78b",
        }
    )


def test_poll_for_query_completion_timeout(mocker, requests_mock: Mocker, sql_client):
    mocker.patch("time.sleep")  # we do not want to wait 500 ms in a test ;)

    requests_mock.post(
        url=ANY,
        status_code=200,
        json={
            "jobId": "2c1d6597-77ea-41ac-9651-b86e19799800",
            "status": {
                "type": "running",
                "running": {"jobId": "2c1d6597-77ea-41ac-9651-b86e19799800"},
            },
            "statementId": "57977172-8a57-4f62-91d1-094c35c0a78b",
        },
    )

    with pytest.raises(FoundrySqlQueryClientTimedOutError):
        sql_client._poll_for_query_completion(
            {
                "jobId": "2c1d6597-77ea-41ac-9651-b86e19799800",
                "status": {
                    "type": "running",
                    "running": {"jobId": "2c1d6597-77ea-41ac-9651-b86e19799800"},
                },
                "statementId": "57977172-8a57-4f62-91d1-094c35c0a78b",
            },
            0.001,
        )


def test_non_arrow_format_throws_exception(requests_mock: Mocker, sql_client):
    response_data = b'S{"metadata":{"computedTime":1627978857810,"resultComputationMetadata":{"submitTimeMillis":1627978856578,"startTimeMillis":1627978856580,"finishResolveDataFrameTimeMillis":1627978857302,"finishTimeMillis":1627978857763,"estimatedResultSizeOnHeapInBytes":32296,"cacheType":"NO_CACHE","initialDataframeResolutionType":"BUILT_INITIAL_SPARK_SET_AND_DF","complexDataframeResolutionType":"NO_COMPLEX_TRANSFORMATIONS","setDescription":{"type":"sql","parentsByParam":{"/Global/Foundry Operations/Foundry Support/iris-167312679":{"type":"initialwithtransaction","identifier":"ri.foundry.main.dataset.14703427-09ab-4c9c-b036-6926b34d150b:master","transaction":"ri.foundry.main.transaction.0000003a-a437-fec8-95b1-0872dc6a0991","schemaId":"00000003-d596-797c-9663-e8970c07d296","viewMetadataId":null,"maybeStartTransaction":"ri.foundry.main.transaction.0000003a-a437-fec8-95b1-0872dc6a0991"}},"sqlQuery":"SELECT * FROM ${/Global/Foundry Operations/Foundry Support/iris-167312679}"},"queryMetricMetadata":{"queueSubmittedTimeMillis":1627978856263,"wasPunted":false},"traceId":"65a45518d71a3922","saveTraceId":"1ae3ca3cbb85ca39","latitudeQueryDescription":{"humanReadableDescription":null,"metadata":{"Skip the non-contour backend cache":"false","UserId":"3c8fbda5-686e-4fcb-ad52-d95e4281d99f","GraphId":"","NodeId":"","RefRid":"","SourceId":"foundry-sql-server"},"sourceMetadata":{"graphId":null,"nodeId":null,"refRid":null,"refParentRid":null,"sourceId":"foundry-sql-server","sparkReporterOwningRid":null,"resourceManagementAttributionRid":null,"userId":"3c8fbda5-686e-4fcb-ad52-d95e4281d99f","datasetRids":["ri.foundry.main.dataset.14703427-09ab-4c9c-b036-6926b34d150b"],"lsdHash":"01b919b1f5528a8895d161da41b4d55d","lsdHashWithMetadataScrubbed":"9c4501650954bcc765e2c8b6fd2920c5","lsdHashWithDatasetAndMetadataScrubbed":"38d9cc6b14c2d21ac287b53050ebc9aa","unresolvedLsdHash":null,"unresolvedLsdHashWithMetadataScrubbed":null,"unresolvedLsdHashWithDatasetAndMetadataScrubbed":null},"trimmedDatasetRids":[],"requestTraceId":"b2a7f6e4f8e68d97","requestTimeMillis":1627978856150},"backendMetricsTag":"foundrysqlserver-contour-backend-foundry-0","backendGroupMetricsTag":"foundry-sql-server-modules","groupProducerMetricsTag":"foundrysqlserver","numberOfTasks":null,"numberOfStages":null},"computedVersion":"9.206.0","warningMessage":null,"resultId":null,"rowCount":150,"columns":["id","is_setosa","species","sepal_length","sepal_width","petal_length","petal_width"],"columnTypes":[{"type":"INTEGER","name":"id","nullable":true,"customMetadata":{}},{"type":"INTEGER","name":"is_setosa","nullable":true,"customMetadata":{}},{"type":"STRING","name":"species","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"sepal_length","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"sepal_width","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"petal_length","nullable":true,"customMetadata":{}},{"type":"DOUBLE","name":"petal_width","nullable":true,"customMetadata":{}}]},"rows":[[1,1,"setosa",5.1,3.5,1.4,0.2],[2,1,"setosa",4.9,3.0,1.4,0.2],[3,1,"setosa",4.7,3.2,1.3,0.2],[4,1,"setosa",4.6,3.1,1.5,0.2],[5,1,"setosa",5.0,3.6,1.4,0.2],[6,1,"setosa",5.0,3.4,1.5,0.2],[7,1,"setosa",4.4,2.9,1.4,0.2],[8,1,"setosa",5.4,3.7,1.5,0.2],[9,1,"setosa",4.8,3.4,1.6,0.2],[10,1,"setosa",5.8,4.0,1.2,0.2],[11,1,"setosa",5.4,3.4,1.7,0.2],[12,1,"setosa",4.6,3.6,1.0,0.2],[13,1,"setosa",4.8,3.4,1.9,0.2],[14,1,"setosa",5.0,3.0,1.6,0.2],[15,1,"setosa",5.2,3.5,1.5,0.2],[16,1,"setosa",5.2,3.4,1.4,0.2],[17,1,"setosa",4.7,3.2,1.6,0.2],[18,1,"setosa",4.8,3.1,1.6,0.2],[19,1,"setosa",5.5,4.2,1.4,0.2],[20,1,"setosa",4.9,3.1,1.5,0.2],[21,1,"setosa",5.0,3.2,1.2,0.2],[22,1,"setosa",5.5,3.5,1.3,0.2],[23,1,"setosa",4.4,3.0,1.3,0.2],[24,1,"setosa",5.1,3.4,1.5,0.2],[25,1,"setosa",4.4,3.2,1.3,0.2],[26,1,"setosa",5.1,3.8,1.6,0.2],[27,1,"setosa",4.6,3.2,1.4,0.2],[28,1,"setosa",5.3,3.7,1.5,0.2],[29,1,"setosa",5.0,3.3,1.4,0.2],[30,0,"versicolor",6.4,3.2,4.5,1.5],[31,0,"versicolor",6.9,3.1,4.9,1.5],[32,0,"versicolor",6.5,2.8,4.6,1.5],[33,0,"versicolor",5.9,3.0,4.2,1.5],[34,0,"versicolor",5.6,3.0,4.5,1.5],[35,0,"versicolor",6.2,2.2,4.5,1.5],[36,0,"versicolor",6.3,2.5,4.9,1.5],[37,0,"versicolor",6.0,2.9,4.5,1.5],[38,0,"versicolor",5.4,3.0,4.5,1.5],[39,0,"versicolor",6.7,3.1,4.7,1.5],[40,0,"virginica",6.0,2.2,5.0,1.5],[41,0,"virginica",6.3,2.8,5.1,1.5],[42,0,"versicolor",5.9,3.2,4.8,1.8],[43,0,"virginica",6.3,2.9,5.6,1.8],[44,0,"virginica",7.3,2.9,6.3,1.8],[45,0,"virginica",6.7,2.5,5.8,1.8],[46,0,"virginica",6.5,3.0,5.5,1.8],[47,0,"virginica",6.3,2.7,4.9,1.8],[48,0,"virginica",7.2,3.2,6.0,1.8],[49,0,"virginica",6.2,2.8,4.8,1.8],[50,0,"virginica",6.1,3.0,4.9,1.8],[51,0,"virginica",6.4,3.1,5.5,1.8],[52,0,"virginica",6.0,3.0,4.8,1.8],[53,0,"virginica",5.9,3.0,5.1,1.8],[54,0,"versicolor",5.5,2.3,4.0,1.3],[55,0,"versicolor",5.7,2.8,4.5,1.3],[56,0,"versicolor",6.6,2.9,4.6,1.3],[57,0,"versicolor",5.6,2.9,3.6,1.3],[58,0,"versicolor",6.1,2.8,4.0,1.3],[59,0,"versicolor",6.4,2.9,4.3,1.3],[60,0,"versicolor",6.3,2.3,4.4,1.3],[61,0,"versicolor",5.6,3.0,4.1,1.3],[62,0,"versicolor",5.5,2.5,4.0,1.3],[63,0,"versicolor",5.6,2.7,4.2,1.3],[64,0,"versicolor",5.7,2.9,4.2,1.3],[65,0,"versicolor",6.2,2.9,4.3,1.3],[66,0,"versicolor",5.7,2.8,4.1,1.3],[67,0,"versicolor",7.0,3.2,4.7,1.4],[68,0,"versicolor",5.2,2.7,3.9,1.4],[69,0,"versicolor",6.1,2.9,4.7,1.4],[70,0,"versicolor",6.7,3.1,4.4,1.4],[71,0,"versicolor",6.6,3.0,4.4,1.4],[72,0,"versicolor",6.8,2.8,4.8,1.4],[73,0,"versicolor",6.1,3.0,4.6,1.4],[74,0,"virginica",6.1,2.6,5.6,1.4],[75,0,"versicolor",4.9,2.4,3.3,1.0],[76,0,"versicolor",5.0,2.0,3.5,1.0],[77,0,"versicolor",6.0,2.2,4.0,1.0],[78,0,"versicolor",5.8,2.7,4.1,1.0],[79,0,"versicolor",5.7,2.6,3.5,1.0],[80,0,"versicolor",5.5,2.4,3.7,1.0],[81,0,"versicolor",5.0,2.3,3.3,1.0],[82,0,"virginica",6.4,3.2,5.3,2.3],[83,0,"virginica",7.7,2.6,6.9,2.3],[84,0,"virginica",6.9,3.2,5.7,2.3],[85,0,"virginica",7.7,3.0,6.1,2.3],[86,0,"virginica",6.9,3.1,5.1,2.3],[87,0,"virginica",6.8,3.2,5.9,2.3],[88,0,"virginica",6.7,3.0,5.2,2.3],[89,0,"virginica",6.2,3.4,5.4,2.3],[90,1,"setosa",5.4,3.9,1.7,0.4],[91,1,"setosa",5.7,4.4,1.5,0.4],[92,1,"setosa",5.4,3.9,1.3,0.4],[93,1,"setosa",5.1,3.7,1.5,0.4],[94,1,"setosa",5.0,3.4,1.6,0.4],[95,1,"setosa",5.4,3.4,1.5,0.4],[96,1,"setosa",5.1,3.8,1.9,0.4],[97,1,"setosa",4.6,3.4,1.4,0.3],[98,1,"setosa",5.1,3.5,1.4,0.3],[99,1,"setosa",5.7,3.8,1.7,0.3],[100,1,"setosa",5.1,3.8,1.5,0.3],[101,1,"setosa",5.0,3.5,1.3,0.3],[102,1,"setosa",4.5,2.3,1.3,0.3],[103,1,"setosa",4.8,3.0,1.4,0.3],[104,0,"virginica",6.5,3.2,5.1,2.0],[105,0,"virginica",5.7,2.5,5.0,2.0],[106,0,"virginica",5.6,2.8,4.9,2.0],[107,0,"virginica",7.7,2.8,6.7,2.0],[108,0,"virginica",7.9,3.8,6.4,2.0],[109,0,"virginica",6.5,3.0,5.2,2.0],[110,0,"versicolor",6.1,2.8,4.7,1.2],[111,0,"versicolor",5.8,2.7,3.9,1.2],[112,0,"versicolor",5.5,2.6,4.4,1.2],[113,0,"versicolor",5.8,2.6,4.0,1.2],[114,0,"versicolor",5.7,3.0,4.2,1.2],[115,0,"virginica",5.8,2.7,5.1,1.9],[116,0,"virginica",6.4,2.7,5.3,1.9],[117,0,"virginica",7.4,2.8,6.1,1.9],[118,0,"virginica",5.8,2.7,5.1,1.9],[119,0,"virginica",6.3,2.5,5.0,1.9],[120,0,"virginica",7.1,3.0,5.9,2.1],[121,0,"virginica",7.6,3.0,6.6,2.1],[122,0,"virginica",6.8,3.0,5.5,2.1],[123,0,"virginica",6.7,3.3,5.7,2.1],[124,0,"virginica",6.4,2.8,5.6,2.1],[125,0,"virginica",6.9,3.1,5.4,2.1],[126,1,"setosa",4.9,3.1,1.5,0.1],[127,1,"setosa",4.8,3.0,1.4,0.1],[128,1,"setosa",4.3,3.0,1.1,0.1],[129,1,"setosa",5.2,4.1,1.5,0.1],[130,1,"setosa",4.9,3.6,1.4,0.1],[131,0,"versicolor",6.3,3.3,4.7,1.6],[132,0,"versicolor",6.0,2.7,5.1,1.6],[133,0,"versicolor",6.0,3.4,4.5,1.6],[134,0,"virginica",7.2,3.0,5.8,1.6],[135,0,"versicolor",5.6,2.5,3.9,1.1],[136,0,"versicolor",5.5,2.4,3.8,1.1],[137,0,"versicolor",5.1,2.5,3.0,1.1],[138,0,"virginica",6.3,3.3,6.0,2.5],[139,0,"virginica",7.2,3.6,6.1,2.5],[140,0,"virginica",6.7,3.3,5.7,2.5],[141,0,"virginica",5.8,2.8,5.1,2.4],[142,0,"virginica",6.3,3.4,5.6,2.4],[143,0,"virginica",6.7,3.1,5.6,2.4],[144,0,"virginica",6.5,3.0,5.8,2.2],[145,0,"virginica",7.7,3.8,6.7,2.2],[146,0,"virginica",6.4,2.8,5.6,2.2],[147,0,"versicolor",6.7,3.0,5.0,1.7],[148,0,"virginica",4.9,2.5,4.5,1.7],[149,1,"setosa",5.0,3.5,1.6,0.6],[150,1,"setosa",5.1,3.3,1.7,0.5]]}'

    requests_mock.post(url=ANY, content=response_data)

    with pytest.raises(FoundrySqlSerializationFormatNotImplementedError):
        sql_client._read_results_arrow("statement-id")


def test_read_arrow_optional_polars(requests_mock: Mocker, sql_client):
    response_data = b'A\xff\xff\xff\xff\xb0\x01\x00\x00\x10\x00\x00\x00\x00\x00\n\x00\x0e\x00\x06\x00\r\x00\x08\x00\n\x00\x00\x00\x00\x00\x04\x00\x10\x00\x00\x00\x00\x01\n\x00\x0c\x00\x00\x00\x08\x00\x04\x00\n\x00\x00\x00\x08\x00\x00\x00,\x01\x00\x00\x01\x00\x00\x00\x0c\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x04\x00\x08\x00\x00\x00\x08\x00\x00\x00\x00\x01\x00\x00\xf5\x00\x00\x00{"computedTime":1627977691184,"resultComputationMetadata":null,"computedVersion":null,"warningMessage":null,"resultId":null,"rowCount":null,"columns":["MATNR"],"columnTypes":[{"type":"STRING","name":"MATNR","nullable":true,"customMetadata":{}}]}\x00\x00\x00\x08\x00\x00\x00metadata\x00\x00\x00\x00\x01\x00\x00\x00\x18\x00\x00\x00\x00\x00\x12\x00\x18\x00\x14\x00\x13\x00\x12\x00\x0c\x00\x00\x00\x08\x00\x04\x00\x12\x00\x00\x00\x14\x00\x00\x00\x14\x00\x00\x00\x18\x00\x00\x00\x00\x00\x05\x01\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x05\x00\x00\x00MATNR\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x98\x00\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x0e\x00\x15\x00\x10\x00\x04\x00\x0c\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x10\x00\x00\x00\x00\x03\n\x00\x18\x00\x0c\x00\x08\x00\x04\x00\n\x00\x00\x00\x14\x00\x00\x00H\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x0000062106-RNA-5UG\xff\xff\xff\xff\x00\x00\x00\x00E'

    requests_mock.post(url=ANY, content=response_data)

    arrow_stream = sql_client._read_results_arrow("statement-id")
    pdf = arrow_stream.read_pandas()
    assert pdf.shape == (1, 1)

    try:
        import polars as pl

        arrow_stream = sql_client._read_results_arrow("statement-id")
        pa_table = arrow_stream.read_all()
        df = pl.from_arrow(pa_table)
        assert df.shape == (1, 1)
    except:
        pass


@pytest.mark.integration
def test_smoke(iris_dataset):
    sql_client = FoundrySqlClient()
    one_row_one_column = sql_client.query(
        f"SELECT sepal_width FROM `{iris_dataset[1]}` LIMIT 1"
    )
    assert one_row_one_column.shape == (1, 1)


@pytest.mark.integration
def test_legacy_fallback(mocker, iris_dataset, client):
    from foundry_dev_tools import FoundryRestClient as frc_class

    spy = mocker.spy(frc_class, "query_foundry_sql_legacy")
    # The query does require SQL compute.
    # Queries which contain aggregate, join, order by, and filter predicates are not direct read eligible.
    # and unfortunately returned in JSON Format, not ARROW
    iris = client.query_foundry_sql(
        f"SELECT * FROM `{iris_dataset[1]}` order by sepal_width LIMIT 100"
    )
    assert iris.shape == (100, 5)
    spy.assert_called()


@pytest.mark.integration
def test_exceptions(iris_dataset, iris_no_schema_dataset):
    with pytest.raises(BranchNotFoundError):
        sql_client1 = FoundrySqlClient(branch="doesNotExist")
        sql_client1.query(f"SELECT * FROM `{iris_dataset[1]}` LIMIT 100")

    client = FoundrySqlClient()
    with pytest.raises(DatasetHasNoSchemaError):
        client.query(f"SELECT * FROM `{iris_no_schema_dataset[1]}` LIMIT 100")

    with pytest.raises(DatasetNotFoundError):
        client.query("SELECT * FROM `/namespace1/does-not_exists/` LIMIT 100")

    with pytest.raises(FoundrySqlQueryFailedError):
        client.query(f"SELECT foo, bar, FROM `{iris_dataset[1]}` LIMIT 100")
