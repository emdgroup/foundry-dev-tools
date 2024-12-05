from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST


def test_list_dataset_files(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "PUT",
        build_api_url(TEST_HOST.url, "foundry-catalog", "catalog/datasets/rid/views/master/files3"),
        response_list=[
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_0.parquet",
                            "physicalPath": "1234",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ],
                    "nextPageToken": "dummy_file_1.parquet",
                }
            },
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_1.parquet",
                            "physicalPath": "2345",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ],
                    "nextPageToken": "dummy_file_2.parquet",
                }
            },
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_2.parquet",
                            "physicalPath": "234234",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ],
                    "nextPageToken": None,
                }
            },
        ],
    )

    files = test_context_mock.catalog.list_dataset_files(dataset_rid="rid", page_size=1)

    assert len(files) == 3
    assert test_context_mock.mock_adapter.call_count == 3


def test_list_dataset_files_nextpagetoken_not_present(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "PUT",
        build_api_url(TEST_HOST.url, "foundry-catalog", "catalog/datasets/rid/views/master/files3"),
        response_list=[
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_0.parquet",
                            "physicalPath": "1234",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ],
                    "nextPageToken": "dummy_file_1.parquet",
                }
            },
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_1.parquet",
                            "physicalPath": "2345",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ],
                    "nextPageToken": "dummy_file_2.parquet",
                }
            },
            {
                "json": {
                    "values": [
                        {
                            "logicalPath": "dummy_file_2.parquet",
                            "physicalPath": "234234",
                            "physicalUri": "https://s3.eu-central-1.amazonaws.com/",
                            "transactionRid": "ri.foundry.main.transaction.000002c3-6680-ad68-8d6b-500ef09cbd46",
                            "fileMetadata": {"length": 523},
                            "isOpen": False,
                            "timeModified": "2024-12-05T14:36:18.413Z",
                        }
                    ]
                }
            },
        ],
    )

    files = test_context_mock.catalog.list_dataset_files(dataset_rid="rid", page_size=1)

    assert len(files) == 3
    assert test_context_mock.mock_adapter.call_count == 3
