from random import choice
from string import ascii_uppercase

import pytest

from foundry_dev_tools.utils import api_types
from tests.integration.conftest import TEST_SINGLETON
from tests.integration.utils import INTEGRATION_TEST_COMPASS_ROOT_RID, INTEGRATION_TEST_EGRESS_POLICY_RID, MARKING_ID


@pytest.fixture()
def empty_s3_source() -> api_types.SourceRid:
    client = TEST_SINGLETON.ctx.magritte_coordinator
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "fdt-test-s3_" + rnd
    description = "FDT Integration Test"
    source_rid = client.create_s3_direct_source(
        name=name, parent_rid=INTEGRATION_TEST_COMPASS_ROOT_RID, url="s3://just-a-test", description=description
    )

    yield source_rid

    # Delete test folder
    response = TEST_SINGLETON.ctx.compass.api_delete_permanently(
        rids={source_rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"}
    )
    assert response.status_code == 200


def test_create_delete(empty_s3_source):
    client = TEST_SINGLETON.ctx.magritte_coordinator
    rid = empty_s3_source

    config = client.get_source_config(source_rid=rid)
    assert config["source"]["type"] == "s3-direct"
    assert config["source"]["url"] == "s3://just-a-test"

    description_returned = client.get_source_description(source_rid=rid)
    assert description_returned["type"] == "s3-direct"


def test_description_mandatory_default_empty():
    client = TEST_SINGLETON.ctx.magritte_coordinator
    # Create a new S3 Direct source
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "fdt-test-s3_" + rnd
    rid = client.create_s3_direct_source(
        name=name, parent_rid=INTEGRATION_TEST_COMPASS_ROOT_RID, url="s3://just-a-test"
    )

    description_returned = client.get_source_description(source_rid=rid)
    assert description_returned["description"] == ""

    TEST_SINGLETON.ctx.compass.api_delete_permanently(rids={rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"})


def test_create_with_catalog():
    client = TEST_SINGLETON.ctx.magritte_coordinator
    # Create a new S3 Direct source
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "fdt-test-s3_" + rnd
    sts_role_configuration = {
        "roleArn": "arn:aws:iam::133751691234:role/foundry-oidc-role",
        "roleSessionName": "session",
        "roleSessionDuration": 1234,
        "externalId": "externalId",
        "stsEndpoint": "https://sts.amazonaws.com",
    }
    rid = client.create_s3_direct_source(
        name=name,
        parent_rid=INTEGRATION_TEST_COMPASS_ROOT_RID,
        url="s3://just-a-test",
        region="eu-central-1",
        catalog={"glue": {}, "type": "glue"},
        sts_role_configuration=sts_role_configuration,
    )

    config_returned = client.get_source_config(source_rid=rid)
    assert config_returned["source"]["catalog"] == {"glue": {}, "type": "glue"}
    assert config_returned["source"]["region"] == "eu-central-1"
    assert config_returned["source"]["stsRoleConfiguration"] == sts_role_configuration

    TEST_SINGLETON.ctx.compass.api_delete_permanently(rids={rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"})


def test_create_with_kwargs():
    client = TEST_SINGLETON.ctx.magritte_coordinator
    # Create a new S3 Direct source
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "fdt-test-s3_" + rnd

    rid = client.create_s3_direct_source(
        name=name,
        parent_rid=INTEGRATION_TEST_COMPASS_ROOT_RID,
        url="s3://just-a-test",
        region="eu-central-1",
        s3Endpoint="s3.eu-central-1.amazonaws.com",
    )

    config_returned = client.get_source_config(source_rid=rid)
    assert config_returned["source"]["s3Endpoint"] == "s3.eu-central-1.amazonaws.com"

    TEST_SINGLETON.ctx.compass.api_delete_permanently(rids={rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"})


def test_add_oidc_config(empty_s3_source):
    client = TEST_SINGLETON.ctx.magritte_coordinator
    source_rid = empty_s3_source

    # enable OIDC
    client.enable_s3_oidc_runtime(source_rid=source_rid)

    # Set Network Egress
    client.set_network_egress_policies(
        source_rid=source_rid,
        network_egress_policies={INTEGRATION_TEST_EGRESS_POLICY_RID},
    )
    runtime_platform = client.api_get_runtime_platform(source_rid=source_rid)
    assert runtime_platform.status_code == 200
    as_json = runtime_platform.json()
    assert as_json["cloud"]["networkEgressPolicies"]["networkEgressPolicies"] == [INTEGRATION_TEST_EGRESS_POLICY_RID]

    oidc_issuer = client.get_oidc_issuer()
    assert as_json["type"] == "cloud"
    assert as_json["cloud"]["oidcRuntime"] == {
        "audience": "sts.amazonaws.com",
        "issuer": oidc_issuer,
        "subject": source_rid,
    }

    client.disable_s3_oidc_runtime(source_rid=source_rid)

    runtime_platform = client.api_get_runtime_platform(source_rid=source_rid)
    assert runtime_platform.status_code == 200
    as_json = runtime_platform.json()
    assert as_json["cloud"]["oidcRuntime"] is None
    assert as_json["cloud"]["networkEgressPolicies"]["networkEgressPolicies"] == [INTEGRATION_TEST_EGRESS_POLICY_RID]

    client.delete_network_egress_policy(
        source_rid=source_rid, network_egress_policy_rid=INTEGRATION_TEST_EGRESS_POLICY_RID
    )
    assert client.get_network_egress_policies(source_rid=source_rid) == []

    client.add_network_egress_policy(
        source_rid=source_rid, network_egress_policy_rid=INTEGRATION_TEST_EGRESS_POLICY_RID
    )
    assert client.get_network_egress_policies(source_rid=source_rid) == [INTEGRATION_TEST_EGRESS_POLICY_RID]


def test_code_import_restrictions(empty_s3_source):
    client = TEST_SINGLETON.ctx.magritte_coordinator

    client.enable_code_imports(source_rid=empty_s3_source, to_enable=["stemmaRepository"])

    response = client.api_bulk_get_usage_restrictions_for_source(source_rids=[empty_s3_source])
    assert response.status_code == 200
    as_json = response.json()
    assert as_json["results"][empty_s3_source]["enabled"][0]["stemmaRepository"] == {}

    client.enable_code_imports(source_rid=empty_s3_source, to_enable=["eddiePipeline"])

    response = client.api_bulk_get_usage_restrictions_for_source(source_rids=[empty_s3_source])
    assert response.status_code == 200
    as_json = response.json()
    assert len(as_json["results"][empty_s3_source]["enabled"]) == 2

    client.restrict_code_imports(source_rid=empty_s3_source, to_restrict=["stemmaRepository"])

    response = client.api_bulk_get_usage_restrictions_for_source(source_rids=[empty_s3_source])
    assert response.status_code == 200
    as_json = response.json()
    assert len(as_json["results"][empty_s3_source]["enabled"]) == 1
    assert as_json["results"][empty_s3_source]["enabled"][0]["eddiePipeline"] == {}

    client.restrict_code_imports(source_rid=empty_s3_source, to_restrict=["eddiePipeline"])

    response = client.api_bulk_get_usage_restrictions_for_source(source_rids=[empty_s3_source])
    assert response.status_code == 200
    as_json = response.json()
    assert len(as_json["results"][empty_s3_source]["enabled"]) == 0


def test_export_toggles(empty_s3_source):
    client = TEST_SINGLETON.ctx.magritte_coordinator

    response = client.api_update_export_state_for_source(source_rid=empty_s3_source, is_enabled=True)
    assert response.status_code == 200
    assert response.json() == {"isEnabled": True, "exportableMarkings": [], "isEnabledWithoutMarkingsValidation": False}

    response = client.api_add_exportable_markings_for_source(
        source_rid=empty_s3_source, exportable_markings=[MARKING_ID]
    )
    assert response.status_code == 200
    assert response.json() == {"exportableMarkingsAdded": [MARKING_ID], "exportableMarkingsLackingPermissions": []}

    response = client.api_remove_exportable_markings_for_source(
        source_rid=empty_s3_source, markings_to_remove=[MARKING_ID]
    )
    assert response.status_code == 200
    assert response.json() == {"isEnabled": True, "exportableMarkings": [], "isEnabledWithoutMarkingsValidation": False}

    response = client.api_update_export_state_for_source(source_rid=empty_s3_source, is_enabled=False)
    assert response.status_code == 200
    assert response.json() == {
        "isEnabled": False,
        "exportableMarkings": [],
        "isEnabledWithoutMarkingsValidation": False,
    }


def test_snowflake():
    client = TEST_SINGLETON.ctx.magritte_coordinator
    # Create a new S3 Direct source
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "fdt-test-snowflake_" + rnd
    account_identifier = "account1"
    source_rid = client.create_snowflake_source(
        name=name, parent_rid=INTEGRATION_TEST_COMPASS_ROOT_RID, account_identifier=account_identifier
    )

    # enable OIDC
    client.enable_snowflake_external_oauth(source_rid=source_rid)

    # Set Network Egress
    client.set_network_egress_policies(
        source_rid=source_rid,
        network_egress_policies={INTEGRATION_TEST_EGRESS_POLICY_RID},
    )
    runtime_platform = client.api_get_runtime_platform(source_rid=source_rid)
    assert runtime_platform.status_code == 200
    as_json = runtime_platform.json()
    assert as_json["cloud"]["networkEgressPolicies"]["networkEgressPolicies"] == [INTEGRATION_TEST_EGRESS_POLICY_RID]

    oidc_issuer = client.get_oidc_issuer()
    assert as_json["type"] == "cloud"
    assert as_json["cloud"]["oidcRuntime"] == {
        "audience": f"https://{account_identifier}.snowflakecomputing.com",
        "issuer": oidc_issuer,
        "subject": source_rid,
    }

    client.disable_snowflake_external_oauth(source_rid=source_rid)

    runtime_platform = client.api_get_runtime_platform(source_rid=source_rid)
    assert runtime_platform.status_code == 200
    as_json = runtime_platform.json()
    assert as_json["cloud"]["oidcRuntime"] is None
    assert as_json["cloud"]["networkEgressPolicies"]["networkEgressPolicies"] == [INTEGRATION_TEST_EGRESS_POLICY_RID]

    config = client.get_source_config(source_rid=source_rid)

    assert config["source"]["config"]["auth"] == {
        "basic": {"username": "", "password": "{{SNOWFLAKE_BASIC_AUTH_PASSWORD}}"},
        "type": "basic",
    }

    TEST_SINGLETON.ctx.compass.api_delete_permanently(rids={source_rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"})
