"""S3Client for the S3 compatible dataset API."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from foundry_dev_tools.errors.handling import raise_foundry_api_error
from foundry_dev_tools.utils.misc import parse_iso

if TYPE_CHECKING:
    import aiobotocore
    import boto3
    import requests

    from foundry_dev_tools.config.context import FoundryContext


class S3Client:
    """The S3 compatible dataset API."""

    def __init__(self, context: FoundryContext):
        self.context = context
        self._credentials: dict | None = None
        self._credentials_expiration = 0.0

    def get_url(self) -> str:
        """Return the s3 endpoint url."""
        return self.context.host.url + "/io/s3"

    def get_s3fs_storage_options(self) -> dict:
        """Get the foundry s3 credentials in the s3fs storage_options format.

        Example:
            >>> ctx = FoundryContext()
            >>> storage_options = ctx.s3.get_s3fs_storage_options()
            >>> df = pd.read_parquet(
            ...     "s3://ri.foundry.main.dataset.<uuid>/spark", storage_options=storage_options
            ... )
        """
        return {
            "session": self._get_aiobotocore_session(),
            "endpoint_url": self.get_url(),
        }

    def get_polars_storage_options(self) -> dict:
        """Get the foundry s3 credentials in the format that polars expects.

        https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html

        Example:
            >>> ctx = FoundryContext()
            >>> storage_options = ctx.s3.get_polars_storage_options()
            >>> df = pl.read_parquet(
            ...     "s3://ri.foundry.main.dataset.<uuid>/**/*.parquet", storage_options=storage_options
            ... )
        """
        credentials = self.get_credentials()
        return {
            "aws_access_key_id": credentials["access_key"],
            "aws_secret_access_key": credentials["secret_key"],
            "aws_session_token": credentials["token"],
            "aws_region": "foundry",
            "aws_endpoint": self.get_url(),
            "aws_virtual_hosted_style_request": "false",
        }

    def get_duckdb_create_secret_string(self) -> str:
        """Returns a CREATE SECRET SQL String with Foundry Configuration.

        https://duckdb.org/docs/extensions/httpfs/s3api.html#config-provider

        Example:
            >>> ctx = FoundryContext()
            >>> con.execute(ctx.s3.get_duckdb_create_secret_string())
            >>> df = con.execute(
            ...     "SELECT * FROM read_parquet('s3://ri.foundry.main.dataset.<uuid>/**/*.parquet') LIMIT 1;"
            ... ).df()
        """
        credentials = self.get_credentials()
        endpoint = self.context.host.domain + "/io/s3"
        return """
                CREATE OR REPLACE SECRET foundryConnection (
                    TYPE S3,
                    KEY_ID '{access_key}',
                    SECRET '{secret_key}',
                    SESSION_TOKEN '{token}',
                    ENDPOINT '{endpoint}',
                    URL_STYLE 'path',
                    REGION 'foundry'
                );
                """.format(**credentials, endpoint=endpoint)

    def _get_boto3_session(self) -> boto3.Session:
        """Returns the boto3 session with foundry s3 credentials applied.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.
        """
        import boto3
        import botocore.session

        from foundry_dev_tools.utils.s3 import CustomFoundryCredentialProvider

        session = botocore.session.Session()
        session.set_config_variable("region", "foundry")
        cred_provider = session.get_component("credential_provider")
        cred_provider.insert_before("env", CustomFoundryCredentialProvider(self, session))
        return boto3.Session(botocore_session=session)

    def _get_aiobotocore_session(self) -> aiobotocore.AioSession:
        """Returns an aiobotocore session with foundry s3 credentials applied.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.
        """
        import aiobotocore.session

        from foundry_dev_tools.utils.async_s3 import (
            CustomAsyncFoundryCredentialProvider,
        )

        session = aiobotocore.session.AioSession()
        session.set_config_variable("region", "foundry")
        cred_provider = session.get_component("credential_provider")
        cred_provider.insert_before("env", CustomAsyncFoundryCredentialProvider(self, session))
        return session

    def get_boto3_client(self, **kwargs):  # noqa: ANN201
        """Returns the boto3 s3 client with credentials applied and endpoint url set.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.

        Example:
            >>> from foundry_dev_tools import FoundryContext
            >>> ctx = FoundryContext()
            >>> s3_client = ctx.s3.get_boto3_client()
            >>> s3_client
        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.client`, `endpoint_url` will be overwritten
        """
        kwargs["endpoint_url"] = self.get_url()
        return self._get_boto3_session().client("s3", **kwargs)

    def get_boto3_resource(self, **kwargs):  # noqa: ANN201
        """Returns boto3 s3 resource with credentials applied and endpoint url set.

        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.resource`, `endpoint_url` will be overwritten
        """
        kwargs["endpoint_url"] = self.get_url()
        return self._get_boto3_session().resource("s3", **kwargs)

    def get_credentials(self, expiration_duration: int = 3600) -> dict:
        """Parses the AssumeRoleWithWebIdentity response and caches the credentials.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.
        """
        if (
            self._credentials is None
            or time.time() > self._credentials_expiration
            or (
                (t := self._credentials_expiration - time.time()) > expiration_duration
            )  # if the expiration duration is larger than the requested expiration
            or (t < 900)  # less than 15 minutes, recommended by the boto library.
        ):
            from foundry_dev_tools.utils.s3 import parse_s3_credentials_response

            self._credentials = parse_s3_credentials_response(
                self.api_assume_role_with_webidentity(expiration_duration).text,
            )
            self._credentials_expiration = parse_iso(self._credentials["expiry_time"]).timestamp()
        return self._credentials

    def api_assume_role_with_webidentity(self, expiration_duration: int = 3600) -> requests.Response:
        """Calls the AssumeRoleWithWebIdentity API to get temporary S3 credentials.

        Args:
            expiration_duration: seconds the credentials should be valid, defaults to 3600 (the upper bound)
        """
        # does not call the api_request method, as this is not a regular api
        resp = self.context.client.request(
            "POST",
            self.get_url(),
            params={
                "Action": "AssumeRoleWithWebIdentity",
                "WebIdentityToken": self.context.token_provider.token,
                "DurationSeconds": expiration_duration,
            },
            auth=lambda x: x,  # stub auth function that is normally set by the token provider
        )
        raise_foundry_api_error(resp)
        return resp
