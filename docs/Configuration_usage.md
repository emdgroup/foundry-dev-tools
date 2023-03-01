# Configuration

By default, the code loads the configuration from the `~/.foundry-dev-tools/config` file.  
If a project [specific config file](#Project-specific-configuration) exists in `your-project-dir/.foundry_dev_tools`, it takes precedence.  
If a environment variable like `FOUNDRY_DEV_TOOLS_config_key` exists, `config_key` gets overwritten by this environment variable.  
Afterwards, the logic falls back to a default value: ( overwrites > environment variable > project config file > config file > default)  

In the standard setup, the configuration should contain either the Foundry Token (`jwt`) or the Foundry Client
ID (`client_id`) for SSO.

## Configuration options

| Name                                   | Description                                                                                                                                       | Values                                 | 
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| foundry_url                            | Url of the foundry instance.                                                                                                                      | e.g. https://foundry.example.com       |
| jwt                                    | The Bearer Token copied from the Foundry Token Page                                                                                               | eyJhb...                               |
| client_id                              | The client_id of the Foundry Third Party application for SSO Authentication.                                                                      | ...                                    |
| client_secret                          | The client_secret of the Foundry Third Party application.                                                                                         | ...                                    |
| grant_type                             | Set this to client_credentials if you use Foundry Third Party application client_credentials flow. Default is authorization_code                  | authorization_code, client_credentials |
| requests_ca_bundle                     | Path to custom ca bundle file, useful in corporate networks with SSL inspection                                                                   | not set                                |
| transforms_sql_sample_row_limit        | Number of rows that are retrieved for each dataset when sql is used                                                                               | 5000                                   | 
| transforms_sql_dataset_size_threshold  | Maximal size of a dataset up to which file gets downloaded. If dataset is larger, sql with limit query will be used to download a partial dataset | 500                                    | 
| transforms_sql_sample_select_random    | True or False, if data is randomly sampled when SQL mode is used (query takes more time)                                                          | True or False                          |
| transforms_force_full_dataset_download | if file_download is True, all files of datasets are downloaded                                                                                    | True or False                          |
| cache_dir                              | Point cache directory to other directory than default                                                                                             | ~/.foundry-dev-tools/cache             |
| transforms_freeze_cache                | Uses offline cache as is; no Foundry connection needed                                                                                            | True or False (default: False)         |
| transforms_output_folder               | When @transform in combination with TransformOutput.filesystem() is used, files are written to this folder.                                       | /projectA/local_files                  |
| enable_runtime_token_providers         | Enables the token providers, Default is True                                                                                                      | True or False                          |

## Changing configuration programmatically

In addition, it is possible to programmatically overwrite or add configuration entries to the global dict with
the following basic interfaces from python code:

```python
from foundry_dev_tools import Configuration

# configuration options are loaded from file ~/.foundry-dev-tools/config
Configuration.get_config()
{'jwt': 'jwt-for-john',
 'transforms_sql_sample_row_limit': 100000,
 'transforms_sql_sample_select_random': True}

# Overwrite once by passing as dict
Configuration.get_config({'transforms_sql_select_random': 'False'})
{'jwt': 'jwt-for-john',
 'transforms_sql_sample_row_limit': 100000,
 'transforms_sql_sample_select_random': False}

# Permanently set config option using set function
Configuration.set('transforms_sql_sample_select_random', False)
Configuration.get_config()
{'jwt': 'jwt-for-john',
 'transforms_sql_sample_row_limit': 100000,
 'transforms_sql_sample_select_random': False}

# The Configuration object can also be used like a dict
Configuration["transforms_sql_sample_row_limit"] = 500
print(Configuration["transforms_sql_sample_row_limit"])
500
for key, value in Configuration.items():
    print(key,value)
jwt jwt-for-john
transforms_sql_sample_row_limit 100000
transforms_sql_sample_select_random False

# When creating a new FoundryRestClient or CachedFoundryClient
# you can also pass an overwrite config to it
from foundry_dev_tools import FoundryRestClient
fc = FoundryRestClient({"jwt":"jwt-for-jane"})

# it will get used by the FoundryRestClient
fc.get_user_info()
{"username":"jane",...}

# but it will not change the global config
fc2 = FoundryRestClient()
fc2.get_user_info()
{"username":"john",...}
```

## Project specific configuration

You can provide a configuration per project at the top level of the git repository in file `.foundry_dev_tools`.
The structure of this file is equivalent to the `~/.foundry-dev-tools/config` file. Entries of the default config are
overwritten with entries from the `.foundry_dev_tools` file.

## Enable Logging

With the following code you can enable the DEBUG logging output of foundry-dev-tools:

```python
import logging
import sys
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logging.getLogger('transforms.api').setLevel(logging.DEBUG)
logging.getLogger('foundry_dev_tools.foundry_api_client').setLevel(logging.DEBUG)
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
```
