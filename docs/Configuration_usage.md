# Configuration

Configuration options can be supplied to the library through multiple different methods that are processed in the following order:
  1. Every configuration option is initalized to its default value as given in the [table below](#configuration-options).
  2. If a file `~/.foundry-dev-tools/config` exists, values defined therein overwrite the corresponding default values.
     See [below](#foundry-dev-toolsconfig-file) for details on its structure.
  3. If a project [specific config file](#project-specific-configuration) exists in `your-project-dir/.foundry_dev_tools`, values set in there take precedence.
  4. If an environment variable like `FOUNDRY_DEV_TOOLS_config_key` exists, `config_key` gets overwritten by this environment variable.
     Note, that environment variable names are always all-caps, e.g. the `foundry_url` config value can be set via `export FOUNDRY_DEV_TOOLS_FOUNDRY_URL=https://foundry.example.com`.
  5. Configuration options can be [overwritten programmatically](#changing-configuration-programmatically) during runtime.

Thus, the order of precedence for configuration options is: runtime overwrites > environment variable > project config file > config file > default.

In the standard setup, the configuration should contain either the Foundry Token (`jwt`) or the Foundry Client ID (`client_id`) for SSO.

## Configuration options

| Name                                   | Description                                                                                                                                       | Values                                 | Default Values         |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|------------------------|
| foundry_url                            | Url of the foundry instance                                                                                                                       | e.g. https://foundry.example.com       | not set (None)         |
| jwt                                    | The Bearer Token copied from the Foundry Token Page                                                                                               | eyJhb...                               | not set (None)         |
| client_id                              | The client_id of the Foundry Third Party application for SSO Authentication                                                                       | ...                                    | not set (None)         |
| client_secret                          | The client_secret of the Foundry Third Party application                                                                                          | ...                                    | not set (None)         |
| grant_type                             | Set this to client_credentials if you use Foundry Third Party application client_credentials flow. Default is authorization_code                  | authorization_code, client_credentials | authorization_code     |
| scopes                                 | Custom scopes for Third Party Application Tokens                                                                                                  | "api:read-data offline_access"         | not set (None)         |
| requests_ca_bundle                     | Path to custom ca bundle file, useful in corporate networks with SSL inspection                                                                   | not set                                | `os.path.expanduser()` |
| transforms_sql_sample_row_limit        | Number of rows that are retrieved for each dataset when sql is used                                                                               | 5000                                   | 5000                   |
| transforms_sql_dataset_size_threshold  | Maximum size of a dataset up to which file gets downloaded. If dataset is larger, sql with limit query will be used to download a partial dataset | 500                                    | 500                    |
| transforms_sql_sample_select_random    | True or False, if data is randomly sampled when SQL mode is used (query takes more time)                                                          | True or False                          | False                  |
| transforms_force_full_dataset_download | if file_download is True, all files of datasets are downloaded                                                                                    | True or False                          | False                  |
| cache_dir                              | Point cache directory to other directory than default                                                                                             | ~/.foundry-dev-tools/cache             | `os.path.expanduser()` |
| transforms_freeze_cache                | Uses offline cache as is; no Foundry connection needed                                                                                            | True or False (default: False)         | False                  |
| transforms_output_folder               | When @transform in combination with `TransformOutput.filesystem()` is used, files are written to this folder.                                     | /projectA/local_files                  | `os.path.expanduser()` |
| enable_runtime_token_providers         | Enables the token providers                                                                                                                       | True or False                          | True                   |

## `~/.foundry-dev-tools/config` file
The content of the file `~/.foundry-dev-tools/config` must be compatible with the Python `configparser` library, i.e. it must have a structure similar to what is found in Microsoft Windows INI files.
All configuration options have to reside in a section titled `[default]`.
Content of all other sections is ignored while parsing the file.
Thus, an exemplary `~/.foundry-dev-tools/config` file reads
```
[default]
foundry_url=https://foundry.example.com
jwt=eyJhb...
```

## Project specific configuration

You can provide a project-specific configuration via a project-local file `.foundry_dev_tools`.
This file has to reside in your project's root directory.
The structure of this file is equivalent to the [`~/.foundry-dev-tools/config`](#foundry-dev-toolsconfig-file) file.
Entries of the default config are overwritten with entries from the `.foundry_dev_tools` file as specified [above](#configuration).

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

# Enable Logging

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
