# CLI

The required dependencies for the CLI can be installed with `pip install 'foundry-dev-tools[cli]'`.

## The info command

This is a quick way to find out, if your environment is correctly setup to use Foundry DevTools.
This also helps us, if you attach it to the issue, so we know what exactly your environment is.

Example, how it looks in my environment:
```shell
fdt info
Foundry DevTools Information
├── Python
│   └── ╭───────────────────────────────────────────────────────────────────────────────────────────────────╮
│       │ Python v3.11.4 | packaged by conda-forge ..., CPython                                             │
│       │ Foundry DevTools v1.2                                                                             │
│       │ using conda ✔ (foundry-dev-tools-dev)                                                             │
│       ╰───────────────────────────────────────────────────────────────────────────────────────────────────╯
├── Spark ✔
│   └── ╭─────────────────────────────────╮
│       │ PySpark v3.4.1 installed ✔      │
│       │ Spark  v3.4.1 installed ✔       │
│       │ Spark and PySpark version match │
│       ╰─────────────────────────────────╯
├──  Java ✔
│   └── ╭───────────────────────────────────────────────╮
│       │ Java Runtime Name:OpenJDK Runtime Environment │
│       │ Java Runtime Version: 20+36                   │
│       ╰───────────────────────────────────────────────╯
├──  System Information
│   └── ╭─────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────╮
│       │ OS              │ Darwin                                                                                           │
│       │ OS release      │ ...                                                                                              │
│       │ OS version      │ Darwin Kernel Version ...                                                                        │
│       │ Instruction set │ x86_64                                                                                           │
│       ╰─────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────╯
├──  Dependencies
│   ├── core ✔
│   │   ├── pyarrow v12.0.1 installed ✔
│   │   ├── pandas v2.0.3 installed ✔
│   │   ├── requests v2.31.0 installed ✔
│   │   ├── fs v2.4.16 installed ✔
│   │   ├── backoff v2.2.1 installed ✔
│   │   └── palantir-oauth-client v1.5.6 installed ✔
│   ├── cli ✔
│   │   ├── click v8.1.3 installed ✔
│   │   ├── inquirer v2.7.0 installed ✔
│   │   ├── websockets v11.0.3 installed ✔
│   │   └── rich v13.4.2 installed ✔
│   ├── integration ✔
│   │   ├── dask v2023.6.1 installed ✔
│   │   └── fastparquet v2023.7.0 installed ✔
│   ├── testing ✔
│   │   ├── pytest v7.4.0 installed ✔
│   │   ├── pytest-mock v3.11.1 installed ✔
│   │   ├── pytest-spark v0.6.0 installed ✔
│   │   ├── requests-mock v1.11.0 installed ✔
│   │   ├── fsspec v2023.6.0 installed ✔
│   │   └── timeflake v0.4.2 installed ✔
│   └── transforms ✔
│       └── pyspark v3.4.1 installed ✔
└── Configuration
    ├── Configuration file: /Users/user/.foundry-dev-tools/config (exists: ✔)
    ├── Project configuration file: x
    └── ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ Config Name                            ┃ Value                                        ┃
        ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ jwt                                    │ Is  set, but not shown for security reasons. │
        │ client_id                              │ Is not set.                                  │
        │ client_secret                          │ Is not set.                                  │
        │ grant_type                             │ authorization_code                           │
        │ scopes                                 │ None                                         │
        │ foundry_url                            │ Is  set, but not shown for security reasons. │
        │ cache_dir                              │ /Users/user/.foundry-dev-tools/cache         │
        │ transforms_output_folder               │ None                                         │
        │ transforms_sql_sample_select_random    │ x                                            │
        │ transforms_force_full_dataset_download │ x                                            │
        │ enable_runtime_token_providers         │ ✔                                            │
        │ transforms_freeze_cache                │ x                                            │
        │ transforms_sql_sample_row_limit        │ 5000                                         │
        │ transforms_sql_dataset_size_threshold  │ 500                                          │
        └────────────────────────────────────────┴──────────────────────────────────────────────┘
```


## The build command

```shell
fdt build [-t transform_file]
```
If you provide a transform file via the -t flag, it will try to execute the checks and the build for that transform file and tail its logs.
If you don't provide a transform file via that flag, it will list the transform files in your last commit and let you choose what transform you want to run.