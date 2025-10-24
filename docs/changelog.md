# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog],
and this project adheres to [Semantic Versioning].

## [2.1.20] - 2025-10-24

## Added
- add new extract/sync creation for MagritteCoordinatorClient. (#109)
- add extract/sync deletion for MagritteCoordinatorClient. (#110)

## [2.1.19] - 2025-07-14

## Added 
- added api_remove_jobspecs and api_get_jobspec_for_dataset for Build2Client (#105)

## [2.1.18] - 2025-06-23

## Added 
- add auth_header to TransformContext and support ctx in lighweight (#100)
- support polars dataframe in CachedFoundryClient.save_dataset (#103)
- added api_get_table_schema for TableClient (#104)

## [2.1.17] - 2025-05-19

## Added 
- Use same requests session in oauth2 token provider as well. (#99)

## [2.1.16] - 2025-05-16

## Added 
- add multipass API to delete Users. (#96)
- bring your own requests.Session() (#98)

## Fixed
- update deploy-pages version to v4	

## [2.1.15] - 2024-12-18

## Added
- Manage Virtual Tables & associated S3 / Snowflake Source (#92)
- Adding initial implementation of SchedulerClient (#93)
- added search to multipass (#94)

## [2.1.14] - 2024-12-18

## Fixed
- establish direct read for full dataset queries in Dataset class (#91)

## [2.1.13] - 2024-12-16

## Fixed
- fix require consent param name, should be `requireConsent` instead of require_consent (#89)

## [2.1.12] - 2024-12-05

## Fixed
- fixed pagination in `list_files` (#88)

## [2.1.11]

## Fixed
- `save_dataframe` incorrectly interacting with optional modules pandas or polars

## [2.1.10] - 2024-10-31

## Changed
  - Typing for callable in transform decorators (#78)
  - Support polars in Dataset class (#81)

## Added
  - Update project roles via compass-API (#80)
  - Add error message to SQL Exception (#82)
  
## [2.1.9] - 2024-10-08

## Changed
  - Allow direct import of `Transform` from `transforms.api`

## [2.1.8] - 2024-09-25

## Fixed
  - fix Resource class can tolerate new Compass JSON Properties (#75)

## [2.1.7] - 2024-09-16

## Fixed
  - fix uninitialized branch attribute for Dataset class (#73)

## [2.1.6] - 2024-09-13

## Added:
  - Retry request 3 times on ConnectionError
## Fixed
  - quote repository name in git clone url
  - fix printing of Datasets instantiated by ctx.get_resource(rid)

## [2.1.5] - 2024-08-30

## Fixed
  - properly pass scopes to multipass endpoint in client_credentials flow

## [2.1.4] - 2024-08-30

## Added
  - check current working directory too in find_project_config_file

## [2.1.3] - 2024-08-28

## Added
  - more API endpoints for MultipassClient for managing groups (https://emdgroup.github.io/foundry-dev-tools/api/foundry_dev_tools.clients.multipass.html#foundry_dev_tools.clients.multipass.MultipassClient)
  - extending Build2Client by API endpoint to submit builds (https://emdgroup.github.io/foundry-dev-tools/api/foundry_dev_tools.clients.build2.html#foundry_dev_tools.clients.build2.Build2Client)
  - object oriented multipass helper classes such as Group, User and Principal (https://emdgroup.github.io/foundry-dev-tools/api/foundry_dev_tools.helpers.multipass.html)

## [2.1.2] - 2024-08-23

## Fixed

- empty token provider config yielded warnings for the app service token provider (#68) 

## [2.1.1] - 2024-08-19

## Fixed
  - app service regression in 2.1.0

## [2.1.0] - 2024-08-15

## Added
  - simplified token provider configuration and improved documentation (#65)

## [2.0.1] - 2024-08-12

## Added
  - better __repr__ for Resource objects.

## Fixed
 - remove backports-datetime-isoformat, as it is not available on osx-arm64 via conda-forge (#64) 

## [2.0.0] - 2024-08-05

## Added
  - full rewrite of Foundry DevTools
  - FoundryRestClient and CachedFoundryClient are still there for backwards compatibility
  - new:
    - configuration format: we now use toml, also with the option to use configuration 'profiles' (https://emdgroup.github.io/foundry-dev-tools/configuration.html)
    - object oriented Dataset/Resource classes (https://emdgroup.github.io/foundry-dev-tools/examples/dataset.html)
    - FoundryContext, makes it easier to discover and use all the API methods/clients (https://emdgroup.github.io/foundry-dev-tools/getting_started/foundry_dev_tools.html#foundrycontext)
    - git credential helper for foundry repositories (https://emdgroup.github.io/foundry-dev-tools/api/foundry_dev_tools.cli.git_credential_foundry.html#module-foundry_dev_tools.cli.git_credential_foundry)
    - if you want to run local transforms, you need to install an extra package (https://pypi.org/project/foundry-dev-tools-transforms)
    - and more! (added typing, git cli, more API endpoints available, made Foundry DevTools extendable via entry points)

## [1.5.0] - 2024-07-31

## Added
  - Official Streamlit Headers method since Streamlit Version 1.37.0 (#58)

## [1.4.3] - 2024-07-11

## Fixed
  - Also check current working directory for project specific config, useful for Jupyter notebooks

## [1.4.2] - 2024-06-03

## Added
  - Support for git submodules in transforms (#57)

## [1.4.1] - 2024-05-29

## Added
  - Support for git submodules (#56)

## [1.4.0] - 2024-02-05

## Added

- support for lightweight transforms (#47)

## Fixed

- PySpark link in installation docs (#48)

## [1.3.5] - 2023-12-05

## Fixed

- `fdt build` escape check/build logs, previously only build job logs were escaped

## [1.3.4] - 2023-11-29

## Fixed

- `fdt build` throws exception when user logs with format other than %s (e.g. %d) #41

## [1.3.3] - 2023-11-02

## Added

- allow to set consent flag when enabling third party application. (#40)

## [1.3.2] - 2023-10-31

## Fixed

- version in UA (#39)

## [1.3.1] - 2023-10-30

### Changed

- allow passing of "None" into scope to not restrict the Oauth2 (#38)

## [1.3] - 2023-10-04

### Added

- S3 cli helper and boto3 methods for the S3 compatible dataset API (#32)

### Fixed

- typos in docs (#30)
- pandas test (#31)
- windows specific test errors (#33)
- use absolute python path in aws config (#35)

## [1.2] - 2023-07-05

### Added
- `fdt info` command (#28)

### Fixed
- foundry dataset view support (#27)

## [1.1] - 2023-06-12

### Added

- foundry-dev-tools build CLI (#22) (#23)

## [1.0.12] - 2023-06-01

### Added
- SQLReturnType enum (#21)

### Changed
- move to arrow_v1 for sql queries (#21)

### Removed
- FoundrySQLClient (#21)

## [1.0.11] - 2023-05-23

### Changed
- better SQL exceptions (#20)

### Fixed
- pip installation documentation for zsh users (#19)

## [1.0.10] - 2023-05-16

### Changed

- use ruff and black instead of pylint and ufmt (#15)
- converted the subprocess calls with git to python only imitations (#15)

### Fixed

- environment variables didn't take precedence (#15)

## [1.0.9] - 2023-04-27

### Fixed

- file upload for files greater than 2GB (#16)

## [1.0.8] - 2023-04-17

### Added

- python 3.11 support, as pyspark 3.4 gained support for it (#13)
- conda-forge badges to the README (#13)

### Fixed

- typo in docs (#12)

### Removed

- pandas<2 restriction, as pyspark 3.4 supports it (#13)

## [1.0.7] - 2023-04-05

### Added

- skip_instance_cache kwarg to FoundryFileSystem, which gets passed to fsspec
  it should be set to True in a multithreaded environment (e.g. Streamlit),
  as the same filesystem instance gets reused by default,
  which resulted in weird behaviour. (#11)
- an extra check, which may prevent sending a bad request to foundry
  when skip_instance_cache is not used (#11)

### Changed

- remove pandas from top level import, which should speed up the initial import (#11)
- restrict pandas to version less than 2,
  as pyspark is currently not compatible with version 2 (#11)

## [1.0.6] - 2023-03-31

### Changed

- Include dataset_rid in DatasetAlreadyExistsError to make it easier to consume. (#10)

## [1.0.5] - 2023-03-24

### Fixed

- Use gatekeeper:view-resource over compass:view (#8)

## [1.0.4] - 2023-03-20

### Fixed

- Better exception handling when git executable is missing (#6)

## [1.0.3] - 2023-03-07

### Added

- tracker and changelog url to setup.cfg (#4)
- more pypi classifiers to setup.cfg (#4)

### Changed

- Changelog is now in the format of [Keep a Changelog] (#4)
- Updated fsspec example in README (#3)

### Fixed

- README code block highlighting and Apache License link (#2)
- wrong styling of pipy batch in readme (#1)

## [1.0.2] - 2023-02-28

### Changed

- Minor documentation and README changes

## [1.0.1] - 2023-02-28 [YANKED]

### Added

- pypi shield to readme
- version classifiers to the setup.cfg

### Fixed

- relative links in readme

## [1.0] - 2023-02-28 [YANKED]

- First public Open Source Release of Foundry DevTools.

[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

[2.1.19]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.18...v2.1.19
[2.1.18]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.17...v2.1.18
[2.1.17]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.16...v2.1.17
[2.1.16]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.15...v2.1.16
[2.1.15]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.14...v2.1.15
[2.1.14]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.13...v2.1.14
[2.1.13]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.12...v2.1.13
[2.1.12]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.11...v2.1.12
[2.1.11]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.10...v2.1.11
[2.1.10]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.9...v2.1.10
[2.1.9]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.8...v2.1.9
[2.1.8]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.7...v2.1.8
[2.1.7]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.6...v2.1.7
[2.1.6]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.5...v2.1.6
[2.1.5]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.4...v2.1.5
[2.1.4]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.3...v2.1.4
[2.1.3]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.2...v2.1.3
[2.1.2]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.1...v2.1.2
[2.1.1]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.0.1...v2.1.0
[2.0.1]: https://github.com/emdgroup/foundry-dev-tools/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.5.0...v2.0.0
[1.5.0]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.4.3...v1.5.0
[1.4.3]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.5...v1.4.0
[1.3.5]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.4...v1.3.5
[1.3.4]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.3...v1.3.4
[1.3.3]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3...v1.3.1
[1.3]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.2...v1.3
[1.2]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.1...v1.2
[1.1]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.12...v1.1
[1.0.12]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.11...v1.0.12
[1.0.11]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.10...v1.0.11
[1.0.10]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.9...v1.0.10
[1.0.9]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.8...v1.0.9
[1.0.8]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/emdgroup/foundry-dev-tools/releases/tag/v1.0.2
[1.0.1]: https://github.com/emdgroup/foundry-dev-tools
[1.0]: https://github.com/emdgroup/foundry-dev-tools
