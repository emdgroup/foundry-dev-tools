# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog],
and this project adheres to [Semantic Versioning].

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

[1.3.4]: https://github.com/emdgroup/foundry-dev-tools/compare/v1.3.4...v1.3.5
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
