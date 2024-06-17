# TL;DR of changes between v1 and v2

# Clients

## In v1
We basically had two 'clients' the FoundryRestClient and the CachedFoundryClient.

## In v2
Each Foundry API endpoint is now its own Client

# Configuration in Python
## v1

We had a global configuration object which was used by all FoundryRestClient instances and copied over on creation.

## v2

We have a FoundryContext, which loads the configuration from ENV variables and Configuration files. The API clients then have access to the configuration from the FoundryContext.
You can also have different Configurations in you configuration files through 'profiles', the profile name can be passed to the FoundryContext so it loads that profile instead of the 'default' config

## Config/Cache location

## v1

Configuration and cache was located at ~/.foundry-dev-tools/cache and config, the configuration file used an 'ini' like format.

## v2

The cache is located in a directory which depends on the Operating System (we use the platformdirs library for this) e.g. on MacOS `~/Library/Caches/foundry-dev-tools`, Windows: `C:\\Users\\trentm\\AppData\\Local\\foundry-dev-tools\\Cache`, Linux: `~/.cache/foundry-dev-tools`
The configuration is now done in TOML.
The configuration file can be placed at multiple places in the 'hirarchy' and will be merged. Run `fdt config` on your machine after installing foundry-dev-tools to see the configuration locations.
Locations that can always used, no matter the OS, are `~/.foundry-dev-tools/config.toml` or `~/.config/foundry-dev-tools/config.toml`

## Env Variables

## v1

The env variables had the format FOUNDRY_DEV_TOOLS_<config_option>

## v2

The env variables are prefixed with `FDT_`. For example to set the config option located at `credentials.token_provider.name`, you'd use `FDT_CREDENTIALS__TOKEN_PROVIDER__NAME`.
It will be parsed by first removing the prefix, and then splitting the rest of the string by "__", this was necessary to allow the usage of nesting in toml and using config options which have underscores in their name.

# Project configuration
## v1

The project config was located at `.foundry_dev_tools`

## v2

The project config is located at `.foundry_dev_tools.toml`


# Transforms
## v1

The `transforms` package was included in the `foundry-dev-tools` package, which created conflicts when used on Foundry itself

## v2

The `transforms` package is now a seperate package `foundry-dev-tools-transforms` which only includes `transforms`. And the `foundry-dev-tools` package only includes the Foundry DevTools API clients.


# Compatibility

`FoundryRestClient` and `CachedFoundryClient` are still included, and are wrappers around the new v2 clients.
We tried to keep both compatible with the `v1` implementation.

We provide a `fdt config migrate` command that migrates your current configuration to the new v2 configuration format, you'll be able to choose in which directory this should be saved.
We also provide a `fdt config migrate-project` command that migrates your project configuration to the new v2 configuration format.
The old configuration will be kept in place for both commands, so you'll be able to use v1 in other projects in parallel.

# Recommendations for v2

The `fdt config` command lets you check your current configuration.
The `fdt config edit` command lets you easily open your configuration in your default editor.
The `fdt info` command checks if your environment is correctly set up.

We also provide a git-credential helper for foundry, this authenticates you to foundry when using git, which eliminates the need to have your token and username stored in every git repository you clone.
After installing Foundry DevTools, run `git-credential-foundry` which will show you the needed steps to set it up.

As the `FoundryContext` loads your configuration when creating a new Instance as the client instances are also cached per FoundryContext instance, it is recommend to only use one FoundryContext in your code.

The v2 is extensible, this way you can create your own Foundry clients and Token Providers easily as if they were integrated into Foundry DevTools.

# Installation

You'll need python 3.10 or newer, as some libraries we use stopped support <3.10

Currently as it is not in the main branch and not released on pypi, the installation would be as follows:

1. Create a new conda/python environment (recommended to start the env from a clean state, as this may lead to fewer problems)
2. To install the API clients and CLI, i.e the foundry-dev-tools package, run: `pip install "git+https://github.com/emdgroup/foundry-dev-tools/@v2#subdirectory=libs/foundry-dev-tools"`
3. (Optional) To also install the transforms implementation run `pip install "git+https://github.com/emdgroup/foundry-dev-tools/@v2#subdirectory=libs/transforms"`
