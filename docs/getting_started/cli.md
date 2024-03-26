# CLI

The required dependencies for the CLI can be installed with `pip install 'foundry-dev-tools[cli]'`.

## The info command

This is a quick way to find out, if your environment is correctly setup to use Foundry DevTools.
This also helps us, if you attach it to the issue, so we know what exactly your environment is.

```shell
fdt info
```

Example, how it looks in my environment:
<!-- https://github.com/executablebooks/MyST-Parser/issues/176 -->
<img src="/pictures/fdt_info_example.svg"/>

## The config command

This is a quick way to see what your config currently is and from which file the config is sourced.
You can also easily launch the config file in an editor.

### View the config and config files

```shell
fdt config
```

<img src="/pictures/fdt_config_example.svg"/>

### Edit a config file

```shell
fdt config -e
```

<img src="/pictures/fdt_config_edit_example.svg"/>


## The build command

```shell
fdt build [-t transform_file]
```
If you provide a transform file via the -t flag, it will try to execute the checks and the build for that transform file and tail its logs.
If you don't provide a transform file via that flag, it will list the transform files in your last commit and let you choose what transform you want to run.

:::{seealso}
[The s3 CLI](/getting_started/s3.md)
:::
