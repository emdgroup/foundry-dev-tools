# CLI

The required dependencies for the CLI can be installed with `pip install 'foundry-dev-tools[cli]'`.

## The build command

```shell
fdt build [-t transform_file]
```
If you provide a transform file via the -t flag, it will try to execute the checks and the build for that transform file and tail its logs.
If you don't provide a transform file via that flag, it will list the transform files in your last commit and let you choose what transform you want to run.