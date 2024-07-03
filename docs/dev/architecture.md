# Architecture

The following pages contain information how Foundry DevTools is structured, and how one could extend it.

```{toctree}
:glob:

architecture/*
```

## Known limitations (Contributions Welcome 🤗)

- CSV format settings are not taken over from the Foundry Schema. Advised to use datasets in parquet format.
- transforms Output not written back to foundry
- @incremental not implemented
- @configure not implemented
