## Examples

#### push_data_to_foundry.py
A simple example on how to use the DataPoxy implementdation of the `FoundryRestClient` to `open a transaction`, `uploading a file` and `committing a transaction`.

The example creates a `FoundryRestClient` with the default configuration, tries to open a new transaction. If a transaction is already open, the client uses the open transaction and pushes the file to the open one. After uploading the file, the current open transaction is committed and closed.
