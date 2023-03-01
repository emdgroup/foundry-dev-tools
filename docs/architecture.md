# Architecture

## How the transforms library that exists on Foundry is stubbed in Foundry DevTools

* Input datasets < 500MB are retrieved with the Foundry Dataproxy File Download API
* Input datasets > 500MB are retrieved as partial subsets with the Foundry Dataproxy SQL API (SELECT * FROM .. LIMIT
  
* Both of these limits can be changed with the corresponding config option
  * `transforms_sql_dataset_size_threshold` size in MB, default 500
  * `transforms_sql_sample_row_limit`, default 5000
* Datasets are cached locally in a folder (`~/.foundry-dev-tools/.cache`)
* Datasets are automatically passed to transform functions
* Output datasets can be visualized using IDE or Notebook functionality
* Output dataset files can be stored to a local folder for inspection
* Foundry `transforms` library is stubbed
* Dataset branch is detected automatically from the local git branch


The following sequence diagrams show  

a) what happens when the `Input` dataset is already cached.
```{image} pictures/mermaid-diagram-already-cached-light.svg
---
class: only-light
---
```
```{image} pictures/mermaid-diagram-already-cached-dark.svg
---
class: only-dark
---
```  

b) what happens when the `Input` dataset has a new transaction  

```{image} pictures/mermaid-diagram-new-transaction-light.svg
---
class: only-light
---
```
```{image} pictures/mermaid-diagram-new-transaction-dark.svg
---
class: only-dark
---
```


### Known limitations (Contributions Welcome 🤗)

* CSV format settings are not taken over from the Foundry Schema. Advised to use datasets in parquet format.
* transforms Output not written back to foundry
* @incremental not implemented
* @configure not implemented
