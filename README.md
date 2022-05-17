Looker Gen
==========

Generate LookML from a dbt project. Reads from your dbt repo and outputs files; can output to your Looker repo.

Currently in experimentation mode, very opinionated about output directory structure.

## Installation
- Clone repo
- Install [Poetry](https://python-poetry.org/docs/)
- Install dependacies `poetry install`
- Looker: [Configure](https://developers.looker.com/api/getting-started) LookerSDK with `looker.ini` file
- Verify: `poetry run gen --help`

## Running

### Build dbt project
In your dbt repo:

```
dbt compile # builds manifest based on dbt yml
dbt docs generate # builds catalog from database
```

Note: `dbt run` can replace `compile`. To ensure that LookML columns matches database schema, tables must be created with `dbt run`.

### Generate LookML
`$DBT_DIR` = Directory of dbt repo

`$LOOKER_DIR` = Directory of LookML repo

`$CONNECTION_NAME` = Database connection name defined in Looker. This is require for the `/models` file to generate properly.

```
poetry run looker-gen -d $DBT_DIR -o $LOOKER_DIR -c $CONNECTION_NAME
```

This will output generated files with to output destination with the following structure:

```
LookMLRepo/
├─ explores/
│  ├─ looker-gen.explore.lkml
│  ├─ ....explore.lkml
├─ views/
   ├─ ....view.lkml
```

To use within Looker, simply add this to your `*.models.lkml` file:
```
include: "/explores/looker-gen.explore.lkml"
```


### Optional: Valiate Looker Project
The `poetry run validate` command can validate your LookML repo with Looker's linter ("LookML Validation") and content validation.

`$LOOKER_DIR` = Directory of LookML repo

`$PROJECT` = Name of project within Looker.

```
poetry run validate -p $PROJECT -l $LOOKER_DIR
```

Note: use `poetry run validate --help` for options

## DBT Configuration Overview
Use the `looker-gen` key witn an element's `meta` tag. (Example below)

Mostly, the yml will be passed through to LookML. There are a few protected words that impact behavior:


### Model
- `explore` will generate an `explore` for the model.
- `joins` will create a list of `join`s atteached to the `explore`.

### Column
- `description`: Uses dbt's standard `description` declaration; not referenced in `looker-gen` arguments.
- `ignore-dim`: By default, `looker-gen` will create a `dimension` or `dimension_group` for every column within the table. This prevent the column from creating a dimension or dimension group.
- `looker-only`: Defines a column in LookML, without an existing column in the database. Useful for derived dimensions, calculations, etc.
- `column-type`: If defined as `looker-only`, will determine LookML column type. Currently only supports `dim` or `dimension` values.
- `measures`: Builds an array of measures.
- Will generate `count` measure for all views.
- Will generate a formatted `view_label` by default for all views.

## DBT Configuration Example
```
version: 2

models:
  - name: fct_sales
    description: "All the sales"
    meta:
      looker-gen:
        explore:
          joins:
            - name: dim_customers
              sql_on: ${fct_sales.customer_id} = ${dim_customers.id}
              type: left_outer
              relationship: many_to_one
    columns:
        - name: revenue
          meta:
            looker-gen:
              ignore-dim: 'yes'
              measures:
                - name: total_revenue
                  type: sum
                  value_format: "$#.0;($#.00)"


    - name: dim_customers
      description: "All the customers"
      meta:
        looker-gen:
          group_label: "People"
      columns:
        - name: id
          description: "Primary Key"
          meta:
            looker-gen:
              primary_key: 'yes'
              measures:
                - name: unique_customers
                  type: count_distinct
    
```
