Looker Gen
==========

Generate LookML from a dbt project.

## Installation
- Clone repo
- Install [Poetry](https://python-poetry.org/docs/)
- Install dependacies `poetry install`
- Optional: [Configure](https://developers.looker.com/api/getting-started) LookerSDK with `looker.ini` file

## Running

### Build dbt project

```
dbt run # builds manifest based on dbt yml
dbt docs generate # builds catalog from database
```

Note: `dbt compile` can replace `run`

### Generate LookML
```
poetry run looker-gen -d '~/dbt-dir' -o '~/looker-dir/' -c connection-name
```

### Optional: Valiate Looker Project
```
poetry run validate -p project -l '~/looker-dir'
```

Note: use `poetry run --help` for options

### DBT Configuration
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
