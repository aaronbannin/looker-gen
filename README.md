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
