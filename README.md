# Overview

This repo contains utilities to help Wells Fargo Harmony generate usefull metadata

# Setup

## Create Python Virtual Environment

```bash
cd harmony-ddp-public
python -m venv venv
```

## Install Modules

```bash
cd harmony-ddp-public
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
cd harmony-ddp-public
python src/sql_parser.py
```

The script minimally expects the following args:

```bash
--input <path to sql file | path to dir with *.sql files | sql statement>
--output <path to output .csv>
```

There are other optional arguments that you can also pass:

```bash
--database <database type (ie ‘hive’, ‘oracle’, ‘postgres’)>
--cluster <logical grouping of datasets (ie ‘cobra’)>
--schema <database schema (ie ‘credw_cre_curated’)
--table <the name of the table generated from the sql…filename is used if a file is passed>
--source_database <the database type where the source datasets reside (ie ‘oracle’)>
--source_cluster <the logical grouping of datasets in the source database (ie ‘wells_fargo’)>
--dialect <the sqlglot dialect name:
            athena
            bigquery
            clickhouse
            databricks
            doris
            drill
            duckdb
            hive
            materialize
            mysql
            oracle
            postgres
            presto
            prql
            redshift
            risingwave
            snowflake
            spark
            spark2
            sqlite
            starrocks
            tableau
            teradata
            trino
            tsql
            >
```