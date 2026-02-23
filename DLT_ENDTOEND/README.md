# End-to-End Delta Live Tables Pipeline (Databricks)

This project demonstrates an end-to-end Medallion Architecture (Bronze, Silver, Gold) built using Delta Live Tables.


This folder defines all source code for the 'DLT_ENDTOEND' pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

## Pipeline Architecture
Bronze → Silver → Gold

## Features
- Streaming ingestion
- SCD transformations
- Incremental processing
- Data quality checks using DLT expectations

## Tech Stack
Databricks | Delta Live Tables | PySpark | Delta Lake