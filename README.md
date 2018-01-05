# Synopsis

On average 90% of data exploration is spent on prep work. We aim at alleviating this pain by
creating normalized form of our domain objects and make them available for processing using Spark,
creating a clear contract between the Data platform and all the downstream computers.

## Repository contents

A Python based Spark application. This can be run standalone or the egg file can be packaged and made
to run in a hosted Spark Environment.


## How to?

After checking out, create a python virtual environment and run `pip3 install -r requirements.txt`

run `python ./catalog/dao/write_catalog.py`

This reads the data from Postgres follower database by default. You can add additional data-sources,
normalizes the dataset and creates domain model including Candidates, Jobs and Employers and persists
then in S3.

### api

TODO: Databricks Api.


### l10n
The l10n folder contains localization strings that are used in the various
json files throughout this repository.

## Problems?

If you find a problem, please [file an issue](https://github.com/hired/data_catalog/issues/new).

