# OncoVault

For project documentation, please refer to https://github.com/umccr/orcahouse-doc

## Development

Create a Python virtual environment (any method) and install the dev toolchain [requirements](requirements.txt).

See [README_DEV.md](README_DEV.md) for more _comprehensive_ setup details.

```
conda activate oncovault
make install
make check
```

We need an authenticated AWS session as we are developing against the remote OncoVault dev environment.

Use your usual AWS CLI setup to authenticate.

For example:
```
export AWS_PROFILE=unimelb-warehouse-prod-poweruser
aws sso login
```

Make the env file.
```
make env
```

Run the dbt debug command to check the connection.
```
dbt debug
```

You should expect to see a successful connection.
```
<...>
00:41:41  Registered adapter: athena=1.10.1
00:41:43    Connection test: [OK connection ok]

00:41:43  All checks passed!
```

## dbt

The dbt has multiple targets. You can `dbt --help` for more details.

```
dbt debug
dbt clean
dbt deps
dbt compile
dbt build
dbt seed
dbt run
dbt test
```

You can run a specific model by name.
```
dbt run -s hub_workflow_run
```

Running the incremental model won't be updated upon the consecutive run. 
You can pass `--full-refresh` flag to reload from the beginning all over again.
BUT. Doing so will lose the model's historical `load_datetime` history.
```
dbt run -s hub_workflow_run --full-refresh
```

## Athena

* Login to the Data Warehouse AWS account console using `AWSPowerUserAccess` role.
* Navigate to the Athena QueryEditor.
* Select the `Workgroup: development`.
* Select the `Data source: AwsDataCatalog`.
* Select the `Database: oncovault_dev_dcl`.
