install:
	@pre-commit install
	@pre-commit autoupdate
	@pip3 install -r requirements.txt

check:
	@pre-commit run --all-files

scan:
	@trufflehog --debug --only-verified git file://./ --since-commit main --branch HEAD --fail

deep: scan
	@ggshield secret scan repo .

baseline:
	@detect-secrets scan --exclude-files '^(.venv/|.local/|.terraform/|terraform.tfstate.d/|dbt_packages/|logs/)|package-lock.yml' > .secrets.baseline

env:
	@aws ssm get-parameter --name '/oncovault/env/dev' --output text --query 'Parameter.Value' > .env
	@echo env ready

debug:
	@dbt debug -t dev

test:
	@dbt deps
	@dbt test
