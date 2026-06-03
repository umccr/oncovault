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

# Usage:
# 	make get-cols INDEX=0 SCHEMA=models/dcl/raw_vault/purple/_schema.yml
# 	make get-cols INDEX=0 SCHEMA=models/dcl/raw_vault/purple/_schema.yml | pbcopy
get-cols:
	@yq '.models[$(INDEX)].columns[].name' $(SCHEMA)

# Usage:
# 	git clone oncoglue at the same level as oncovault
up-schema-purple:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/purple*.yml > models/dcl/raw_vault/purple/_schema.yml

up-schema-cuppa:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/cuppa*.yml > models/dcl/raw_vault/cuppa/_schema.yml

up-schema-amber:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/amber*.yml > models/dcl/raw_vault/amber/_schema.yml

up-schema-cobalt:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/cobalt*.yml > models/dcl/raw_vault/cobalt/_schema.yml

up-schema-chord:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/chord*.yml > models/dcl/raw_vault/chord/_schema.yml

up-schema-alignments:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/alignments*.yml > models/dcl/raw_vault/alignments/_schema.yml

up-schema-cider:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/cider*.yml > models/dcl/raw_vault/cider/_schema.yml

up-schema-lilac:
	@cat ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9003/2025091002d1f664/dcl/lilac*.yml > models/dcl/raw_vault/lilac/_schema.yml
