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
TRANSLATOR_VERSION = ../oncoglue/tidywigits-schema-translator/schema/tidywigits/0.0.7.9005/2025091002d1f664

ALIGNMENTS_SCHEMA = models/dcl/raw_vault/alignments/_schema.yml
up-schema-alignments:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/alignments*.yml > $(ALIGNMENTS_SCHEMA)
	@yq -i -I2 -P -N '.' $(ALIGNMENTS_SCHEMA)

AMBER_SCHEMA = models/dcl/raw_vault/amber/_schema.yml
up-schema-amber:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/amber*.yml > $(AMBER_SCHEMA)
	@yq -i -I2 -P -N '.' $(AMBER_SCHEMA)

BAMTOOLS_SCHEMA = models/dcl/raw_vault/bamtools/_schema.yml
up-schema-bamtools:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/bamtools*.yml > $(BAMTOOLS_SCHEMA)
	@yq -i -I2 -P -N '.' $(BAMTOOLS_SCHEMA)

CHORD_SCHEMA = models/dcl/raw_vault/chord/_schema.yml
up-schema-chord:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/chord*.yml > $(CHORD_SCHEMA)
	@yq -i -I2 -P -N '.' $(CHORD_SCHEMA)

CIDER_SCHEMA = models/dcl/raw_vault/cider/_schema.yml
up-schema-cider:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/cider*.yml > $(CIDER_SCHEMA)
	@yq -i -I2 -P -N '.' $(CIDER_SCHEMA)

COBALT_SCHEMA = models/dcl/raw_vault/cobalt/_schema.yml
up-schema-cobalt:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/cobalt*.yml > $(COBALT_SCHEMA)
	@yq -i -I2 -P -N '.' $(COBALT_SCHEMA)

CUPPA_SCHEMA = models/dcl/raw_vault/cuppa/_schema.yml
up-schema-cuppa:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/cuppa*.yml > $(CUPPA_SCHEMA)
	@yq -i -I2 -P -N '.' $(CUPPA_SCHEMA)

LILAC_SCHEMA = models/dcl/raw_vault/lilac/_schema.yml
up-schema-lilac:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/lilac*.yml > $(LILAC_SCHEMA)
	@yq -i -I2 -P -N '.' $(LILAC_SCHEMA)

LINX_SCHEMA = models/dcl/raw_vault/linx/_schema.yml
up-schema-linx:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/linx*.yml > $(LINX_SCHEMA)
	@yq -i -I2 -P -N '.' $(LINX_SCHEMA)

NEO_SCHEMA = models/dcl/raw_vault/neo/_schema.yml
up-schema-neo:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/neo*.yml > $(NEO_SCHEMA)
	@yq -i -I2 -P -N '.' $(NEO_SCHEMA)

PEACH_SCHEMA = models/dcl/raw_vault/peach/_schema.yml
up-schema-peach:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/peach*.yml > $(PEACH_SCHEMA)
	@yq -i -I2 -P -N '.' $(PEACH_SCHEMA)

PURPLE_SCHEMA = models/dcl/raw_vault/purple/_schema.yml
up-schema-purple:
	@# Deep-merge all files together and append their internal array lists instantly
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/purple*.yml > $(PURPLE_SCHEMA)
	@# Enforce clean 2-space indentation layout formatting
	@yq -i -I2 -P -N '.' $(PURPLE_SCHEMA)

SAGE_SCHEMA = models/dcl/raw_vault/sage/_schema.yml
up-schema-sage:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/sage*.yml > $(SAGE_SCHEMA)
	@yq -i -I2 -P -N '.' $(SAGE_SCHEMA)

SIGS_SCHEMA = models/dcl/raw_vault/sigs/_schema.yml
up-schema-sigs:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/sigs*.yml > $(SIGS_SCHEMA)
	@yq -i -I2 -P -N '.' $(SIGS_SCHEMA)

TEAL_SCHEMA = models/dcl/raw_vault/teal/_schema.yml
up-schema-teal:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/teal*.yml > $(TEAL_SCHEMA)
	@yq -i -I2 -P -N '.' $(TEAL_SCHEMA)

VIRUSBREAKEND_SCHEMA = models/dcl/raw_vault/virusbreakend/_schema.yml
up-schema-virusbreakend:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/virusbreakend*.yml > $(VIRUSBREAKEND_SCHEMA)
	@yq -i -I2 -P -N '.' $(VIRUSBREAKEND_SCHEMA)

VIRUSINTERPRETER_SCHEMA = models/dcl/raw_vault/virusinterpreter/_schema.yml
up-schema-virusinterpreter:
	@yq eval-all '. as $$item ireduce ({}; . *+ $$item)' $(TRANSLATOR_VERSION)/dcl/virusinterpreter*.yml > $(VIRUSINTERPRETER_SCHEMA)
	@yq -i -I2 -P -N '.' $(VIRUSINTERPRETER_SCHEMA)

# keep this target last
up-schema-all: \
	up-schema-alignments \
	up-schema-amber \
	up-schema-bamtools \
	up-schema-chord \
	up-schema-cider \
	up-schema-cobalt \
	up-schema-cuppa \
	up-schema-lilac \
	up-schema-linx \
	up-schema-neo \
	up-schema-peach \
	up-schema-purple \
	up-schema-sage \
	up-schema-sigs \
	up-schema-teal \
	up-schema-virusbreakend \
	up-schema-virusinterpreter
