import json
import logging
from pathlib import Path

from graphrag.config.models import (
    GraphRagConfig,
)
from graphrag.index.config.pipeline import (
    PipelineConfig,
)
from graphrag.index.workflows.default_workflows import (
    create_final_covariates,
)
from graphrag.index.create_pipeline_config import _get_pipeline_input_config, _get_reporting_config, _get_storage_config, _get_cache_config, _document_workflows, _text_unit_workflows, _graph_workflows, _community_workflows, _covariate_workflows, _determine_skip_workflows, _get_embedded_fields, _log_llm_settings


from graphy.config.storage_config import StorageType
from graphy.config.cosmos_storage_config import CosmosDBStorageConfig

log = logging.getLogger(__name__)



def create_pipeline_config(settings: GraphRagConfig, verbose=False) -> PipelineConfig:
    """Get the default config for the pipeline."""
    # relative to the root_dir
    if verbose:
        _log_llm_settings(settings)

    skip_workflows = _determine_skip_workflows(settings)
    embedded_fields = _get_embedded_fields(settings)
    covariates_enabled = (
        settings.claim_extraction.enabled
        and create_final_covariates not in skip_workflows
    )

    result = PipelineConfig(
        root_dir=settings.root_dir,
        input=_get_pipeline_input_config(settings),
        reporting=_get_reporting_config(settings),
        storage=_get_graphy_storage_config(settings),
        cache=_get_cache_config(settings),
        workflows=[
            *_document_workflows(settings, embedded_fields),
            *_text_unit_workflows(settings, covariates_enabled, embedded_fields),
            *_graph_workflows(settings, embedded_fields),
            *_community_workflows(settings, covariates_enabled, embedded_fields),
            *(_covariate_workflows(settings) if covariates_enabled else []),
        ],
    )

    # Remove any workflows that were specified to be skipped
    log.info("skipping workflows %s", ",".join(skip_workflows))
    result.workflows = [w for w in result.workflows if w.name not in skip_workflows]
    return result



def _get_graphy_storage_config(
    settings: GraphRagConfig,
) -> any:
    """Get the storage type from the settings."""
    if settings.storage is None:
        msg = "Storage configuration must be provided."
        raise ValueError(msg)
    
    if settings.storage.type == StorageType.cosmos:
        confg = settings.storage.model_dump()
        return CosmosDBStorageConfig(
            connection_string=settings.storage.connection_string, 
            database_name=confg.get('database_name'),
            account_host=confg.get('account_host'),
            account_key=confg.get('account_key')
            )
    else: 
        return _get_storage_config(settings)
        