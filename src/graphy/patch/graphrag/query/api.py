import pandas as pd
from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.query.indexer_adapters import (
    read_indexer_covariates,
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    read_indexer_text_units,
)
from graphrag.model.entity import Entity
from graphrag.vector_stores.lancedb import LanceDBVectorStore
from graphrag.vector_stores.typing import VectorStoreFactory, VectorStoreType
from graphrag.query.input.loaders.dfs import store_entity_semantic_embeddings
from graphrag.query.structured_search.local_search.search import SearchResult
from graphrag.query.structured_search.global_search.search import GlobalSearchResult
from .factories import get_global_search_engine, get_local_search_engine


## Set Copy on Write to True
## This is a workaround to supress a warning from pandas triggered by the way GraphRAG uses it
## We could consider patching graphrag using the ideas mentioned here: https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pd.options.mode.copy_on_write = True

def __get_embedding_description_store(
    entities: list[Entity],
    vector_store_type: str = VectorStoreType.LanceDB,
    config_args: dict | None = None,
):
    """Get the embedding description store."""
    if not config_args:
        config_args = {}

    collection_name = config_args.get("query_collection_name", "entity_description_embeddings")
    config_args.update({"collection_name": collection_name})
    description_embedding_store = VectorStoreFactory.get_vector_store(vector_store_type=vector_store_type, kwargs=config_args)
    description_embedding_store.connect(**config_args)

    if config_args.get("overwrite", True):
        # this step assumes the embeddings were originally stored in a file rather
        # than a vector database

        # dump embeddings from the entities list to the description_embedding_store
        store_entity_semantic_embeddings(
            entities=entities, vectorstore=description_embedding_store
        )
    else:
        # load description embeddings to an in-memory lancedb vectorstore
        # and connect to a remote db, specify url and port values.
        description_embedding_store = LanceDBVectorStore(collection_name=collection_name)
        description_embedding_store.connect(db_uri=config_args.get("db_uri", "./lancedb"))

        # load data from an existing table
        description_embedding_store.document_collection = (
            description_embedding_store.db_connection.open_table(
                description_embedding_store.collection_name
            )
        )

    return description_embedding_store


async def global_search(
    config: GraphRagConfig,
    nodes: pd.DataFrame,
    entities: pd.DataFrame,
    community_reports: pd.DataFrame,
    community_level: int,
    response_type: str,
    query: str,
) -> GlobalSearchResult:
    reports = read_indexer_reports(community_reports, nodes, community_level)
    _entities = read_indexer_entities(nodes, entities, community_level)
    search_engine = get_global_search_engine(
        config,
        reports=reports,
        entities=_entities,
        response_type=response_type,
    )
    result = await search_engine.asearch(query=query)
    return result


async def local_search(
    config: GraphRagConfig,
    nodes: pd.DataFrame,
    entities: pd.DataFrame,
    community_reports: pd.DataFrame,
    text_units: pd.DataFrame,
    relationships: pd.DataFrame,
    covariates: pd.DataFrame | None,
    community_level: int,
    response_type: str,
    query: str,
) -> SearchResult:
    vector_store_args = (
        config.embeddings.vector_store if config.embeddings.vector_store else {}
    )

    vector_store_type = vector_store_args.get("type", VectorStoreType.LanceDB)

    _entities = read_indexer_entities(nodes, entities, community_level)
    description_embedding_store = __get_embedding_description_store(
        entities=_entities,
        vector_store_type=vector_store_type,
        config_args=vector_store_args,
    )
    _covariates = read_indexer_covariates(covariates) if covariates is not None else []

    search_engine = get_local_search_engine(
        config=config,
        reports=read_indexer_reports(community_reports, nodes, community_level),
        text_units=read_indexer_text_units(text_units),
        entities=_entities,
        relationships=read_indexer_relationships(relationships),
        covariates={"claims": _covariates},
        description_embedding_store=description_embedding_store,
        response_type=response_type,
    )

    result = await search_engine.asearch(query=query)
    return result

