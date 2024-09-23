from graphrag.config.models.graph_rag_config import GraphRagConfig

from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.index.progress.types import PrintProgressReporter
from graphrag.model.entity import Entity
from graphrag.vector_stores.lancedb import LanceDBVectorStore
from graphrag.vector_stores.typing import VectorStoreFactory, VectorStoreType

from .input.loaders.dfs import store_entity_semantic_embeddings

from azure.cosmos import DatabaseProxy

from graphy.query import GraphySearchResult
from .factory import get_local_search_engine

from ..factory import get_text_embedder

reporter = PrintProgressReporter("")

async def local_search(
    config: GraphRagConfig,
    db: DatabaseProxy,
    community_level: int,
    response_type: str,
    query: str,
) -> GraphySearchResult:
    vector_store_args = (
        config.embeddings.vector_store if config.embeddings.vector_store else {}
    )

    vector_store_type = vector_store_args.get("type", VectorStoreType.LanceDB)


    search_engine = get_local_search_engine(
        config,
        db=db,
        community_level=community_level,
        response_type=response_type,
    )
    result = await search_engine.asearch(query=query)
    return result


def __get_embedding_description_store(
    db:DatabaseProxy,
    vector_store_type: str = VectorStoreType.LanceDB,
    config_args: dict | None = None,
):
    """Get the embedding description store."""
    if not config_args:
        config_args = {}

    collection_name = config_args.get(
        "query_collection_name", "entity_description_embeddings"
    )
    config_args.update({"collection_name": collection_name})
    description_embedding_store = VectorStoreFactory.get_vector_store(
        vector_store_type=vector_store_type, kwargs=config_args
    )

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
        description_embedding_store = LanceDBVectorStore(
            collection_name=collection_name
        )
        description_embedding_store.connect(
            db_uri=config_args.get("db_uri", "./lancedb")
        )

        # load data from an existing table
        description_embedding_store.document_collection = (
            description_embedding_store.db_connection.open_table(
                description_embedding_store.collection_name
            )
        )

    return description_embedding_store


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
) -> str | dict[str, Any] | list[dict[str, Any]]:
    """Perform a local search."""

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
    reporter.success(f"Local Search Response: {result.response}")
    return result.response
