from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.vector_stores.typing import VectorStoreType
from graphrag.query.structured_search.base import SearchResult
from azure.cosmos import DatabaseProxy

from .factory import get_local_search_engine
from .ai_search_vector_store import AISearchVectorStore

async def local_search(
    config: GraphRagConfig,
    db: DatabaseProxy,
    response_type: str,
    query: str,
) -> SearchResult:
    vector_store_args = (
        config.embeddings.vector_store if config.embeddings.vector_store else {}
    )

    vector_store_type = vector_store_args.get("type", VectorStoreType.AzureAISearch)
    if vector_store_type != VectorStoreType.AzureAISearch:
        raise ValueError(
            f"Only Azure AI Search is supported for local search, not {vector_store_type}"
        )
    
    vector_store = AISearchVectorStore(collection_name=vector_store_args.get("collection_name", "entity_description_embeddings"))
    vector_store.connect(**vector_store_args)

    search_engine = get_local_search_engine(
        config,
        db=db,
        response_type=response_type,
        vector_store=vector_store,
    )
    
    result = await search_engine.asearch(query=query)
    return result
