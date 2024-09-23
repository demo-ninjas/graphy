from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.query.structured_search.global_search.search import GlobalSearchResult
from graphrag.query.structured_search.global_search.callbacks import GlobalSearchLLMCallback
from azure.cosmos import DatabaseProxy

from .factory import get_global_search_engine

async def global_search(
    config: GraphRagConfig,
    db: DatabaseProxy,
    community_level: int,
    response_type: str,
    query: str,
    min_rank: int = 0,
    use_summary: bool = True,
    allow_general_knowledge: bool = False,
    estimate_tokens: bool = True,
    conversation_history: list[dict[str, str]] | None = None,
    callbacks: list[GlobalSearchLLMCallback] | None = []
) -> GlobalSearchResult:
    search_engine = get_global_search_engine(
        config,
        db=db,
        community_level=community_level,
        min_rank=min_rank,
        use_summary=use_summary,
        allow_general_knowledge=allow_general_knowledge,
        response_type=response_type,
        estimate_tokens=estimate_tokens,
        callbacks=callbacks
    )
    result = await search_engine.asearch(query=query, conversation_history=conversation_history)
    return result
