import asyncio

from .global_search import global_search

from graphrag.config.models.graph_rag_config import GraphRagConfig
from graphrag.query.context_builder.conversation_history import ConversationHistory
from azure.cosmos import DatabaseProxy

from .result import GraphySearchResult

async def search(
    query: str,
    community_level: int,
    response_type: str,
    query_type: str,
    db: DatabaseProxy, 
    min_rank: int = 8.5,
    use_summary:bool = True,
    allow_general_knowledge: bool = False,
    estimate_tokens: bool = True,
    config: GraphRagConfig = None,
    load_sources: bool = False,
    gather_documents: bool = False,
    conversation_history: list[dict[str, str]] | None = None,
) -> GraphySearchResult:
    if config is None:
        from pathlib import Path
        from graphrag.config import create_graphrag_config
        settings_path = Path(__file__).parent / "_default_settings.yaml"
        if settings_path.exists():
            with settings_path.open("rb") as file:
                import yaml
                data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
                config = create_graphrag_config(data, root_dir="./")

    result = None

    if conversation_history is not None:
        conversation_history = ConversationHistory.from_list(conversation_history)

    query_type = query_type.upper()
    if "GLOBAL" in query_type:
        result = await asyncio.create_task(global_search(
            config=config,
            db=db,
            conversation_history=conversation_history,
            community_level=community_level,
            min_rank=min_rank,
            response_type=response_type,
            use_summary=use_summary,
            allow_general_knowledge=allow_general_knowledge,
            estimate_tokens=estimate_tokens,
            query=query))
    else:
        raise ValueError(f"Query type {query_type} not supported")

    ## Parse the Results, look for sources + load them if requested              
    res = GraphySearchResult(
        result=result,
        query=query,
        community_level=community_level,
        response_type=response_type,
        query_type=query_type)
    
    if load_sources:
        res.load_sources(db, gather_documents=gather_documents)

    return res
