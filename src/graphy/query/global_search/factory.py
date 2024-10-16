
import tiktoken
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

from graphrag.config import (
    GraphRagConfig,
    LLMType,
)
from graphrag.model import (
    CommunityReport,
    Covariate,
    Entity,
    Relationship,
    TextUnit,
)
from graphrag.query.context_builder.entity_extraction import EntityVectorStoreKey
from graphrag.query.llm.oai.chat_openai import ChatOpenAI
from graphrag.query.llm.oai.embedding import OpenAIEmbedding
from graphrag.query.llm.oai.typing import OpenaiApiType

from graphrag.query.structured_search.global_search.search import GlobalSearch
from graphrag.query.structured_search.local_search.mixed_context import (
    LocalSearchMixedContext,
)
from graphrag.query.structured_search.local_search.search import LocalSearch
from graphrag.vector_stores import BaseVectorStore
from graphrag.query.structured_search.global_search.callbacks import GlobalSearchLLMCallback

from azure.cosmos import ContainerProxy, DatabaseProxy

from ..factory import get_llm
from .community_context import GraphyGlobalCommunityContext


def get_global_search_engine(
    config: GraphRagConfig,
    db: DatabaseProxy,
    community_level:int,
    response_type: str,
    min_rank:int = 0,
    allow_general_knowledge: bool = False,
    use_summary: bool = True,
    estimate_tokens: bool = True,
    only_user_turns_in_history: bool = False,
    callbacks: list[GlobalSearchLLMCallback] | None = []
):
    """Create a global search engine based on data + configuration."""
    token_encoder = tiktoken.get_encoding(config.encoding_model)
    gs_config = config.global_search

    return GlobalSearch(
        llm=get_llm(config),
        callbacks=callbacks,
        context_builder=GraphyGlobalCommunityContext(
            db=db, 
            token_encoder=token_encoder,
        ),
        token_encoder=token_encoder,
        max_data_tokens=gs_config.data_max_tokens,
        map_llm_params={
            "max_tokens": gs_config.map_max_tokens,
            "temperature": gs_config.temperature,
            "top_p": gs_config.top_p,
            "n": gs_config.n,
        },
        reduce_llm_params={
            "max_tokens": gs_config.reduce_max_tokens,
            "temperature": gs_config.temperature,
            "top_p": gs_config.top_p,
            "n": gs_config.n,
        },
        allow_general_knowledge=allow_general_knowledge,
        json_mode=True,
        context_builder_params={
            "use_community_summary": use_summary,
            "shuffle_data": True,
            "include_community_rank": True,
            "under_community_level": community_level,
            "min_community_rank": min_rank,
            "community_rank_name": "rank",
            "include_community_weight": True,
            "community_weight_name": "weight",
            "normalize_community_weight": True,
            "max_tokens": gs_config.max_tokens,
            "estimate_tokens": estimate_tokens,
            "context_name": "Reports",
            "conversation_history_user_turns_only": only_user_turns_in_history,
        },
        concurrent_coroutines=gs_config.concurrency,
        response_type=response_type,
    )
