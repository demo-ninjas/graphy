
import tiktoken
from azure.cosmos import DatabaseProxy

from graphrag.config import (
    GraphRagConfig,
)
from graphrag.query.context_builder.entity_extraction import EntityVectorStoreKey
from graphrag.query.structured_search.local_search.search import LocalSearch
from graphrag.vector_stores import BaseVectorStore

from ..factory import get_llm, get_text_embedder
from .community_context import GraphyLocalSearchContextBuilder

def get_local_search_engine(
    config: GraphRagConfig,
    response_type: str,
    db: DatabaseProxy,
    vector_store: BaseVectorStore,
) -> LocalSearch:
    """Create a local search engine based on data + configuration."""
    llm = get_llm(config)
    text_embedder = get_text_embedder(config)
    token_encoder = tiktoken.get_encoding(config.encoding_model)

    ls_config = config.local_search

    return LocalSearch(
        llm=llm,
        context_builder=GraphyLocalSearchContextBuilder(
            db=db,
            embedding_vectorstore_key=EntityVectorStoreKey.ID,
            entity_text_embeddings=vector_store,
            text_embedder=text_embedder,
            token_encoder=token_encoder,
        ),
        token_encoder=token_encoder,
        llm_params={
            "max_tokens": ls_config.llm_max_tokens,  # change this based on the token limit you have on your model (if you are using a model with 8k limit, a good setting could be 1000=1500)
            "temperature": ls_config.temperature,
            "top_p": ls_config.top_p,
            "n": ls_config.n,
        },
        context_builder_params={
            "text_unit_prop": ls_config.text_unit_prop,
            "community_prop": ls_config.community_prop,
            "conversation_history_max_turns": ls_config.conversation_history_max_turns,
            "conversation_history_user_turns_only": True,
            "top_k_mapped_entities": ls_config.top_k_entities,
            "top_k_relationships": ls_config.top_k_relationships,
            "include_entity_rank": True,
            "include_relationship_weight": True,
            "include_community_rank": False,
            "return_candidate_context": False,
            "embedding_vectorstore_key": EntityVectorStoreKey.ID,  # set this to EntityVectorStoreKey.TITLE if the vectorstore uses entity title as ids
            "max_tokens": ls_config.max_tokens,  # change this based on the token limit you have on your model (if you are using a model with 8k limit, a good setting could be 5000)
        },
        response_type=response_type,
    )

