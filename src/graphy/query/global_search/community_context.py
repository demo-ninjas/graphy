# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Contains algorithms to build context data for global search prompt."""
from random import random
from typing import Any
from time import time
import pandas as pd
import tiktoken

from azure.cosmos import ContainerProxy, DatabaseProxy

from graphrag.model import CommunityReport, Entity
from graphrag.query.context_builder.conversation_history import (
    ConversationHistory,
)
from graphrag.query.structured_search.base import GlobalContextBuilder

from ..context_builder import build_community_context


class GraphyGlobalCommunityContext(GlobalContextBuilder):
    """GlobalSearch community context builder."""

    def __init__(
        self,
        db: DatabaseProxy,
        token_encoder: tiktoken.Encoding | None = None,
        random_state: int = None,
        cace_community_context: bool = True,
    ):
        self.db = db
        self.token_encoder = token_encoder
        self.random_state = random_state if random_state is not None else int(1000 * random())
        self.cache = {} if cace_community_context else None # cache community context

    def build_context(
        self,
        conversation_history: ConversationHistory | None = None,
        use_community_summary: bool = True,
        column_delimiter: str = "|",
        shuffle_data: bool = True,
        include_community_rank: bool = True,
        under_community_level:int | None = None, 
        min_community_rank: int = 0,
        community_rank_name: str = "rank",
        include_community_weight: bool = True,
        community_weight_name: str = "weights",
        normalize_community_weight: bool = True,
        max_tokens: int = 16_000,
        context_name: str = "Reports",
        conversation_history_user_turns_only: bool = False,
        conversation_history_max_turns: int | None = 5,
        estimate_tokens: bool = True,
        **kwargs: Any,
    ) -> tuple[str | list[str], dict[str, pd.DataFrame]]:
        """Prepare batches of community report data table as context data for global search."""
        start = time()
        conversation_history_context = ""
        final_context_data = {}
        if conversation_history:
            # build conversation history context
            (
                conversation_history_context,
                conversation_history_context_data,
            ) = conversation_history.build_context(
                include_user_turns_only=conversation_history_user_turns_only,
                max_qa_turns=conversation_history_max_turns,
                column_delimiter=column_delimiter,
                max_tokens=max_tokens,
                recency_bias=False,
            )
            if conversation_history_context != "":
                final_context_data = conversation_history_context_data
        
        community_context, community_context_data = None, None
        cache_key = f"{use_community_summary}_{under_community_level}_{min_community_rank}_{include_community_weight}_{normalize_community_weight}" if self.cache is not None else None
        if self.cache is not None:
            if cache_key in self.cache:
                community_context, community_context_data = self.cache[cache_key]
        
        if community_context is None:
            community_context, community_context_data = build_community_context(
                db=self.db,
                token_encoder=self.token_encoder,
                use_community_summary=use_community_summary,
                column_delimiter=column_delimiter,
                shuffle_data=shuffle_data,
                under_community_level=under_community_level,
                include_community_rank=include_community_rank,
                min_community_rank=min_community_rank,
                community_rank_name=community_rank_name,
                include_community_weight=include_community_weight,
                community_weight_name=community_weight_name,
                normalize_community_weight=normalize_community_weight,
                max_tokens=max_tokens,
                single_batch=False,
                context_name=context_name,
                random_state=self.random_state,
                estimate_tokens=estimate_tokens
            )

            if self.cache is not None:
                self.cache[cache_key] = (community_context, community_context_data)


        if isinstance(community_context, list):
            final_context = [
                f"{conversation_history_context}\n\n{context}"
                for context in community_context
            ]
        else:
            final_context = f"{conversation_history_context}\n\n{community_context}"

        final_context_data.update(community_context_data)
        # print(f"GlobalSearch context built in {time() - start:.2f} seconds.")
        return (final_context, final_context_data)
