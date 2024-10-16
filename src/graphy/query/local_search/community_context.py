# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License
"""Algorithms to build context data for local search prompt."""

import logging
from typing import Any, cast

import pandas as pd
import tiktoken


from graphrag.query.context_builder.conversation_history import (
    ConversationHistory,
)
from graphrag.query.context_builder.entity_extraction import (
    EntityVectorStoreKey,
)
from graphrag.query.llm.base import BaseTextEmbedding
from graphrag.query.llm.text_utils import num_tokens
from graphrag.query.structured_search.base import LocalContextBuilder
from graphrag.vector_stores import BaseVectorStore

from azure.cosmos import DatabaseProxy

from graphy.data import Entity, Community, TextUnit
from ..context_builder import build_community_context
from .local_context import build_entity_context, build_covariates_context, build_relationship_context

log = logging.getLogger(__name__)


class GraphyLocalSearchContextBuilder(LocalContextBuilder):
    """Build data context for local search prompt combining community reports and entity/relationship/covariate tables."""

    def __init__(
        self,
        db:DatabaseProxy, 
        entity_text_embeddings: BaseVectorStore,
        text_embedder: BaseTextEmbedding,
        token_encoder: tiktoken.Encoding | None = None,
        embedding_vectorstore_key: str = EntityVectorStoreKey.ID,
    ):
        # if community_reports is None:
        #     community_reports = []
        # if relationships is None:
        #     relationships = []
        # if covariates is None:
        #     covariates = {}
        # if text_units is None:
        #     text_units = []
        # self.entities = {entity.id: entity for entity in entities}
        # self.community_reports = {
        #     community.id: community for community in community_reports
        # }
        # self.text_units = {unit.id: unit for unit in text_units}
        # self.relationships = {
        #     relationship.id: relationship for relationship in relationships
        # }
        # self.covariates = covariates
        self.db = db
        self.entity_text_embeddings = entity_text_embeddings
        self.text_embedder = text_embedder
        self.token_encoder = token_encoder
        self.embedding_vectorstore_key = embedding_vectorstore_key

    def filter_by_entity_keys(self, entity_keys: list[int] | list[str]):
        """Filter entity text embeddings by entity keys."""
        self.entity_text_embeddings.filter_by_id(entity_keys)

    def build_context(
        self,
        query: str,
        conversation_history: ConversationHistory | None = None,
        include_entity_names: list[str] | None = None,
        exclude_entity_names: list[str] | None = None,
        conversation_history_max_turns: int | None = 5,
        conversation_history_user_turns_only: bool = True,
        max_tokens: int = 8000,
        text_unit_prop: float = 0.5,
        community_prop: float = 0.25,
        top_k_mapped_entities: int = 10,
        top_k_relationships: int = 10,
        include_community_rank: bool = False,
        include_entity_rank: bool = False,
        rank_description: str = "number of relationships",
        include_relationship_weight: bool = False,
        relationship_ranking_attribute: str = "rank",
        return_candidate_context: bool = False,
        use_community_summary: bool = False,
        min_community_rank: int = 0,
        community_context_name: str = "Reports",
        column_delimiter: str = "|",
        **kwargs: dict[str, Any],
    ) -> tuple[str | list[str], dict[str, pd.DataFrame]]:
        """
        Build data context for local search prompt.

        Build a context by combining community reports and entity/relationship/covariate tables, and text units using a predefined ratio set by summary_prop.
        """
        if include_entity_names is None:
            include_entity_names = []
        if exclude_entity_names is None:
            exclude_entity_names = []
        if community_prop + text_unit_prop > 1:
            value_error = (
                "The sum of community_prop and text_unit_prop should not exceed 1."
            )
            raise ValueError(value_error)

        # map user query to entities
        # if there is conversation history, attached the previous user questions to the current query
        if conversation_history:
            pre_user_questions = "\n".join(
                conversation_history.get_user_turns(conversation_history_max_turns)
            )
            query = f"{query}\n{pre_user_questions}"

        selected_entities = self.map_query_to_entities(
            query=query,
            text_embedding_vectorstore=self.entity_text_embeddings,
            text_embedder=self.text_embedder,
            include_entity_names=include_entity_names,
            exclude_entity_names=exclude_entity_names,
            k=top_k_mapped_entities,
            oversample_scaler=2,
        )

        # build context
        final_context = list[str]()
        final_context_data = dict[str, pd.DataFrame]()

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
            if conversation_history_context.strip() != "":
                final_context.append(conversation_history_context)
                final_context_data = conversation_history_context_data
                max_tokens = max_tokens - num_tokens(
                    conversation_history_context, self.token_encoder
                )

        # build community context
        community_tokens = max(int(max_tokens * community_prop), 0)
        community_context, community_context_data = self._build_community_context(
            selected_entities=selected_entities,
            max_tokens=community_tokens,
            use_community_summary=use_community_summary,
            column_delimiter=column_delimiter,
            include_community_rank=include_community_rank,
            min_community_rank=min_community_rank,
            return_candidate_context=return_candidate_context,
            context_name=community_context_name,
        )
        if community_context.strip() != "":
            final_context.append(community_context)
            final_context_data = {**final_context_data, **community_context_data}

        # build local (i.e. entity-relationship-covariate) context
        local_prop = 1 - community_prop - text_unit_prop
        local_tokens = max(int(max_tokens * local_prop), 0)
        local_context, local_context_data = self._build_local_context(
            selected_entities=selected_entities,
            max_tokens=local_tokens,
            include_entity_rank=include_entity_rank,
            rank_description=rank_description,
            include_relationship_weight=include_relationship_weight,
            top_k_relationships=top_k_relationships,
            relationship_ranking_attribute=relationship_ranking_attribute,
            return_candidate_context=return_candidate_context,
            column_delimiter=column_delimiter,
        )
        if local_context.strip() != "":
            final_context.append(str(local_context))
            final_context_data = {**final_context_data, **local_context_data}

        # build text unit context
        text_unit_tokens = max(int(max_tokens * text_unit_prop), 0)
        text_unit_context, text_unit_context_data = self._build_text_unit_context(
            db=self.db,
            selected_entities=selected_entities,
            max_tokens=text_unit_tokens,
            return_candidate_context=return_candidate_context,
        )
        if text_unit_context.strip() != "":
            final_context.append(text_unit_context)
            final_context_data = {**final_context_data, **text_unit_context_data}

        return ("\n\n".join(final_context), final_context_data)

    def _build_community_context(
        self,
        selected_entities: list[Entity],
        max_tokens: int = 4000,
        use_community_summary: bool = False,
        column_delimiter: str = "|",
        include_community_rank: bool = False,
        min_community_rank: int = 0,
        return_candidate_context: bool = False,
        context_name: str = "Reports",
    ) -> tuple[str, dict[str, pd.DataFrame]]:
        """Add community data to the context window until it hits the max_tokens limit."""
        if len(selected_entities) == 0:
            return ("", {context_name.lower(): pd.DataFrame()})

        community_matches = {}
        for entity in selected_entities:
            # increase count of the community that this entity belongs to
            if entity.community_ids:
                for community_id in entity.community_ids:
                    community_matches[community_id] = (
                        community_matches.get(community_id, 0) + 1
                    )

        # sort communities by number of matched entities and rank
        selected_communities = [
            Community.load(community_id, db=self.db)
            for community_id in community_matches.keys()
        ]
        for community in selected_communities:
            community.matches = community_matches[community.id]
            # if community.attributes is None:
            #     community.attributes = {}
            # community.attributes["matches"] = community_matches[community.id]
        selected_communities.sort(
            key=lambda x: (x.matches, x.rank),  # type: ignore
            reverse=True,  # type: ignore
        )
        # for community in selected_communities:
        #     del community.attributes["matches"]  # type: ignore

        context_text, context_data = build_community_context(
            db=self.db,
            selected_communities=selected_communities,
            token_encoder=self.token_encoder,
            use_community_summary=use_community_summary,
            column_delimiter=column_delimiter,
            shuffle_data=False,
            include_community_rank=include_community_rank,
            min_community_rank=min_community_rank,
            max_tokens=max_tokens,
            single_batch=True,
            context_name=context_name,
        )
        if isinstance(context_text, list) and len(context_text) > 0:
            context_text = "\n\n".join(context_text)

        # TODO: Determine if we need to return candidate context
        # if return_candidate_context:
        #     candidate_context_data = get_candidate_communities(
        #         selected_entities=selected_entities,
        #         use_community_summary=use_community_summary,
        #         include_community_rank=include_community_rank,
        #     )
        #     context_key = context_name.lower()
        #     if context_key not in context_data:
        #         context_data[context_key] = candidate_context_data
        #         context_data[context_key]["in_context"] = False
        #     else:
        #         if (
        #             "id" in candidate_context_data.columns
        #             and "id" in context_data[context_key].columns
        #         ):
        #             candidate_context_data["in_context"] = candidate_context_data[
        #                 "id"
        #             ].isin(  # cspell:disable-line
        #                 context_data[context_key]["id"]
        #             )
        #             context_data[context_key] = candidate_context_data
        #         else:
        #             context_data[context_key]["in_context"] = True
        return (str(context_text), context_data)

    def _build_text_unit_context(
        self,
        db:DatabaseProxy,
        selected_entities: list[Entity],
        max_tokens: int = 8000,
        return_candidate_context: bool = False,
        column_delimiter: str = "|",
        context_name: str = "Sources",
    ) -> tuple[str, dict[str, pd.DataFrame]]:
        """Rank matching text units and add them to the context window until it hits the max_tokens limit."""
        if len(selected_entities) == 0:
            return ("", {context_name.lower(): pd.DataFrame()})

        selected_text_units = dict[str, TextUnit]()
        # for each matching text unit, rank first by the order of the entities that match it, then by the number of matching relationships
        # that the text unit has with the matching entities
        for index, entity in enumerate(selected_entities):
            entity.load_metadata(db)  ## To ensure the sources (textunits) are loaded
            entity.load_relationships(db) ## To ensure the relationships are loaded
            if entity.sources is not None and len(entity.sources) > 0:
                for text_id in entity.sources:
                    if (text_id not in selected_text_units):
                        selected_unit = TextUnit.load(text_id, db=self.db)
                        if selected_unit is None: 
                            continue

                        num_relationships = self.count_relationships(selected_unit, entity)
                        selected_unit._entity_order = index
                        selected_unit._num_relationships = num_relationships
                        selected_text_units[text_id] = selected_unit

        # sort selected text units by ascending order of entity order and descending order of number of relationships
        selected_text_units = list(selected_text_units.values())
        selected_text_units.sort(
            key=lambda x: (
                x._entity_order,  # type: ignore
                -x._num_relationships,  # type: ignore
            )
        )

        context_text, context_data = self.build_text_unit_context(
            text_units=selected_text_units,
            token_encoder=self.token_encoder,
            max_tokens=max_tokens,
            shuffle_data=False,
            context_name=context_name,
            column_delimiter=column_delimiter,
        )

        # if return_candidate_context:
        #     candidate_context_data = get_candidate_text_units(
        #         selected_entities=selected_entities,
        #         text_units=list(self.text_units.values()),
        #     )
        #     context_key = context_name.lower()
        #     if context_key not in context_data:
        #         context_data[context_key] = candidate_context_data
        #         context_data[context_key]["in_context"] = False
        #     else:
        #         if (
        #             "id" in candidate_context_data.columns
        #             and "id" in context_data[context_key].columns
        #         ):
        #             candidate_context_data["in_context"] = candidate_context_data[
        #                 "id"
        #             ].isin(  # cspell:disable-line
        #                 context_data[context_key]["id"]
        #             )
        #             context_data[context_key] = candidate_context_data
        #         else:
        #             context_data[context_key]["in_context"] = True
        return (str(context_text), context_data)

    def _build_local_context(
        self,
        selected_entities: list[Entity],
        max_tokens: int = 8000,
        include_entity_rank: bool = False,
        rank_description: str = "relationship count",
        include_relationship_weight: bool = False,
        top_k_relationships: int = 10,
        relationship_ranking_attribute: str = "rank",
        return_candidate_context: bool = False,
        column_delimiter: str = "|",
    ) -> tuple[str, dict[str, pd.DataFrame]]:
        """Build data context for local search prompt combining entity/relationship/covariate tables."""
        # build entity context
        entity_context, entity_context_data = build_entity_context(
            db=self.db,
            selected_entities=selected_entities,
            token_encoder=self.token_encoder,
            max_tokens=max_tokens,
            column_delimiter=column_delimiter,
            include_entity_rank=include_entity_rank,
            rank_description=rank_description,
            context_name="Entities",
        )
        entity_tokens = num_tokens(entity_context, self.token_encoder)

        # build relationship-covariate context
        added_entities = []
        final_context = []
        final_context_data = {}

        # gradually add entities and associated metadata to the context until we reach limit
        for entity in selected_entities:
            current_context = []
            current_context_data = {}
            added_entities.append(entity)

            # build relationship context
            (
                relationship_context,
                relationship_context_data,
            ) = build_relationship_context(
                db=self.db,
                selected_entities=added_entities,
                token_encoder=self.token_encoder,
                max_tokens=max_tokens,
                column_delimiter=column_delimiter,
                top_k_relationships=top_k_relationships,
                include_relationship_weight=include_relationship_weight,
                relationship_ranking_attribute=relationship_ranking_attribute,
                context_name="Relationships",
            )
            current_context.append(relationship_context)
            current_context_data["relationships"] = relationship_context_data
            total_tokens = entity_tokens + num_tokens(
                relationship_context, self.token_encoder
            )

            # build covariate context
            # for covariate in self.covariates:
            covariate_context, covariate_context_data = build_covariates_context(
                db=self.db,
                selected_entities=added_entities,
                token_encoder=self.token_encoder,
                max_tokens=max_tokens,
                column_delimiter=column_delimiter,
                # context_name=covariate,
            )
            total_tokens += num_tokens(covariate_context, self.token_encoder)
            current_context.append(covariate_context)
            current_context_data["Covariates"] = covariate_context_data

            if total_tokens > max_tokens:
                log.info("Reached token limit - reverting to previous context state")
                break

            final_context = current_context
            final_context_data = current_context_data

        # attach entity context to final context
        final_context_text = entity_context + "\n\n" + "\n\n".join(final_context)
        final_context_data["entities"] = entity_context_data

        # if return_candidate_context:
        #     # we return all the candidate entities/relationships/covariates (not only those that were fitted into the context window)
        #     # and add a tag to indicate which records were included in the context window
        #     candidate_context_data = get_candidate_context(
        #         selected_entities=selected_entities,
        #         entities=list(self.entities.values()),
        #         relationships=list(self.relationships.values()),
        #         covariates=self.covariates,
        #         include_entity_rank=include_entity_rank,
        #         entity_rank_description=rank_description,
        #         include_relationship_weight=include_relationship_weight,
        #     )
        #     for key in candidate_context_data:
        #         candidate_df = candidate_context_data[key]
        #         if key not in final_context_data:
        #             final_context_data[key] = candidate_df
        #             final_context_data[key]["in_context"] = False
        #         else:
        #             in_context_df = final_context_data[key]

        #             if "id" in in_context_df.columns and "id" in candidate_df.columns:
        #                 candidate_df["in_context"] = candidate_df[
        #                     "id"
        #                 ].isin(  # cspell:disable-line
        #                     in_context_df["id"]
        #                 )
        #                 final_context_data[key] = candidate_df
        #             else:
        #                 final_context_data[key]["in_context"] = True

        # else:
        for key in final_context_data:
            final_context_data[key]["in_context"] = True
        return (final_context_text, final_context_data)

    def map_query_to_entities(
        self, 
        query: str,
        text_embedding_vectorstore: BaseVectorStore,
        text_embedder: BaseTextEmbedding,
        include_entity_names: list[str] | None = None,
        exclude_entity_names: list[str] | None = None,
        k: int = 10,
        oversample_scaler: int = 2,
    ) -> list[Entity]:
        """Extract entities that match a given query using semantic similarity of text embeddings of query and entity descriptions."""
        if include_entity_names is None:
            include_entity_names = []
        if exclude_entity_names is None:
            exclude_entity_names = []
        matched_entities = []
        if query != "":
            # get entities with highest semantic similarity to query
            # oversample to account for excluded entities
            search_results = text_embedding_vectorstore.similarity_search_by_text(
                text=query,
                text_embedder=lambda t: text_embedder.embed(t),
                k=k * oversample_scaler,
            )

            result_entity_ids = [result.document.id for result in search_results]
            matched_entities = Entity.load_all(result_entity_ids, db=self.db)
        else:
            raise ValueError("Query cannot be empty")

        # filter out excluded entities
        if exclude_entity_names:
            matched_entities = [
                entity
                for entity in matched_entities
                if entity.title not in exclude_entity_names
            ]

        # add entities in the include_entity list
        included_entities = []
        for entity_name in include_entity_names:
            included_entities.extend(Entity.load(entity_name, db=self.db))
        return included_entities + matched_entities

    
    def count_relationships(self, text_unit: TextUnit, entity: Entity) -> int:
        """Count the number of relationships of the selected entity that are associated with the text unit."""
        matching_relationships = []
        for rel in entity.inbound_relationships:
            if rel.id in text_unit.relationship_ids:
                matching_relationships.append(rel)
        for rel in entity.outbound_relationships:
            if rel.id in text_unit.relationship_ids:
                matching_relationships.append(rel)

        return len(matching_relationships)
    
        
    def build_text_unit_context(
        self, 
        text_units: list[TextUnit],
        token_encoder: tiktoken.Encoding | None = None,
        column_delimiter: str = "|",
        shuffle_data: bool = True,
        max_tokens: int = 8000,
        context_name: str = "Sources",
        random_state: int = 86,
    ) -> tuple[str, dict[str, pd.DataFrame]]:
        """Prepare text-unit data table as context data for system prompt."""
        if text_units is None or len(text_units) == 0:
            return ("", {})
        
        if shuffle_data:
            import random
            random.seed(random_state)
            random.shuffle(text_units)

        # add context header
        current_context_text = f"-----{context_name}-----" + "\n"

        # add header
        header = ["id", "text"]
        # attribute_cols = (
        #     list(text_units[0].attributes.keys()) if text_units[0].attributes else []
        # )
        # attribute_cols = [col for col in attribute_cols if col not in header]
        # header.extend(attribute_cols)

        current_context_text += column_delimiter.join(header) + "\n"
        current_tokens = num_tokens(current_context_text, token_encoder)
        all_context_records = [header]

        for unit in text_units:
            new_context = [
                unit.id,
                unit.text
            ]
            new_context_text = column_delimiter.join(new_context) + "\n"
            new_tokens = num_tokens(new_context_text, token_encoder)

            if current_tokens + new_tokens > max_tokens:
                break

            current_context_text += new_context_text
            all_context_records.append(new_context)
            current_tokens += new_tokens

        if len(all_context_records) > 1:
            record_df = pd.DataFrame(
                all_context_records[1:], columns=cast(Any, all_context_records[0])
            )
        else:
            record_df = pd.DataFrame()
        return current_context_text, {context_name.lower(): record_df}
