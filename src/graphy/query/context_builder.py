# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Community Context."""

import logging
import random
import os
from typing import Any, cast
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import tiktoken

from graphrag.model import CommunityReport, Entity
from graphrag.query.llm.text_utils import num_tokens

from azure.cosmos import ContainerProxy, DatabaseProxy

from graphy.data import Community

log = logging.getLogger(__name__)

NO_COMMUNITY_RECORDS_WARNING: str = (
    "Warning: No community records added when building community context."
)


def build_community_context(
    db:DatabaseProxy,
    token_encoder: tiktoken.Encoding | None = None,
    use_community_summary: bool = True,
    column_delimiter: str = "|",
    shuffle_data: bool = True,
    under_community_level:int = 2,
    include_community_rank: bool = False,
    min_community_rank: int = 0,
    community_rank_name: str = "rank",
    include_community_weight: bool = True,
    community_weight_name: str = "weight",
    normalize_community_weight: bool = True,
    max_tokens: int = 8000,
    single_batch: bool = True,
    context_name: str = "Reports",
    random_state: int = 86,
    estimate_tokens: bool = True,
    selected_communities: list[Community] | None = None,
) -> tuple[str | list[str], dict[str, pd.DataFrame]]:
    """
    Prepare community report data table as context data for system prompt.

    If entities are provided, the community weight is calculated as the count of text units associated with entities within the community.

    The calculated weight is added as an attribute to the community reports and added to the context data table.
    """
    from time import time
    time1 = time()

    cpus_count = (os.cpu_count() or 1)
    threadpool = ThreadPoolExecutor(cpus_count*2)

    def _report_context_text(community:Community, attributes: list[str]) -> tuple[str, list[str]]:
        context = []
        for attribute in attributes:
            fallback_val = "0" if 'weight' in attribute else ""
            if hasattr(community, attribute):
                val = getattr(community, attribute)
                context.append(str(val) if val is not None else fallback_val)
            else: 
                context.append(fallback_val)
        
        result = column_delimiter.join(context) + "\n"
        return result, context 

    ## Select the reports that meet the minimum rank
    community_fields = ["id", "title", "rank", "level"]
    if use_community_summary:
        community_fields.append("summary")
    else:
        community_fields.append("full_content")
    if include_community_weight:
        community_fields.extend(["weight", "normalised_weight"])
    
    communities = Community.load_all_under_level(under_community_level, min_rank=min_community_rank, db=db, only_fields=community_fields) if selected_communities is None else selected_communities
    time2 = time()
    if communities is None or len(communities) == 0:
        return ([], {})

    if shuffle_data:
        random.seed(random_state)
        random.shuffle(communities)
    time3 = time()

    # "global" variables
    # header = ["id", "title", "rank", "level" ]
    # if use_community_summary:
    #     header.append("summary")
    # else: 
    #     header.append("full_content")
    # if include_community_weight: 
    #     header.extend(["weight", "normalised_weight"])
    header = community_fields
    all_context_text: list[str] = []
    all_context_records: list[dict] = []

    # batch variables
    batch_text: str = ""
    batch_tokens: int = 0
    batch_records: list[list[str]] = []

    def _init_batch() -> None:
        nonlocal batch_text, batch_tokens, batch_records
        batch_text = (
            f"-----{context_name}-----" + "\n" + column_delimiter.join(header) + "\n"
        )
        batch_tokens = num_tokens(batch_text, token_encoder)
        batch_records = []

    def _build_batch(records:list[list[str]]) -> None:
        # convert the current context records to pandas dataframe and sort by weight and rank if exist
        record_df = _convert_report_context_to_df(
            context_records=records,
            header=header,
            weight_column=(
                community_weight_name if include_community_weight else None
            ),
            rank_column=community_rank_name if include_community_rank else None,
        )
        if len(record_df) == 0:
            return
        
        current_context_text = record_df.to_csv(index=False, sep=column_delimiter)
        all_context_text.append(current_context_text)
        all_context_records.append(record_df)

    # initialize the first batch
    _init_batch()
    time4 = time()


    def build_context_text_and_count_tokens(community:Community) -> tuple[str, int]:
        new_context_text, new_context = _report_context_text(community, header)
        if estimate_tokens: 
            num_chars = len(new_context_text)
            new_tokens = num_chars // 4
        else:
            new_tokens = num_tokens(new_context_text, token_encoder)
        return new_context_text, new_context, new_tokens

    ## Build Context Text
    context_text_tasks = []
    for report in communities:
        context_text_tasks.append(threadpool.submit(build_context_text_and_count_tokens, report))
    
    ## Wait for all the tasks to be completed
    
    context_texts = [task.result() for task in context_text_tasks]
    # context_texts = asyncio.run_coroutine_threadsafe(asyncio.gather(*context_text_tasks), asyncio.get_running_loop()).result()
    time4a = time()

    ## Build the Batches
    batch_tasks = []
    for new_context_text, new_context, new_tokens in context_texts:
        # new_context_text, new_context, new_tokens = context_texts[i]

        if batch_tokens + new_tokens > max_tokens:
            # add the current batch to the context data and start a new batch if we are in multi-batch mode
            records = batch_records
            _init_batch()
            batch_tasks.append(threadpool.submit(_build_batch, records))
            if single_batch:
                break

        # add current report to the current batch
        batch_text += new_context_text
        batch_tokens += new_tokens
        batch_records.append(new_context)
    
    if len(batch_records) > 0:
        batch_tasks.append(threadpool.submit(_build_batch, batch_records))
    
    ## Wait for all the tasks to be completed
    for task in batch_tasks:
        task.result()

    # for report in communities:
    #     new_context_text, new_context = _report_context_text(report, header)
    #     new_tokens = num_tokens(new_context_text, token_encoder)

    #     if batch_tokens + new_tokens > max_tokens:
    #         # add the current batch to the context data and start a new batch if we are in multi-batch mode
    #         _cut_batch()
    #         if single_batch:
    #             break
    #         _init_batch()

    #     # add current report to the current batch
    #     batch_text += new_context_text
    #     batch_tokens += new_tokens
    #     batch_records.append(new_context)

    time5 = time()
    # add the last batch if it has not been added
    # if batch_text not in all_context_text:
    #     _cut_batch()

    if len(all_context_records) == 0:
        log.warning(NO_COMMUNITY_RECORDS_WARNING)
        return ([], {})

    context_map = {
        context_name.lower(): pd.concat(all_context_records, ignore_index=True)
    }
    time6 = time()

    if log.isEnabledFor(logging.DEBUG):
        log.debug(f"    Time taken to load communities: {time2 - time1:.2f}s")
        log.debug(f" Time taken to shuffle communities: {time3 - time2:.2f}s")
        log.debug(f"    Time taken to initialise batch: {time4 - time3:.2f}s")
        log.debug(f" Time taken to Build Context Texts: {time4a - time4:.2f}s")
        log.debug(f"Time taken to build context Frames: {time5 - time4a:.2f}s")
        log.debug(f"        Time taken to load context: {time6 - time5:.2f}s")
    return all_context_text, context_map


def _rank_report_context(
    report_df: pd.DataFrame,
    weight_column: str | None = "weight",
    rank_column: str | None = "rank",
) -> pd.DataFrame:
    """Sort report context by community weight and rank if exist."""
    rank_attributes: list[str] = []
    if weight_column:
        rank_attributes.append(weight_column)
        report_df[weight_column] = report_df[weight_column].astype(float)
    if rank_column:
        rank_attributes.append(rank_column)
        report_df[rank_column] = report_df[rank_column].astype(float)
    if len(rank_attributes) > 0:
        report_df.sort_values(by=rank_attributes, ascending=False, inplace=True)
    return report_df


def _convert_report_context_to_df(
    context_records: list[list[str]],
    header: list[str],
    weight_column: str | None = None,
    rank_column: str | None = None,
) -> pd.DataFrame:
    """Convert report context records to pandas dataframe and sort by weight and rank if exist."""
    if len(context_records) == 0:
        return pd.DataFrame()

    record_df = pd.DataFrame(
        context_records,
        columns=cast(Any, header),
    )
    return _rank_report_context(
        report_df=record_df,
        weight_column=weight_column,
        rank_column=rank_column,
    )
