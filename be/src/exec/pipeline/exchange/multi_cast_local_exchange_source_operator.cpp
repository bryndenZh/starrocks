// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/pipeline/exchange/multi_cast_local_exchange_source_operator.h"

#include "exec/exec_node.h"
#include "simd/simd.h"

namespace starrocks::pipeline {

Status MultiCastLocalExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_exchanger->init_metrics(_unique_metrics.get(), false));
    _exchanger->open_source_operator(_mcast_consumer_index);
    _exchanger->observable().attach_source_observer(state, observer());
    return Status::OK();
}

Status MultiCastLocalExchangeSourceOperator::set_finishing(RuntimeState* state) {
    auto notify = _exchanger->observable().defer_notify_sink();
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_source_operator(_mcast_consumer_index);
    }
    return Status::OK();
}

StatusOr<ChunkPtr> MultiCastLocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto notify = _exchanger->observable().defer_notify_sink();
    auto ret = _exchanger->pull_chunk(state, _mcast_consumer_index);
    if (ret.status().is_end_of_file()) {
        (void)set_finishing(state);
        return ret;
    }
    RETURN_IF_ERROR(ret.status());

    bool has_conjuncts = !_conjunct_ctxs.empty();
    auto* bloom_filters = runtime_bloom_filters();
    bool has_rf = bloom_filters != nullptr && !bloom_filters->descriptors().empty();

    if (has_conjuncts || has_rf) {
        ChunkPtr& chunk = ret.value();
        if (chunk == nullptr || chunk->num_rows() == 0) {
            return ret;
        }
        size_t num_rows = chunk->num_rows();

        ChunkUniquePtr owned;

        if (has_conjuncts) {
            FilterPtr filter;
            RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, chunk.get(), &filter, false));

            if (filter == nullptr) {
                if (has_rf) {
                    owned = chunk->clone_unique();
                }
            } else {
                size_t true_count = SIMD::count_nonzero(*filter);
                if (true_count == num_rows) {
                    if (has_rf) {
                        owned = chunk->clone_unique();
                    }
                } else if (true_count == 0) {
                    return ChunkPtr(chunk->clone_empty(0));
                } else {
                    owned = chunk->clone_empty(0);
                    std::vector<uint32_t> selection;
                    selection.reserve(true_count);
                    for (uint32_t i = 0; i < filter->size(); i++) {
                        if ((*filter)[i]) selection.push_back(i);
                    }
                    owned->append_selective(*chunk, selection.data(), 0, selection.size());
                }
            }
        }

        if (has_rf) {
            if (owned == nullptr) {
                owned = chunk->clone_unique();
            }
            eval_runtime_bloom_filters(owned.get());
        }

        if (owned != nullptr) {
            ChunkPtr result(owned.release());
            return result;
        }
    }

    return ret;
}

bool MultiCastLocalExchangeSourceOperator::has_output() const {
    return _exchanger->can_pull_chunk(_mcast_consumer_index);
}

Status MultiCastLocalExchangeSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    if (_runtime_filter_collector != nullptr) {
        RETURN_IF_ERROR(_runtime_filter_collector->prepare(state, _runtime_profile.get()));
    }
    return Status::OK();
}

void MultiCastLocalExchangeSourceOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    if (_runtime_filter_collector != nullptr) {
        _runtime_filter_collector->close(state);
    }
    SourceOperatorFactory::close(state);
}

void MultiCastLocalExchangeSourceOperatorFactory::set_runtime_filter_collector(
        RuntimeFilterProbeCollector* collector) {
    auto rc_collector = std::make_shared<RefCountedRuntimeFilterProbeCollector>(
            1, std::move(*collector));
    _runtime_filter_collector = std::move(rc_collector);
}

} // namespace starrocks::pipeline
