/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.ActionType;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.FailAwareWeightedRouting;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.opensearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * Transport action for field capabilities request in an index
 *
 * @opensearch.internal
 */
public class TransportFieldCapabilitiesIndexAction extends HandledTransportAction<
    FieldCapabilitiesIndexRequest,
    FieldCapabilitiesIndexResponse> implements TransportIndicesResolvingAction<FieldCapabilitiesIndexRequest> {

    private static final Logger logger = LogManager.getLogger(TransportFieldCapabilitiesIndexAction.class);

    private static final String ACTION_NAME = FieldCapabilitiesAction.NAME + "[index]";
    private static final String ACTION_SHARD_NAME = ACTION_NAME + "[s]";
    public static final ActionType<FieldCapabilitiesIndexResponse> TYPE = new ActionType<>(
        ACTION_NAME,
        FieldCapabilitiesIndexResponse::new
    );

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchService searchService;
    private final IndicesService indicesService;
    private final Executor executor;

    @Inject
    public TransportFieldCapabilitiesIndexAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        SearchService searchService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(ACTION_NAME, transportService, actionFilters, FieldCapabilitiesIndexRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchService = searchService;
        this.indicesService = indicesService;
        this.executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        transportService.registerRequestHandler(
            ACTION_SHARD_NAME,
            ThreadPool.Names.SAME,
            FieldCapabilitiesIndexRequest::new,
            new ShardTransportHandler()
        );
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesIndexRequest request, ActionListener<FieldCapabilitiesIndexResponse> listener) {
        if (request.includeDynamicProperties()) {
            new AsyncDynamicPropertiesShardsAction(request, listener).start();
        } else {
            new AsyncShardsAction(request, listener).start();
        }
    }

    private FieldCapabilitiesIndexResponse shardOperation(final FieldCapabilitiesIndexRequest request) throws IOException {
        boolean canMatch = canMatchShard(request);
        if (canMatch == false && request.includeDynamicProperties() == false) {
            return new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false);
        }
        ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        MapperService mapperService = indexService.mapperService();
        IndexSettings indexSettings = indexService.getIndexSettings();
        Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
        if (canMatch) {
            collectMappedFieldCaps(request, shardId, mapperService, indexSettings, responseMap);
        }
        if (request.includeDynamicProperties()) {
            collectDynamicPropertyFieldCaps(request, indexService, mapperService, indexSettings, responseMap);
        }
        return new FieldCapabilitiesIndexResponse(request.index(), responseMap, canMatch);
    }

    /**
     * Field caps taken from the cluster-state mapping. Concrete {@code dynamic_properties} names are not part of
     * that mapping, so a wildcard such as {@code *} does not list them here. An exact name still resolves through
     * {@link MapperService#fieldType}, which is why {@code fields=never_seen_i} stays unchanged.
     */
    private void collectMappedFieldCaps(
        FieldCapabilitiesIndexRequest request,
        ShardId shardId,
        MapperService mapperService,
        IndexSettings indexSettings,
        Map<String, IndexFieldCapabilities> responseMap
    ) {
        Set<String> fieldNames = new HashSet<>();
        for (String field : request.fields()) {
            fieldNames.addAll(mapperService.simpleMatchToFullName(field));
        }
        Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
        for (String field : fieldNames) {
            MappedFieldType ft = mapperService.fieldType(field);
            if (ft != null) {
                if (indicesService.isMetadataField(field) || fieldPredicate.test(ft.name())) {
                    addMappedField(responseMap, field, ft, indexSettings);
                } else {
                    continue;
                }
                addParentObjectFields(responseMap, mapperService, ft.name());
            }
        }
    }

    /**
     * Concrete columns created by {@code dynamic_properties} live only in the shard's Lucene {@link FieldInfos}.
     * The external searcher is the last refresh, so documents written since then stay hidden.
     */
    private void collectDynamicPropertyFieldCaps(
        FieldCapabilitiesIndexRequest request,
        IndexService indexService,
        MapperService mapperService,
        IndexSettings indexSettings,
        Map<String, IndexFieldCapabilities> responseMap
    ) throws IOException {
        DocumentMapper documentMapper = mapperService.documentMapper();
        if (documentMapper == null || documentMapper.root().dynamicProperties().length == 0) {
            return;
        }
        Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(request.shardId().getIndexName());
        IndexShard shard = indexService.getShard(request.shardId().id());
        try (Engine.Searcher searcher = shard.acquireSearcher("field_caps_dynamic_properties")) {
            FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(searcher.getIndexReader());
            if (fieldInfos == null) {
                return;
            }
            for (FieldInfo fieldInfo : fieldInfos) {
                String name = fieldInfo.name;
                if (responseMap.containsKey(name) || indicesService.isMetadataField(name)) {
                    continue;
                }
                if (Regex.simpleMatch(request.fields(), name) == false) {
                    continue;
                }
                if (documentMapper.root().findDynamicProperty(name) == null) {
                    continue;
                }
                MappedFieldType ft = mapperService.fieldType(name);
                if (ft == null || fieldPredicate.test(ft.name()) == false) {
                    continue;
                }
                addMappedField(responseMap, name, ft, indexSettings);
                addParentObjectFields(responseMap, mapperService, ft.name());
            }
        }
    }

    private void addMappedField(
        Map<String, IndexFieldCapabilities> responseMap,
        String field,
        MappedFieldType ft,
        IndexSettings indexSettings
    ) {
        // On a pluggable data format index, doc-values-backed fields can be searched, so
        // report searchability accordingly; otherwise use the plain indexed flag.
        boolean searchable;
        if (indexSettings.isPluggableDataFormatEnabled()) {
            searchable = ft.isSearchableViaDocValues(indexSettings);
        } else {
            searchable = ft.isSearchable();
        }
        responseMap.put(field, new IndexFieldCapabilities(field, ft.familyTypeName(), searchable, ft.isAggregatable(), ft.meta()));
    }

    private void addParentObjectFields(Map<String, IndexFieldCapabilities> responseMap, MapperService mapperService, String fieldName) {
        int dotIndex = fieldName.lastIndexOf('.');
        while (dotIndex > -1) {
            String parentField = fieldName.substring(0, dotIndex);
            if (responseMap.containsKey(parentField)) {
                // we added this path on another field already
                break;
            }
            // checks if the parent field contains sub-fields
            if (mapperService.fieldType(parentField) == null) {
                // no field type, it must be an object field
                ObjectMapper mapper = mapperService.getObjectMapper(parentField);
                if (mapper == null) {
                    // parentField is part of a literal dotted field name under a disable_objects=true parent
                    // No ObjectMapper exists for this intermediate path so skip it and continue up the chain
                    dotIndex = parentField.lastIndexOf('.');
                    continue;
                }
                String type = mapper.nested().isNested() ? "nested" : "object";
                responseMap.put(parentField, new IndexFieldCapabilities(parentField, type, false, false, Collections.emptyMap()));
            }
            dotIndex = parentField.lastIndexOf('.');
        }
    }

    /**
     * One searchable copy per shard id: the primary when it is active, otherwise any other started copy.
     */
    private static List<ShardRouting> activeCopiesPreferPrimary(IndexShardRoutingTable routing) {
        List<ShardRouting> copies = new ArrayList<>();
        ShardRouting primary = routing.primaryShard();
        if (primary != null && primary.active() && primary.assignedToNode()) {
            copies.add(primary);
        }
        for (ShardRouting shard : routing.activeShards()) {
            if (shard.assignedToNode() == false || shard.primary()) {
                continue;
            }
            copies.add(shard);
        }
        return copies;
    }

    private boolean canMatchShard(FieldCapabilitiesIndexRequest req) throws IOException {
        if (req.indexFilter() == null || req.indexFilter() instanceof MatchAllQueryBuilder) {
            return true;
        }
        assert req.nowInMillis() != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(req.shardId(), req.nowInMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(req.indexFilter()));
        return searchService.canMatch(searchRequest).canMatch();
    }

    private ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    private ClusterBlockException checkRequestBlock(ClusterState state, String concreteIndex) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, concreteIndex);
    }

    @Override
    public ResolvedIndices resolveIndices(FieldCapabilitiesIndexRequest request) {
        return ResolvedIndices.of(request.index());
    }

    /**
     * An action that executes on each shard sequentially until it finds one that can match the provided
     * {@link FieldCapabilitiesIndexRequest#indexFilter()}. In which case the shard is used
     * to create the final {@link FieldCapabilitiesIndexResponse}.
     *
     * @opensearch.internal
     */
    class AsyncShardsAction {
        private final FieldCapabilitiesIndexRequest request;
        private final DiscoveryNodes nodes;
        private final ActionListener<FieldCapabilitiesIndexResponse> listener;
        private final GroupShardsIterator<ShardIterator> shardsIt;

        private volatile int shardIndex = 0;

        private AsyncShardsAction(FieldCapabilitiesIndexRequest request, ActionListener<FieldCapabilitiesIndexResponse> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            this.request = request;
            blockException = checkRequestBlock(clusterState, request.index());
            if (blockException != null) {
                throw blockException;
            }

            shardsIt = clusterService.operationRouting().searchShards(clusterService.state(), new String[] { request.index() }, null, null);
        }

        public void start() {
            tryNext(null, true);
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, request), e);
            }
            tryNext(e, false);
        }

        private ShardRouting nextRoutingOrNull(Exception failure) {
            if (shardsIt.size() == 0 || shardIndex >= shardsIt.size()) {
                return null;
            }
            ShardRouting next = FailAwareWeightedRouting.getInstance()
                .findNext(shardsIt.get(shardIndex), clusterService.state(), failure, this::moveToNextShard);

            if (next != null) {
                return next;
            }
            moveToNextShard();
            return nextRoutingOrNull(failure);
        }

        private void moveToNextShard() {
            ++shardIndex;
        }

        private void tryNext(@Nullable final Exception lastFailure, boolean canMatchShard) {
            ShardRouting shardRouting = nextRoutingOrNull(lastFailure);
            if (shardRouting == null) {
                if (canMatchShard == false) {
                    listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false));
                } else {
                    if (lastFailure == null || isShardNotAvailableException(lastFailure)) {
                        listener.onFailure(
                            new NoShardAvailableActionException(
                                null,
                                LoggerMessageFormat.format("No shard available for [{}]", request),
                                lastFailure
                            )
                        );
                    } else {
                        logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, request), lastFailure);
                        listener.onFailure(lastFailure);
                    }
                }
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                request.shardId(shardRouting.shardId());
                if (logger.isTraceEnabled()) {
                    logger.trace("sending request [{}] on node [{}]", request, node);
                }
                transportService.sendRequest(
                    node,
                    ACTION_SHARD_NAME,
                    request,
                    new TransportResponseHandler<FieldCapabilitiesIndexResponse>() {

                        @Override
                        public FieldCapabilitiesIndexResponse read(StreamInput in) throws IOException {
                            return new FieldCapabilitiesIndexResponse(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(final FieldCapabilitiesIndexResponse response) {
                            if (response.canMatch()) {
                                listener.onResponse(response);
                            } else {
                                moveToNextShard();
                                tryNext(null, false);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                    }
                );
            }
        }
    }

    /**
     * Reads mapping field caps plus concrete {@code dynamic_properties} names from one active copy of every shard id.
     * The primary is preferred. If a shard id has no active copy, or every active copy fails, the request fails.
     *
     * @opensearch.internal
     */
    class AsyncDynamicPropertiesShardsAction {
        private final FieldCapabilitiesIndexRequest request;
        private final DiscoveryNodes nodes;
        private final ActionListener<FieldCapabilitiesIndexResponse> listener;
        private final List<List<ShardRouting>> candidatesByShard;
        private final int[] nextCandidate;
        private final FieldCapabilitiesIndexResponse[] shardResponses;
        private final AtomicInteger remaining;
        private final AtomicBoolean finished = new AtomicBoolean();

        private AsyncDynamicPropertiesShardsAction(
            FieldCapabilitiesIndexRequest request,
            ActionListener<FieldCapabilitiesIndexResponse> listener
        ) {
            this.listener = listener;
            this.request = request;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] with dynamic properties on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }
            blockException = checkRequestBlock(clusterState, request.index());
            if (blockException != null) {
                throw blockException;
            }

            IndexRoutingTable indexRouting = clusterState.routingTable().index(request.index());
            if (indexRouting == null) {
                throw new IndexNotFoundException(request.index());
            }
            List<IndexShardRoutingTable> shardRoutings = new ArrayList<>(indexRouting.shards().values());
            shardRoutings.sort((left, right) -> Integer.compare(left.shardId().id(), right.shardId().id()));
            candidatesByShard = new ArrayList<>(shardRoutings.size());
            for (IndexShardRoutingTable shardRouting : shardRoutings) {
                List<ShardRouting> copies = activeCopiesPreferPrimary(shardRouting);
                if (copies.isEmpty()) {
                    throw new NoShardAvailableActionException(
                        shardRouting.shardId(),
                        LoggerMessageFormat.format("No shard available for [{}]", request)
                    );
                }
                candidatesByShard.add(copies);
            }
            if (candidatesByShard.isEmpty()) {
                throw new NoShardAvailableActionException(null, LoggerMessageFormat.format("No shard available for [{}]", request));
            }
            nextCandidate = new int[candidatesByShard.size()];
            shardResponses = new FieldCapabilitiesIndexResponse[candidatesByShard.size()];
            remaining = new AtomicInteger(candidatesByShard.size());
        }

        public void start() {
            for (int shardIndex = 0; shardIndex < candidatesByShard.size(); shardIndex++) {
                if (finished.get()) {
                    return;
                }
                tryNext(shardIndex, null);
            }
        }

        private void tryNext(int shardIndex, @Nullable Exception lastFailure) {
            if (finished.get()) {
                return;
            }
            List<ShardRouting> copies = candidatesByShard.get(shardIndex);
            int candidate = nextCandidate[shardIndex]++;
            if (candidate >= copies.size()) {
                fail(shardIndex, lastFailure);
                return;
            }
            ShardRouting shardRouting = copies.get(candidate);
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                tryNext(shardIndex, new NoShardAvailableActionException(shardRouting.shardId()));
                return;
            }
            FieldCapabilitiesIndexRequest shardRequest = request.forShard(shardRouting.shardId());
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "sending dynamic properties field caps [{}] to shard [{}] on node [{}]",
                    request,
                    shardRouting.shardId(),
                    node
                );
            }
            transportService.sendRequest(
                node,
                ACTION_SHARD_NAME,
                shardRequest,
                new TransportResponseHandler<FieldCapabilitiesIndexResponse>() {

                    @Override
                    public FieldCapabilitiesIndexResponse read(StreamInput in) throws IOException {
                        return new FieldCapabilitiesIndexResponse(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(FieldCapabilitiesIndexResponse response) {
                        onShardResponse(shardIndex, response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, request), exp);
                        tryNext(shardIndex, exp);
                    }
                }
            );
        }

        private void onShardResponse(int shardIndex, FieldCapabilitiesIndexResponse response) {
            shardResponses[shardIndex] = response;
            if (remaining.decrementAndGet() == 0) {
                finishSuccess();
            }
        }

        private void fail(int shardIndex, @Nullable Exception lastFailure) {
            if (finished.compareAndSet(false, true) == false) {
                return;
            }
            ShardId shardId = candidatesByShard.get(shardIndex).isEmpty() ? null : candidatesByShard.get(shardIndex).get(0).shardId();
            if (lastFailure == null || isShardNotAvailableException(lastFailure)) {
                listener.onFailure(
                    new NoShardAvailableActionException(
                        shardId,
                        LoggerMessageFormat.format("No shard available for [{}]", request),
                        lastFailure
                    )
                );
            } else {
                logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardId, request), lastFailure);
                listener.onFailure(lastFailure);
            }
        }

        private void finishSuccess() {
            if (finished.compareAndSet(false, true) == false) {
                return;
            }
            boolean anyShardCanMatch = false;
            Map<String, IndexFieldCapabilities> merged = new HashMap<>();
            for (FieldCapabilitiesIndexResponse response : shardResponses) {
                if (response == null) {
                    continue;
                }
                if (response.canMatch()) {
                    anyShardCanMatch = true;
                }
                // Shards that miss index_filter still carry concrete dynamic_properties fields read from FieldInfos.
                for (Map.Entry<String, IndexFieldCapabilities> entry : response.get().entrySet()) {
                    merged.putIfAbsent(entry.getKey(), entry.getValue());
                }
            }
            if (anyShardCanMatch == false && merged.isEmpty()) {
                listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false));
                return;
            }
            // A non-empty map with no matching shard is schema discovery: keep those names and mark the index
            // included, otherwise the coordinating node drops canMatch=false responses.
            listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), merged, true));
        }
    }

    /**
     * Shard transport handler for field capabilities index action
     *
     * @opensearch.internal
     */
    private class ShardTransportHandler implements TransportRequestHandler<FieldCapabilitiesIndexRequest> {
        @Override
        public void messageReceived(final FieldCapabilitiesIndexRequest request, final TransportChannel channel, Task task)
            throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}]", request);
            }
            ActionListener<FieldCapabilitiesIndexResponse> listener = new ChannelActionListener<>(channel, ACTION_SHARD_NAME, request);
            executor.execute(ActionRunnable.supply(listener, () -> shardOperation(request)));
        }
    }
}
