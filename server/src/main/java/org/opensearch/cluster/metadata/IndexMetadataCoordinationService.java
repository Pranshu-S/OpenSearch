/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.RemoteClusterStateService;

/**
 * Service running on IMC node to handle index metadata operations
 */
public class IndexMetadataCoordinationService {
    private static final Logger logger = LogManager.getLogger(IndexMetadataCoordinationService.class);

    private final ClusterService clusterService;
    private final MetadataCreateIndexService createIndexService;
    private final RemoteClusterStateService remoteClusterStateService;

    @Inject
    public IndexMetadataCoordinationService(
        ClusterService clusterService,
        MetadataCreateIndexService createIndexService,
        RemoteClusterStateService remoteClusterStateService
    ) {
        this.clusterService = clusterService;
        this.createIndexService = createIndexService;
        this.remoteClusterStateService = remoteClusterStateService;
    }

    /**
     * Handle index creation request on IMC node
     */
    public void createIndex(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        try {
            CreateIndexClusterStateUpdateRequest clusterStateRequest = new CreateIndexClusterStateUpdateRequest(
                request.cause(),
                request.index(),
                request.index()
            )
                .ackTimeout(request.timeout())
                .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
                .settings(request.settings())
                .mappings(request.mappings())
                .aliases(request.aliases())
                .waitForActiveShards(request.waitForActiveShards());

            ClusterState currentState = clusterService.state();
            ClusterState newState = createIndexService.applyCreateIndexRequest(currentState, clusterStateRequest, false);
            
            remoteClusterStateService.writeFullMetadata(newState, currentState.metadata().clusterUUID());
            
            IndexMetadata indexMetadata = newState.metadata().index(request.index());
            listener.onResponse(new CreateIndexResponse(true, true, indexMetadata.getIndex().getName()));

        } catch (Exception e) {
            logger.error("Failed to create index {} on IMC node", request.index(), e);
            listener.onFailure(e);
        }
    }
}
