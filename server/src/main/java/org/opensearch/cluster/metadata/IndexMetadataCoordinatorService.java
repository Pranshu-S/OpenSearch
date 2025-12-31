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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.*;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.gateway.remote.RemoteIndexMetadataDownloadRequest;
import org.opensearch.gateway.remote.RemoteIndexMetadataDownloadResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for coordinating index metadata updates without cluster state publication.
 * Similar to ClusterService.submitStateUpdateTask but skips the publish/commit phases.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class IndexMetadataCoordinatorService {

    private static final Logger log = LogManager.getLogger(IndexMetadataCoordinatorService.class);
    private static final String REMOTE_INDEX_METADATA_DOWNLOAD_ACTION = "internal:cluster/remote_index_metadata_download";

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final PersistedStateRegistry persistedStateRegistry;
    private volatile PrioritizedOpenSearchThreadPoolExecutor threadPoolExecutor;
    private volatile IndexMetadataTaskBatcher taskBatcher;
    private final ClusterApplier clusterApplier;


    public IndexMetadataCoordinatorService(ClusterService clusterService, ThreadPool threadPool, TransportService transportService, PersistedStateRegistry persistedStateRegistry, ClusterApplier clusterApplier) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.persistedStateRegistry = persistedStateRegistry;
        this.threadPoolExecutor = createThreadPoolExecutor();
        this.taskBatcher = new IndexMetadataTaskBatcher(threadPoolExecutor);
        this.clusterApplier = clusterApplier;


        // Register transport action handler
        transportService.registerRequestHandler(
            REMOTE_INDEX_METADATA_DOWNLOAD_ACTION,
            ThreadPool.Names.GENERIC,
            RemoteIndexMetadataDownloadRequest::new,
            new RemoteIndexMetadataDownloadHandler()
        );

        log.info("Initialised IndexMetadataCoordinatorService");
    }

    private PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
        return OpenSearchExecutors.newSinglePrioritizing(
            "indexMetadataCoordinator",
            OpenSearchExecutors.daemonThreadFactory("indexMetadataCoordinator"),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
    }

    /**
     * Submits an index metadata update task without publishing to the cluster.
     */
    public <T> void submitIndexMetadataUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor,
        IndexMetadataUpdateListener<T> listener
    ) {
        submitIndexMetadataUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Submits a batch of index metadata update tasks without publishing to the cluster.
     */
    public <T> void submitIndexMetadataUpdateTasks(
        final String source,
        final Map<T, IndexMetadataUpdateListener<T>> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        List<IndexMetadataTaskBatcher.UpdateTask> safeTasks = tasks.entrySet()
            .stream()
            .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), e.getValue(), executor))
            .collect(Collectors.toList());
        log.info("Submitting task");
        taskBatcher.submitTasks(safeTasks, config.timeout());
    }

    class IndexMetadataTaskBatcher extends TaskBatcher {

        IndexMetadataTaskBatcher(PrioritizedOpenSearchThreadPoolExecutor threadExecutor) {
            super(LogManager.getLogger(IndexMetadataTaskBatcher.class), threadExecutor, new NoOpTaskBatcherListener());
        }

        IndexMetadataTaskBatcher(PrioritizedOpenSearchThreadPoolExecutor threadExecutor, TaskBatcherListener taskBatcherListener) {
            super(LogManager.getLogger(IndexMetadataTaskBatcher.class), threadExecutor, taskBatcherListener);
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, org.opensearch.common.unit.TimeValue timeout) {
            tasks.forEach(task -> ((UpdateTask) task).listener.onFailure(
                new ProcessClusterEventTimeoutException(timeout, task.source())
            ));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, Function<Boolean, String> taskSummaryGenerator) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;

            try {
                log.info("Executing IMC task");

                ClusterState currentState = clusterService.state();
                ClusterStateTaskExecutor.ClusterTasksResult<Object> result = taskExecutor.execute(
                    currentState,
                    updateTasks.stream().map(BatchedTask::getTask).collect(Collectors.toList())
                );

                // Mark state as committed and notify listeners
                ClusterState finalState = result.resultingState != null ? result.resultingState : currentState;
                if (finalState != currentState) {
                    log.info("Updating IndexMetadata State");
                    String version = persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE).uploadIndexMetadataState(finalState);

                    // Fire async download and apply action on all nodes after writes
                    fireAsyncDownloadAndApply(finalState, version);
                }

                for (UpdateTask updateTask : updateTasks) {
                    ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.get(updateTask.getTask());
                    if (taskResult.isSuccess()) {
                        updateTask.listener.onResponse(finalState);
                    } else {
                        updateTask.listener.onFailure(taskResult.getFailure());
                    }
                }
            } catch (Exception e) {
                updateTasks.forEach(task -> task.listener.onFailure(e));
            }
        }

        class UpdateTask extends BatchedTask {
            final IndexMetadataUpdateListener<Object> listener;

            UpdateTask(
                Priority priority,
                String source,
                Object task,
                IndexMetadataUpdateListener<?> listener,
                ClusterStateTaskExecutor<?> executor
            ) {
                super(priority, source, executor, task);
                this.listener = (IndexMetadataUpdateListener<Object>) listener;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<Object>) batchingKey).describeTasks(
                    tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList())
                );
            }
        }
    }

    /**
     * Listener for index metadata update tasks that provides access to the computed state
     * without waiting for cluster publication.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.0.0")
    public interface IndexMetadataUpdateListener<T> {
        /**
         * Called when the task execution completes successfully.
         * The newState contains the computed changes but is not yet published.
         */
        void onResponse(ClusterState newState);

        /**
         * Called when the task execution fails.
         */
        void onFailure(Exception e);
    }

    /**
     * Transport action handler for remote index metadata download requests
     */
    private class RemoteIndexMetadataDownloadHandler implements TransportRequestHandler<RemoteIndexMetadataDownloadRequest> {
        @Override
        public void messageReceived(RemoteIndexMetadataDownloadRequest request, TransportChannel channel, Task task) throws Exception {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    log.info("Received remote index metadata download request from {} for cluster UUID: {}",
                        request.getSourceNode().getName(), request.getClusterUUID());

                    persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE).downloadAndCommitIndexMetadataState(request.getManifestFileName());

                    clusterApplier.setPreCommitState(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE).getLastAcceptedState());

                    clusterApplier.onNewClusterState("create-index", () -> persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE).getLastAcceptedState(), new ClusterApplier.ClusterApplyListener() {
                        @Override
                        public void onFailure(String source, Exception e) {
                        }

                        @Override
                        public void onSuccess(String source) {
                        }
                    });


                    channel.sendResponse(RemoteIndexMetadataDownloadResponse.success());
                } catch (Exception e) {
                    log.error("Failed to process remote index metadata download request", e);
                    try {
                        channel.sendResponse(RemoteIndexMetadataDownloadResponse.failure(e.getMessage()));
                    } catch (Exception ex) {
                        log.error("Failed to send error response", ex);
                    }
                }
            });
        }
    }

    private void fireAsyncDownloadAndApply(ClusterState clusterState, String version) {
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (!node.equals(clusterState.getNodes().getLocalNode())) {
                sendIndexMetadataDownload(node, clusterState, version);
            }
        }
    }

    private void sendIndexMetadataDownload(DiscoveryNode destination, ClusterState clusterState, String version) {
        try {
            log.info("sending remote index metadata download request to node: {}", destination.getName());
            final RemoteIndexMetadataDownloadRequest downloadRequest = new RemoteIndexMetadataDownloadRequest(
                clusterState.getNodes().getLocalNode(),
                clusterState.metadata().clusterUUID(),
                version
            );

            final TransportResponseHandler<RemoteIndexMetadataDownloadResponse> responseHandler = new TransportResponseHandler<>() {

                @Override
                public RemoteIndexMetadataDownloadResponse read(StreamInput in) throws IOException {
                    return new RemoteIndexMetadataDownloadResponse(in);
                }

                @Override
                public void handleResponse(RemoteIndexMetadataDownloadResponse response) {
                    if (response.isSuccess()) {
                        log.info("successfully sent remote index metadata download to {}", destination.getName());
                    } else {
                        log.warn("remote index metadata download failed on {}: {}", destination.getName(), response.getErrorMessage());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    log.warn("failed to send remote index metadata download to {}", destination, exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            };

            transportService.sendRequest(destination, REMOTE_INDEX_METADATA_DOWNLOAD_ACTION, downloadRequest, responseHandler);
        } catch (Exception e) {
            log.warn("error preparing remote index metadata download for {}", destination, e);
        }
    }


}
