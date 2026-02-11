/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobContainerInterceptor;
import org.opensearch.common.blobstore.BlobContainerInterceptorRegistry;
import org.opensearch.common.blobstore.InterceptingBlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.plugins.BlobContainerInterceptorPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobContainerInterceptorIT extends RemoteStoreBaseIntegTestCase {

    public static class TestChaosPlugin extends Plugin implements BlobContainerInterceptorPlugin {

        public static final AtomicBoolean interceptorCalled = new AtomicBoolean(false);

        @Override
        public List<BlobContainerInterceptor> getBlobContainerInterceptors() {
            return Collections.singletonList(new TestBlobContainerInterceptor());
        }

        public static class TestBlobContainerInterceptor implements BlobContainerInterceptor {
            @Override
            public void interceptWriteBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
                interceptorCalled.set(true);
            }
        }
    }

    private static final String INDEX_NAME = "test-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(TestChaosPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private void initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount, Settings settings) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount, settings);
        ensureGreen(INDEX_NAME);
    }

    public void testInterceptingBlobContainerCreation() throws Exception {
        // Reset the flag
        TestChaosPlugin.interceptorCalled.set(false);

        clusterSettingsSuppliedByTest = true;
        Path segmentRepoPath = randomRepoPath();
        Path translogRepoPath = randomRepoPath();
        Path remoteRoutingTableRepoPath = randomRepoPath();
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(
            buildRemoteStoreNodeAttributes(
                REPOSITORY_NAME,
                segmentRepoPath,
                REPOSITORY_2_NAME,
                translogRepoPath,
                REMOTE_ROUTING_TABLE_REPO,
                remoteRoutingTableRepoPath,
                false
            )
        );
        settingsBuilder.put(
                RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING.getKey(),
                RemoteStoreEnums.PathType.HASHED_PREFIX.toString()
            )
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, REMOTE_ROUTING_TABLE_REPO)
            .put(REMOTE_PUBLICATION_SETTING_KEY, true);


        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount, settingsBuilder.build());

        // Get the repository instance - REPOSITORY_NAME is already set up by RemoteStoreBaseIntegTestCase
        RepositoriesService repositoriesService = internalCluster().getDataNodeInstance(RepositoriesService.class);
        Repository repository = repositoriesService.repository(REPOSITORY_NAME);
        assertTrue("Repository should be a BlobStoreRepository", repository instanceof BlobStoreRepository);

        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

        // Verify that the interceptor registry is properly set up
        BlobContainerInterceptorRegistry interceptorRegistry = repositoriesService.getInterceptorRegistry();
        assertNotNull("Interceptor registry should not be null", interceptorRegistry);
        assertFalse("Interceptor registry should have interceptors", interceptorRegistry.getInterceptors().isEmpty());
        assertEquals("Should have exactly one interceptor", 1, interceptorRegistry.getInterceptors().size());

        assertTrue("Interceptor should have been called", TestChaosPlugin.interceptorCalled.get());
    }

    public void testBlobContainerInterceptorRegistry() throws Exception {
        // Verify the interceptor registry behavior
        RepositoriesService repositoriesService = internalCluster().getDataNodeInstance(RepositoriesService.class);
        BlobContainerInterceptorRegistry interceptorRegistry = repositoriesService.getInterceptorRegistry();

        // With our plugin, we should have interceptors
        assertNotNull("Interceptor registry should not be null", interceptorRegistry);
        assertFalse("Should have interceptors from our plugin", interceptorRegistry.getInterceptors().isEmpty());

        // Verify the interceptor type
        BlobContainerInterceptor interceptor = interceptorRegistry.getInterceptors().get(0);
        assertTrue("Should be our test interceptor", interceptor instanceof TestChaosPlugin.TestBlobContainerInterceptor);
    }
}
