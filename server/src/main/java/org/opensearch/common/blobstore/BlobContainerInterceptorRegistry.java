/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for blob container interceptors used in chaos testing
 * @opensearch.internal
 */
@ExperimentalApi
public class BlobContainerInterceptorRegistry {

    private final List<BlobContainerInterceptor> interceptors = new CopyOnWriteArrayList<>();

    public BlobContainerInterceptorRegistry() {}

    /**
     * Register an interceptor
     */
    public void registerInterceptor(BlobContainerInterceptor interceptor) {
        interceptors.add(interceptor);
    }

    /**
     * Get all registered interceptors
     */
    public List<BlobContainerInterceptor> getInterceptors() {
        return new ArrayList<>(interceptors);
    }

    /**
     * Wrap a blob container with interceptors if chaos is enabled
     */
    public BlobContainer wrapContainer(BlobContainer container) {
        if (interceptors.isEmpty()) {
            return container;
        }
        return new InterceptingBlobContainer(container, getInterceptors());
    }
}
