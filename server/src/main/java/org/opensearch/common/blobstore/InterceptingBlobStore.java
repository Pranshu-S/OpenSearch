/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import java.io.IOException;

/**
 * A wrapper around BlobStore that applies interceptors for chaos testing
 */
public class InterceptingBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final BlobContainerInterceptorRegistry interceptorRegistry;

    public InterceptingBlobStore(BlobStore delegate, BlobContainerInterceptorRegistry interceptorRegistry) {
        this.delegate = delegate;
        this.interceptorRegistry = interceptorRegistry;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        BlobContainer container = delegate.blobContainer(path);
        return interceptorRegistry.wrapContainer(container);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
