/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.common.blobstore.BlobContainerInterceptorRegistry;

/**
 * Interface for registering blob container interceptors.
 *
 * @opensearch.internal
 */
public interface BlobContainerInterceptorRegistrar {

    /**
     * Registers blob container interceptors with the provided registry.
     *
     * @param registry the registry to register interceptors with
     */
    void registerInterceptors(BlobContainerInterceptorRegistry registry);
}