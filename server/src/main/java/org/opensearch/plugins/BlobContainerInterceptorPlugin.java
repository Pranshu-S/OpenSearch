/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.blobstore.BlobContainerInterceptor;

import java.util.Collections;
import java.util.List;

/**
 * Plugin interface for providing blob container interceptors for chaos testing
 */
public interface BlobContainerInterceptorPlugin {
    
    /**
     * Returns a list of blob container interceptors provided by this plugin
     */
    default List<BlobContainerInterceptor> getBlobContainerInterceptors() {
        return Collections.emptyList();
    }
}