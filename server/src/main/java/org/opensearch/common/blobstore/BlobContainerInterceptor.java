/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.core.action.ActionListener;

import java.io.InputStream;
import java.util.Map;

/**
 * Interface for intercepting blob container operations for chaos testing
 */
public interface BlobContainerInterceptor {
    
    /**
     * Intercepts blob read operations
     */
    default InputStream interceptReadBlob(String blobName, InputStream originalStream) throws Exception {
        return originalStream;
    }
    
    /**
     * Intercepts blob write operations
     */
    default void interceptWriteBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists, ActionListener<Void> listener) throws Exception {
        // Default: no interception, proceed with original operation
        throw new UnsupportedOperationException("Interceptor must implement write interception");
    }
    
    /**
     * Intercepts blob deletion
     */
    default void interceptDeleteBlob(String blobName) throws Exception {
        // Default: no interception
    }
    
    /**
     * Intercepts blob listing
     */
    default Map<String, BlobMetadata> interceptListBlobs() throws Exception {
        throw new UnsupportedOperationException("Interceptor must implement list interception");
    }
}