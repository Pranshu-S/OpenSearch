/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BlobContainerTests extends OpenSearchTestCase {

    public void testBlobContainerDefaultTimeoutMethod() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        List<String> blobNames = Arrays.asList("blob1", "blob2");
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        doCallRealMethod().when(container).deleteBlobsIgnoringIfNotExists(eq(blobNames), eq(timeout));
        container.deleteBlobsIgnoringIfNotExists(blobNames, timeout);

        verify(container).deleteBlobsIgnoringIfNotExists(blobNames);
    }
}
