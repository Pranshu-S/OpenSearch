/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class S3BlobContainerTimeoutTests extends OpenSearchTestCase {

    private S3BlobContainer s3BlobContainer;
    private S3BlobStore mockBlobStore;

    @Before
    public void setup() {
        mockBlobStore = mock(S3BlobStore.class);
        s3BlobContainer = spy(new S3BlobContainer(BlobPath.cleanPath(), mockBlobStore));
    }

    public void testDeleteBlobsWithCustomTimeout() throws IOException {
        List<String> blobNames = Arrays.asList("blob1", "blob2", "blob3");
        TimeValue timeout = TimeValue.timeValueSeconds(60);

        doAnswer(invocation -> {
            PlainActionFuture<Void> future = invocation.getArgument(1);
            future.onResponse(null);
            return null;
        }).when(s3BlobContainer).deleteBlobsAsyncIgnoringIfNotExists(eq(blobNames), any());

        doAnswer(invocation -> {
            PlainActionFuture<Void> future = invocation.getArgument(0);
            TimeValue passedTimeout = invocation.getArgument(1);
            assertEquals(timeout, passedTimeout);
            return null;
        }).when(s3BlobContainer).getFutureValue(any(), eq(timeout));

        s3BlobContainer.deleteBlobsIgnoringIfNotExists(blobNames, timeout);

        verify(s3BlobContainer).deleteBlobsAsyncIgnoringIfNotExists(eq(blobNames), any());
        verify(s3BlobContainer).getFutureValue(any(), eq(timeout));
    }

    public void testGetFutureValueWithTimeout() throws Exception {
        PlainActionFuture<String> future = new PlainActionFuture<>();
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        String expectedResult = "test-result";

        future.onResponse(expectedResult);

        S3BlobContainer realContainer = new S3BlobContainer(BlobPath.cleanPath(), mockBlobStore);

        String result = realContainer.getFutureValue(future, timeout);
        assertEquals(expectedResult, result);
    }

    public void testDeleteBlobsWithDefaultTimeout() throws IOException {
        List<String> blobNames = Arrays.asList("blob1", "blob2");

        doAnswer(invocation -> {
            PlainActionFuture<Void> future = invocation.getArgument(1);
            future.onResponse(null);
            return null;
        }).when(s3BlobContainer).deleteBlobsAsyncIgnoringIfNotExists(eq(blobNames), any());

        doAnswer(invocation -> {
            PlainActionFuture<Void> future = invocation.getArgument(0);
            TimeValue passedTimeout = invocation.getArgument(1);
            assertEquals(TimeValue.timeValueSeconds(30), passedTimeout);
            return null;
        }).when(s3BlobContainer).getFutureValue(any(), eq(TimeValue.timeValueSeconds(30)));

        s3BlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);

        verify(s3BlobContainer).deleteBlobsAsyncIgnoringIfNotExists(eq(blobNames), any());
        verify(s3BlobContainer).getFutureValue(any(), eq(TimeValue.timeValueSeconds(30)));
    }
}
