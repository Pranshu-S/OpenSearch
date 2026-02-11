/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around BlobContainer that applies interceptors for chaos testing
 */
public class InterceptingBlobContainer implements BlobContainer {

    private final BlobContainer delegate;
    private final List<BlobContainerInterceptor> interceptors;

    public InterceptingBlobContainer(BlobContainer delegate, List<BlobContainerInterceptor> interceptors) {
        this.delegate = delegate;
        this.interceptors = interceptors;
    }

    /**
     * Gets the {@link BlobPath} that defines the implementation specific paths to where the blobs are contained.
     *
     * @return the BlobPath where the blobs are contained
     */
    @Override
    public BlobPath path() {
        return delegate.path();
    }

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the {@link BlobContainer} with the given name, and {@code false} otherwise.
     */
    @Override
    public boolean blobExists(String blobName) throws IOException {
        return delegate.blobExists(blobName);
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return delegate.readBlob(blobName);
    }

    /**
     * Creates a new {@link InputStream} that can be used to read the given blob starting from
     * a specific {@code position} in the blob. The {@code length} is an indication of the
     * number of bytes that are expected to be read from the {@link InputStream}.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param position The position in the blob where the next byte will be read.
     * @param length   An indication of the number of bytes to be read.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        return delegate.readBlob(blobName, position, length);
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name.
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param inputStream         The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize            The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                            this value is used in writing the blob to the repository.
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        interceptors.forEach(
            interceptor -> interceptor.interceptWriteBlob(blobName, inputStream, blobSize, failIfAlreadyExists)
        );
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name,
     * using an atomic write operation if the implementation supports it.
     * <p>
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param inputStream         The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize            The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                            this value is used in writing the blob to the repository.
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    /**
     * Deletes this container and all its contents from the repository.
     *
     * @return delete result
     * @throws IOException on failure
     */
    @Override
    public DeleteResult delete() throws IOException {
        return delegate.delete();
    }

    /**
     * Deletes the blobs with given names. This method will not throw an exception
     * when one or multiple of the given blobs don't exist and simply ignore this case.
     *
     * @param blobNames The names of the blob to delete.
     * @throws IOException if a subset of blob exists but could not be deleted.
     */
    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return delegate.listBlobs();
    }

    /**
     * Lists all child containers under this container. A child container is defined as a container whose {@link #path()} method returns
     * a path that has this containers {@link #path()} return as its prefix and has one more path element than the current
     * container's path.
     *
     * @return Map of name of the child container to child container
     * @throws IOException on failure to list child containers
     */
    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return delegate.children();
    }

    /**
     * Lists all blobs in the container that match the specified prefix.
     *
     * @param blobNamePrefix The prefix to match against blob names in the container.
     * @return A map of the matching blobs in the container.  The keys in the map are the names of the blobs
     * and the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name, and metadata.
     * This method assumes the container does not already contain a blob of the same blobName. If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   inputStream
     *          The input stream from which to retrieve the bytes to write to the blob.
     * @param   metadata
     *          The metadata to be associate with the blob upload.
     * @param   blobSize
     *          The size of the blob to be written, in bytes.  It is implementation dependent whether
     *          this value is used in writing the blob to the repository.
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    @Override
    public void writeBlobWithMetadata(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists, Map<String, String> metadata) throws IOException {
        delegate.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name, metadata,
     * and optional encryption settings for index-level encryption override.
     * This method assumes the container does not already contain a blob of the same blobName. If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   inputStream
     *          The input stream from which to retrieve the bytes to write to the blob.
     * @param   blobSize
     *          The size of the blob to be written, in bytes.  It is implementation dependent whether
     *          this value is used in writing the blob to the repository.
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @param   metadata
     *          The metadata to be associate with the blob upload.
     * @param   cryptoMetadata
     *          Optional CryptoMetadata for index-level encryption override (null = use repository defaults)
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */

    @Override
    public void writeBlobWithMetadata(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists, @Nullable Map<String, String> metadata, @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {
        delegate.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, cryptoMetadata);
    };
}
