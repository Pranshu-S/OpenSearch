/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Response for remote index metadata download operations
 *
 * @opensearch.internal
 */
public class RemoteIndexMetadataDownloadResponse extends ActionResponse {

    private final boolean success;
    private final String errorMessage;

    public RemoteIndexMetadataDownloadResponse(boolean success, String errorMessage) {
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public RemoteIndexMetadataDownloadResponse(StreamInput in) throws IOException {
        super(in);
        this.success = in.readBoolean();
        this.errorMessage = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeOptionalString(errorMessage);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public static RemoteIndexMetadataDownloadResponse success() {
        return new RemoteIndexMetadataDownloadResponse(true, null);
    }

    public static RemoteIndexMetadataDownloadResponse failure(String errorMessage) {
        return new RemoteIndexMetadataDownloadResponse(false, errorMessage);
    }
}
