/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Request for downloading and applying index metadata from remote store
 *
 * @opensearch.internal
 */
public class RemoteIndexMetadataDownloadRequest extends ActionRequest {

    private final DiscoveryNode sourceNode;
    private final String clusterUUID;
    private final String manifestFileName;

    public RemoteIndexMetadataDownloadRequest(DiscoveryNode sourceNode, String clusterUUID, String manifestFileName) {
        this.sourceNode = sourceNode;
        this.clusterUUID = clusterUUID;
        this.manifestFileName = manifestFileName;
    }

    public RemoteIndexMetadataDownloadRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceNode = new DiscoveryNode(in);
        this.clusterUUID = in.readString();
        this.manifestFileName = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeString(clusterUUID);
        out.writeString(manifestFileName);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getManifestFileName() {
        return manifestFileName;
    }
}
