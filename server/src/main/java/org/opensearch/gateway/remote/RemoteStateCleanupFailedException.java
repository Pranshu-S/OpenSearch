/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.message.ParameterizedMessage;

/**
 * Exception for Remote Cleanup Failed
 */
public class RemoteStateCleanupFailedException extends RuntimeException {

    public RemoteStateCleanupFailedException(ParameterizedMessage parameterizedMessage, Throwable cause) {
        super(parameterizedMessage.toString(), cause);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
