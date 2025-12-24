/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Checksum for index metadata to ensure integrity
 *
 * @opensearch.internal
 */
public class IndexStateChecksum implements ToXContentFragment, Writeable {
    private static final String INDICES_CHECKSUM_FIELD = "indices_checksum";
    private static final String OVERALL_CHECKSUM_FIELD = "overall_checksum";

    private long indicesChecksum;
    private final long overallChecksum;

    public IndexStateChecksum(Map<String, IndexMetadata> indices, ThreadPool threadpool) {
        ExecutorService executorService = threadpool.executor(ThreadPool.Names.REMOTE_STATE_CHECKSUM);
        CountDownLatch latch = new CountDownLatch(1);

        executeChecksumTask((stream) -> {
            stream.writeMapValues(
                indices,
                (checksumStream, value) -> value.writeVerifiableTo((BufferedChecksumStreamOutput) checksumStream)
            );
            return null;
        }, checksum -> this.indicesChecksum = checksum, executorService, latch);

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RemoteStateTransferException("Failed to create checksum for index metadata.", e);
        }

        this.overallChecksum = this.indicesChecksum; // For now, same as indices checksum
    }

    public IndexStateChecksum(long indicesChecksum, long overallChecksum) {
        this.indicesChecksum = indicesChecksum;
        this.overallChecksum = overallChecksum;
    }

    public IndexStateChecksum(StreamInput in) throws IOException {
        this.indicesChecksum = in.readLong();
        this.overallChecksum = in.readLong();
    }

    private void executeChecksumTask(
        CheckedFunction<BufferedChecksumStreamOutput, Void, IOException> checksumTask,
        Consumer<Long> checksumConsumer,
        ExecutorService executorService,
        CountDownLatch latch
    ) {
        executorService.execute(() -> {
            try {
                long checksum = createChecksum(checksumTask);
                checksumConsumer.accept(checksum);
                latch.countDown();
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to execute checksum task", e);
            }
        });
    }

    private long createChecksum(CheckedFunction<BufferedChecksumStreamOutput, Void, IOException> task) throws IOException {
        try (
            BytesStreamOutput out = new BytesStreamOutput();
            BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out)
        ) {
            task.apply(checksumOut);
            return checksumOut.getChecksum();
        }
    }

    public long getIndicesChecksum() {
        return indicesChecksum;
    }

    public long getOverallChecksum() {
        return overallChecksum;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(indicesChecksum);
        out.writeLong(overallChecksum);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INDICES_CHECKSUM_FIELD, indicesChecksum);
        builder.field(OVERALL_CHECKSUM_FIELD, overallChecksum);
        return builder;
    }

    public static IndexStateChecksum fromXContent(XContentParser parser) throws IOException {
        long indicesChecksum = -1;
        long overallChecksum = -1;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);

        String currentFieldName = parser.currentName();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case INDICES_CHECKSUM_FIELD:
                        indicesChecksum = parser.longValue();
                        break;
                    case OVERALL_CHECKSUM_FIELD:
                        overallChecksum = parser.longValue();
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            }
        }

        return new IndexStateChecksum(indicesChecksum, overallChecksum);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexStateChecksum that = (IndexStateChecksum) o;
        return indicesChecksum == that.indicesChecksum && overallChecksum == that.overallChecksum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesChecksum, overallChecksum);
    }
}
