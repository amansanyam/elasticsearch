/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.monitor.jvm.SunThreadInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;

/**
 * What the direct-to-block CSV walk spends on a row whose field fails to convert under {@code null_field}.
 *
 * <p>The direct walkers exist to convert a record straight from its character range into typed block builders,
 * with no {@link String} per cell. The error excerpt is the one thing on that path that would materialize the
 * whole record, and only a row-level rejection ever quotes it: under {@code null_field} a failed conversion is
 * reported per field and the row is kept, so nothing reads an excerpt. A file whose every row carries one bad
 * column must therefore allocate no more than one whose rows are all clean.
 *
 * <p>The two files here are the same size and shape and differ in one column's content, so the difference
 * between the two reads is what the error path itself costs. Allocation is read from
 * {@link SunThreadInfo}, the same JDK counter {@code HotThreads} reports with; a JVM that does not
 * offer it skips the case.
 */
public class CsvDirectRowExcerptCostTests extends ESTestCase {

    /** Rows per file. Deliberately small: the signal comes from record SIZE, so the walk stays below C2. */
    private static final int ROWS = 200;
    /** Characters of clean, unprojected payload per record — what an excerpt would copy. */
    private static final int PAD_CHARS = 64 * 1024;

    private static final String CLEAN = buildCsv("1");
    private static final String ONE_BAD_COLUMN = buildCsv("notanumber");

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
    }

    /** Lenient reads emit response-header warnings; drop them so the parent {@code ensureNoWarnings} passes. */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    public void testNullFieldRowDoesNotMaterializeTheRecord() throws IOException {
        assumeTrue("JVM does not report per-thread allocation", SunThreadInfo.INSTANCE.isThreadAllocatedMemorySupported());

        // Load the read path, the conversion path and the error path before anything is measured.
        read(buildCsv("1", 4, 64));
        read(buildCsv("notanumber", 4, 64));

        long clean = allocatedDuringRead(CLEAN);
        long oneBadColumn = allocatedDuringRead(ONE_BAD_COLUMN);
        long excerptBytes = (long) ROWS * PAD_CHARS;

        logger.info(
            "read allocated: clean={} bytes, one-bad-column={} bytes, delta={} bytes, record bytes={}",
            clean,
            oneBadColumn,
            oneBadColumn - clean,
            excerptBytes
        );
        assertThat("a null_field row must not pay for a record excerpt nobody reads", oneBadColumn - clean, lessThan(excerptBytes / 4));
    }

    private long allocatedDuringRead(String content) throws IOException {
        long id = Thread.currentThread().threadId();
        long before = SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id);
        read(content);
        return SunThreadInfo.INSTANCE.getThreadAllocatedBytes(id) - before;
    }

    /**
     * Reads with the direct-to-block walk, projecting only the column that fails to convert: the padded column
     * is never materialized, so what a bad row costs over a clean one is the excerpt and nothing else.
     */
    private void read(String content) throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withDirectBlockEnabled(true);
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("a"))
            .batchSize(1024)
            .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100_000, 0.0, false))
            .build();
        int rows = 0;
        try (CloseableIterator<Page> pages = reader.read(object, ctx)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals("every row is kept under null_field", content.lines().count() - 1, rows);
    }

    private static String buildCsv(String firstColumn) {
        return buildCsv(firstColumn, ROWS, PAD_CHARS);
    }

    private static String buildCsv(String firstColumn, int rows, int padChars) {
        String pad = "x".repeat(padChars);
        StringBuilder sb = new StringBuilder(rows * (padChars + 32));
        sb.append("a:long,b:keyword\n");
        for (int i = 0; i < rows; i++) {
            sb.append(firstColumn).append(',').append(pad).append('\n');
        }
        return sb.toString();
    }

    private record InMemoryStorageObject(byte[] bytes) implements StorageObject {
        @Override
        public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, Math.toIntExact(position), Math.toIntExact(length));
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("mem://host/csv-direct-row-excerpt-cost-tests");
        }

        @Override
        public StorageIdentity storageIdentity() {
            return AbstractTestStorageObject.NOOP;
        }
    }
}
