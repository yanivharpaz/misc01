package org.example.config;

public class BatchConfig {
    private final int batchSize;
    private final long batchTimeoutMs;

    private BatchConfig(Builder builder) {
        this.batchSize = builder.batchSize;
        this.batchTimeoutMs = builder.batchTimeoutMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int batchSize = 1000;       // default batch size
        private long batchTimeoutMs = 5000; // default timeout 5 seconds

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withBatchTimeoutMs(long batchTimeoutMs) {
            this.batchTimeoutMs = batchTimeoutMs;
            return this;
        }

        public BatchConfig build() {
            return new BatchConfig(this);
        }
    }
}