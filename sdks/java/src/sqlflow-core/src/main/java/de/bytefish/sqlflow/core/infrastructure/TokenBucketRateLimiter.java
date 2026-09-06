package de.bytefish.sqlflow.core.infrastructure;

import java.util.concurrent.locks.LockSupport;

final class TokenBucketRateLimiter
{
    private final long permitsPerSecond;

    private final long capacity;

    private long availableTokens;

    private long lastRefillNanos;

    private final Object sync =
            new Object();

    public TokenBucketRateLimiter(long permitsPerSecond, long capacity)
    {
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = capacity;
        this.availableTokens = capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    public void acquire(int permits)
    {
        while (true)
        {
            long waitNanos;

            synchronized (sync)
            {
                refill();

                if (availableTokens >= permits)
                {
                    availableTokens -= permits;

                    return;
                }

                long missing = permits - availableTokens;

                waitNanos = (missing * 1_000_000_000L) / permitsPerSecond;
            }

            LockSupport.parkNanos(waitNanos);
        }
    }

    private void refill()
    {
        long now = System.nanoTime();

        long elapsed = now - lastRefillNanos;

        if (elapsed <= 0)
        {
            return;
        }

        long newTokens = elapsed * permitsPerSecond / 1_000_000_000L;

        if (newTokens == 0)
        {
            return;
        }

        availableTokens = Math.min(capacity, availableTokens + newTokens);

        lastRefillNanos = now;
    }
}