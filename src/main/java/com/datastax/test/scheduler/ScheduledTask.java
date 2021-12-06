package com.datastax.test.scheduler;

import java.time.Instant;

/**
 * Represents a Runnable task scheduled for execution via @TaskScheduler
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class ScheduledTask implements Comparable<ScheduledTask>{
    final Runnable runnable;
    final Instant executeOnOrAfter;
    volatile boolean executed;
    volatile Throwable error;

    //TODO: can use Future and/or add cancel/notify after execution/block until executed capabilities by back-linking to the scheduler

    public ScheduledTask(Runnable runnable, Instant executeOnOrAfter) {
        this.runnable = runnable;
        this.executeOnOrAfter = executeOnOrAfter;
    }

    @Override
    public int compareTo(ScheduledTask o) {
        return this.executeOnOrAfter.compareTo(o.executeOnOrAfter);
    }
}
