package com.datastax.test.scheduler;

import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TaskSchedulerTest {

    @org.junit.jupiter.api.Test
    void happySchedule() throws InterruptedException {
        Executor testExecutor = Mockito.mock(Executor.class);

        Supplier<Instant> testTimeProvider = Mockito.mock(Supplier.class);
        when(testTimeProvider.get()).thenReturn(Instant.ofEpochSecond(1), Instant.ofEpochSecond(2), Instant.ofEpochSecond(3));

        TaskScheduler scheduler = new TaskScheduler(testExecutor, testTimeProvider);

        Runnable task = Mockito.mock(Runnable.class);
        scheduler.schedule(task, Duration.ofMillis(1));

        verify(testExecutor, times(1)).execute(any());

        //TODO: wire up calling run() of test tasks

        //TODO: test many more scenarios:
        //- throwing from task
        //- emulate delayed execution (enqueued tasks should have been executed earlier)
        //- assert queue sizes in different scenarios etc
        //- etc etc
        //"integrated" tests with real executors, threading, extreme scenarios (fast scheduling vs slow execution etc)
    }
}