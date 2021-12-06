package com.datastax.test.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Simple task scheduler/timer which allows scheduling arbitrary Runnable code (tasks) to execute after specified delay
 *
 *
 * Exercise requirements:
 * - should allow scheduling tasks with given delay
 * - no limit on the number of tasks being scheduled (due to time limit, will assume that we still can fit the whole schedule in the heap - just no arbitrary bounds on the length of the queue)
 * - task "doesn't repeat" - let's assume that it means the scheduler doesn't need to support periodic triggering of the same task
 * - implementation needs to conserve resources - assuming this means no constant/unnecessarily frequent polling
 *
 * - no specific requirements re error handling/cancelling - will start with limited support for it
 * - no requirement to support triggers at specific date-time, cron-like expressions etc - presumably because of time limit and to keep things simple for the test exercise
 * - no specific requirements about resilience of schedule/persistence/surviving crashes/restarts etc
 *
 * NOTE: the standard java.util.concurrent.ScheduledThreadPoolExecutor seems to fit the requirements pretty well, but assuming that the task is to create a similar scheduler using slightly lower-level primitives
 */

public class TaskScheduler implements AutoCloseable {
    final PriorityQueue<ScheduledTask> taskSchedule; //this represents the "schedule of tasks", in the form of the priority queue where teh earliest task requiring execution still is at the head

    //will use commandQueue.poll(timeout) to implement "wake up on command or timeout"
    //alternatives considered - interruptible sleep with wait/notify, using phaser or series of CountDownLatches etc - all seem more complex
    final BlockingQueue<SchedulerCommand> commandQueue;
    Instant lastEvaluated = Instant.MIN;

    final Thread evaluatorThread;

    /**
     * supplies "now" time - can be overridden in tests to simulate sequence of timings
     */
    final Supplier<Instant> timeProvider;

    //abstracts the actual execution mechanism. if standard thread pool executors are not supposed to be used, can execute in own threads as simplest solution;
    //TODO: require ExecutorService to be able to return Futures to support cancellation etc, and manage the shutdown
    Executor executor;

    //TODO: implement pollable metrics/query methods such as schedule size, queue size, tasks scheduled between x and y instants etc

    public TaskScheduler(Executor executor)
    {
        this(executor, Instant::now);
    }

    public TaskScheduler(Executor executor, Supplier<Instant> timeProvider)
    {
        this.executor = executor;
        this.timeProvider = timeProvider;
        this.taskSchedule = new PriorityQueue<>();
        this.evaluatorThread = new Thread(this::evaluate, this.toString()+" evaluator thread");
        this.evaluatorThread.setDaemon(true);
        this.commandQueue = new LinkedBlockingDeque<>();


        evaluatorThread.start();
    }

    /**
     * Schedules the given Runnable to be executed on the executor service provided in constructor, AFTER given delay
     * if delay is 0, interpret it as instruction to execute immediately
     *
     * //TODO: better javadoc - parameters etc
     *
     * No guarantees are given to execute at exact moment scheduled with any specific time resolution, will attempt execute on best effort basis AFTER the given delay since the instant of scheduling elapses
     * @param task
     * @param delay
     * @return
     */
    public ScheduledTask schedule(Runnable task, Duration delay) throws InterruptedException {
        //calculate instant at(after) which task need to be executed. if delay is <=0, assume now
        //add to the schedule with specific instant, wake up the schedule manager to enqueue for execution or update next wake up interval

        Instant now = timeProvider.get();
        Instant executeOnOrAfter = now.plus(delay);

        ScheduledTask scheduledTask = new ScheduledTask(task, executeOnOrAfter);

        commandQueue.put(new NewTaskCommand(scheduledTask));

        return scheduledTask;
    }

    //the logic of reevaluation thread
    private void evaluate() {
        long timeoutNanos = Long.MAX_VALUE; //initially, we don't have anything scheduled so will wake up only on first command

        //is supposed to be waken up on new schedule addition or when it is time to execute some previously scheduled task(s)
        //add new task to the schedule if it was addition
        //figure out if there are non-executed tasks "past due", execute them
        //figure out what is the next time execution is required, wait until that time or a signal about new addition to the schedule
        try {
            SchedulerCommand command;
            do {
                command = commandQueue.poll(timeoutNanos, TimeUnit.NANOSECONDS);
                Instant now = timeProvider.get();

                //NOTE: as it stands now, only the evaluatorThread is working with taskSchedule, so synchronisation seems unnecessary
                //however, if we were to add features such as recurring tasks, cron schedules etc - that could possibly mean that
                //we need to sync write access (hopefully not - as we would try to use commands in the queue for modifying other structures too!)
                //or at least read access
                synchronized (taskSchedule) {
                    if (command != null) {
                        if (command instanceof ShutdownCommand) {
                            //TODO: we could notify the enqueued/scheduled but not executed tasks that we are shutting down
                            taskSchedule.clear();
                            commandQueue.clear();
                            return;
                        }

                        if (command instanceof NewTaskCommand) {
                            NewTaskCommand newTaskCommand = (NewTaskCommand) command;
                            ScheduledTask task = newTaskCommand.task;
                            if (task.executeOnOrAfter.isBefore(now))
                                task = new ScheduledTask(task.runnable, now);
                            taskSchedule.add(task);
                        }
                    }

                    Instant nextTimeToWake = executeScheduledUpto(now);
                    timeoutNanos = (nextTimeToWake == null) ? Long.MAX_VALUE : Math.min(nextTimeToWake.getNano() - timeProvider.get().getNano(), 1);
                }
            }
            while (true);
        } catch (InterruptedException e) {
            //assume we have been interrupted as part of aborting operations - just exit
            //TODO: introduce logging framework, log this event at debug level
        }
    }

    /**
     * Executes all enqueued tasks scheduled for up to given "now" moment.
     * @param now
     * @return  when next task in the queue needs to be executed, or null if no more tasks in the queue
     */
    private Instant executeScheduledUpto(Instant now) {
        ScheduledTask task;
        do {
            task = taskSchedule.peek();
            if (task == null)
                break;

            if (task.executeOnOrAfter.compareTo(now) <= 0) {
                taskSchedule.poll(); //TODO: assert task is same - should be by construction as we synchronize access to schedule
                wrapAndExecuteTask(task, executor);
            }
            else  //top task should be executed later
                break;
        }
        while(true);
        return task == null ? null : task.executeOnOrAfter;
    }

    private void wrapAndExecuteTask(final ScheduledTask task, final Executor executor) {
        executor.execute(() ->
        {
            try {
                task.runnable.run();
            }
            catch(Throwable t)
            {
                task.error = t;
                //TODO: introduce logging framework, log this event at debug level
            }
            finally
            {
                task.executed = true;
            }
        });
    }

    @Override
    public void close() throws Exception {
        commandQueue.put(new ShutdownCommand());
        evaluatorThread.join();
    }

    private interface SchedulerCommand
    {}

    private class NewTaskCommand implements SchedulerCommand
    {
        final ScheduledTask task;

        public NewTaskCommand(ScheduledTask task) {
            this.task = task;
        }
    }

    private class ShutdownCommand implements SchedulerCommand
    {
    }

}
