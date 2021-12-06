package test;

import com.datastax.test.scheduler.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static void printlnWithTime(String s)
    {
        System.out.println(Instant.now().toString() + ": " + s);
    }

    static void simpleUsage()
    {
        ExecutorService taskExecutor = Executors.newFixedThreadPool(10);
        try ( TaskScheduler scheduler = new TaskScheduler(taskExecutor)){
            scheduler.schedule(() -> printlnWithTime("Task0 in 10 msec"), Duration.ofMillis(10));
            scheduler.schedule(() -> printlnWithTime("Task1 in 3 sec"), Duration.ofSeconds(3));
            scheduler.schedule(() -> printlnWithTime("Task2 in 1 sec"), Duration.ofSeconds(1));
            scheduler.schedule(() -> printlnWithTime("Task3 in 10 msec"), Duration.ofMillis(10));
            scheduler.schedule(() -> printlnWithTime("Task4 in 10 sec - shouldn't normally execute as we don't sleep long enough and shut down"), Duration.ofSeconds(10));

            Thread.sleep(3100); //allow enough time to execute first few tasks according to their schedule

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally
        {
            taskExecutor.shutdown();
        }
    }

    static void concurrentScheduling()
    {
        ExecutorService taskExecutor = Executors.newFixedThreadPool(2);
        ExecutorService submissionExecutor = Executors.newFixedThreadPool(10);
        try ( TaskScheduler scheduler = new TaskScheduler(taskExecutor)){
            submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task0 in 10 msec"), Duration.ofMillis(10)));
            submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task1 in 3 sec"), Duration.ofSeconds(3)));
            submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task2 in 1 sec"), Duration.ofSeconds(1)));
            submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task3 in 10 msec"), Duration.ofMillis(10)));
            submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task4 in 10 sec - shouldn't normally execute as we don't sleep long enough and shut down"), Duration.ofSeconds(10)));

            Thread.sleep(3100); //allow enough time to execute first few tasks according to their schedule

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally
        {
            taskExecutor.shutdown();
            submissionExecutor.shutdown();
        }
    }

    static void schedulingFasterThanExecuting()
    {
        ExecutorService taskExecutor = Executors.newFixedThreadPool(1);
        ExecutorService submissionExecutor = Executors.newFixedThreadPool(10);
        Random rnd = new Random();
        try ( TaskScheduler scheduler = new TaskScheduler(taskExecutor)){
            for(int i=0; i<100; i++)
            {
                long delayMillis = rnd.nextInt(200);
                final int taskNo = i;
                submissionExecutor.submit(() -> scheduler.schedule(() -> printlnWithTime("Task" + taskNo + " in " + delayMillis + " msec"), Duration.ofMillis(delayMillis)));
            }

            Thread.sleep(300); //allow enough time to execute first few tasks according to their schedule

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally
        {
            taskExecutor.shutdown();
            submissionExecutor.shutdown();
        }
    }



    public static void main(String[] args) {

        System.out.println("=== Example usage of TaskScheduler");

        simpleUsage();

        System.out.println("===");

        concurrentScheduling();

        System.out.println("===");

        schedulingFasterThanExecuting();

        System.out.println("===");
    }
}
