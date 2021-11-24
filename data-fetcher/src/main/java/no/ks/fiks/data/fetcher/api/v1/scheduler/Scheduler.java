package no.ks.fiks.data.fetcher.api.v1.scheduler;

import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TableFilterAndGroups;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Configuration
public class Scheduler {

    private TaskExecutor taskExecutor;
    private List<TableFilterAndGroups> tableFilterAndGroups;
    private List<SsbApiCallTask> threadQueue;
    private Map<String, SsbApiCallTask> runningThreads;
    private static int queryCounter;
    private static int queryCounter2;
    private static Object LOCK = new Object();

    public Scheduler(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        CsvReader csvReader = new CsvReader();
        try {
            csvReader.readFromCsv();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        tableFilterAndGroups = csvReader.getTablesAndFilters();
        threadQueue = new LinkedList<>();
        runningThreads = new LinkedHashMap<>();
    }

    public synchronized void decreaseQueryCounter(int querySize) {
        queryCounter2 -= querySize;
    }

    public synchronized void increaseQueryCounter(int querySize) {
        queryCounter += querySize;
    }

    public synchronized void resetQueryCounter() {
        queryCounter = 0;
    }

    public synchronized int getQueryCounter() {
        return queryCounter;
    }

    public synchronized void removeThreadFromList(String tableCode) {
        runningThreads.remove(tableCode);
    }

    public synchronized int amountOfRunningThreads() {
        return runningThreads.size();
    }

    public void runApiCall() {
        //30 reqs per 60s
        String jsonString;
        SsbApiCall ssbApiCall;
        int queryCounter = 0;

        ThreadManager manager = new ThreadManager(taskExecutor);
        taskExecutor.execute(manager);

        for (TableFilterAndGroups s : tableFilterAndGroups) {
            /*jsonString = String.format("{\\\"tableCode\\\":\\\"%s\\\"," +
                    "\\\"schemaName\\\":\\\"%s\\\"," +
                    "\\\"numberOfYears\\\":\\\"5\\\"" +
                    "}", s.getTabellnummer(), s.getKildeId().toLowerCase());

             */

            synchronized (manager) {

                ssbApiCall = new SsbApiCall(s.getTabellnummer(), 5, "131", "104", "214", "231", "127");

                if (!s.getTabellnummer().equals("11211") && !s.getTabellnummer().equals("12247")) {
                    manager.addThreadToQueue(new SsbApiCallTask(ssbApiCall, s.getTabellnummer(), ssbApiCall.getQuerySize(),
                            System.nanoTime(), manager));

                    System.out.println("[runApiCall] Time to notify the manager.");
                    manager.notify();
                }
            }
            //System.out.println("Query size: " + ssbApiCall.getQuerySize());

            //increaseQueryCounter(ssbApiCall.getQuerySize());
            /*
            if (!s.getTabellnummer().equals("11211") && !s.getTabellnummer().equals("12247")) {
                threadQueue.add(new SsbApiCallTask(ssbApiCall, s.getTabellnummer(), ssbApiCall.getQuerySize(),
                        System.nanoTime(), this));

                if (getQueryCounter() >= 30 || getQueryCounter() + ssbApiCall.getQuerySize() >= 30) {
                    long alreadyWaitedTime = System.nanoTime() - threadQueue.get(0).getStartTime();
                    System.out.println(System.nanoTime());
                    System.out.println(threadQueue.get(0).getStartTime());
                    System.out.println("Time since first thread nano seconds: " + alreadyWaitedTime);
                    long alreadyWaitedTimeSeconds = TimeUnit.SECONDS.convert(alreadyWaitedTime, TimeUnit.NANOSECONDS);
                    System.out.println("Time since first thread seconds: " + alreadyWaitedTimeSeconds);
                    try {
                        System.out.println("Sleeping for 60s");
                        TimeUnit.SECONDS.sleep(60 - alreadyWaitedTimeSeconds);
                        resetQueryCounter();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                taskExecutor.execute(threadQueue.get(0));
                threadQueue.remove(0);
                //threadQueue.add(new SsbApiCallTask(ssbApiCall, queryCounter));
            }

             */
        }
        /*
        while (amountOfRunningThreads() > 0) {
            System.out.println("Waiting for all threads to finish. Sleeping for 60s");
            try {
                TimeUnit.SECONDS.sleep(60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

         */
    }

    private static class ThreadManager implements Runnable {

        private static List<SsbApiCallTask> threadQueue;
        private static Map<String, SsbApiCallTask> runningThreads;
        private TaskExecutor taskExecutor;
        private int queryCounter;
        private static Object LOCK = new Object();

        public ThreadManager(TaskExecutor taskExecutor) {
            this.taskExecutor = taskExecutor;
            runningThreads = new LinkedHashMap<>();
            threadQueue = new LinkedList<>();
            //this.LOCK = LOCK;
            queryCounter = 0;
        }

        synchronized public void addThreadToQueue(SsbApiCallTask thread) {
            threadQueue.add(thread);
        }

        synchronized public SsbApiCallTask getFirstThreadInQueue() {
            return threadQueue.get(0);
        }

        public synchronized void increaseQueryCounter(int querySize) {
            queryCounter += querySize;
        }

        public synchronized void resetQueryCounter() {
            queryCounter = 0;
        }

        public synchronized int getQueryCounter() {
            return queryCounter;
        }

        public synchronized void removeThreadFromList(String tableCode) {
            runningThreads.remove(tableCode);
        }

        @Override
        public void run() {
            System.out.println("[ThreadManager] Starting up the thread manager.");
            while (true) {
                try {
                    synchronized (this) {
                        if (threadQueue.size() == 0) {
                            System.out.println("[ThreadManager] Waiting for more threads.");
                            this.wait();
                        }
                    }
                    System.out.println("[ThreadManager] Getting first thread in queue.");
                    System.out.println("[ThreadManager] Queue size: " + threadQueue.size());
                    SsbApiCallTask nextTable = getFirstThreadInQueue();

                    if (getQueryCounter() >= 30 || getQueryCounter() + nextTable.getQuerySize() >= 30) {
                        long alreadyWaitedTime = System.nanoTime() - threadQueue.get(0).getStartTime();
                        System.out.println(System.nanoTime());
                        System.out.println(threadQueue.get(0).getStartTime());
                        System.out.println("[ThreadManager] Time since first thread nano seconds: " + alreadyWaitedTime);
                        long alreadyWaitedTimeSeconds = TimeUnit.SECONDS.convert(alreadyWaitedTime, TimeUnit.NANOSECONDS);
                        System.out.println("[ThreadManager] Time since first thread seconds: " + alreadyWaitedTimeSeconds);
                        try {
                            System.out.println("[ThreadManager] Sleeping for 60s");
                            TimeUnit.SECONDS.sleep(60 - alreadyWaitedTimeSeconds);
                            resetQueryCounter();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    taskExecutor.execute(threadQueue.get(0));
                    threadQueue.remove(0);
                } catch (InterruptedException ie) {

                }
            }
        }
    }

    private static class SsbApiCallTask implements Runnable {

        private final ThreadManager monitor;
        private final SsbApiCall ssbApiCall;
        private final int querySize;
        private final String tableCode;
        private long startTime;

        public SsbApiCallTask(SsbApiCall ssbApiCall, String tableCode, int querySize, long startTime, ThreadManager monitor) {
            this.ssbApiCall = ssbApiCall;
            this.tableCode = tableCode;
            this.querySize = querySize;
            this.monitor = monitor;
            this.startTime = startTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public int getQuerySize() {
            return querySize;
        }

        @Override
        public void run() {
            try {
                System.out.println("[" + tableCode + "] STARTING. " + ssbApiCall.getMetadata().getTitle() + ". Size: " + ssbApiCall.getQuerySize());
                monitor.increaseQueryCounter(querySize);
                ssbApiCall.tableApiCall();
                //monitor.decreaseQueryCounter(querySize);
                monitor.removeThreadFromList(tableCode);
                System.out.println("[" + tableCode + "] DONE. " + ssbApiCall.getMetadata().getTitle());
            } catch (IOException e) {
                System.err.println(ssbApiCall.getMetadata().getTitle());
                e.printStackTrace();
            }
        }
    }
}
