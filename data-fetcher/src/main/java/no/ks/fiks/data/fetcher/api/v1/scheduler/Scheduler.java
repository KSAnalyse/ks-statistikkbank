package no.ks.fiks.data.fetcher.api.v1.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ks.fiks.data.fetcher.api.v1.service.DataFetcherService;
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

    public Scheduler(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;

        CsvReader csvReader = new CsvReader();
        try {
            csvReader.readFromCsv();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        tableFilterAndGroups = csvReader.getTablesAndFilters();
    }

    public void runApiCall() {
        //30 reqs per 60s
        ObjectMapper om = new ObjectMapper();
        ThreadManager manager = new ThreadManager(taskExecutor);
        taskExecutor.execute(manager);
        String json;

        for (TableFilterAndGroups s : tableFilterAndGroups) {
            ObjectNode jsonObject = om.createObjectNode();
            ObjectNode filterObject = om.createObjectNode();

            jsonObject.put("tableCode", s.getTabellnummer());

            jsonObject.put("schemaName", s.getKildeId().toLowerCase());

            json = String.format("{\"tableCode\":\"%s\", \"schemaName\":\"%s\"}", s.getTabellnummer(),
                    s.getKildeId().toLowerCase());
            /*TODO: Filters
            filters = s.getLagTabellFilter();

            if (filters != null) {
                for (String filterName: filters.keySet()) {
                    filterObject.put("code", filterName);
                    filterObject.set()
                    String [] test = {"hei", "hei2"};
                    jsonObject.putArray("filters");
                }
                System.out.println("Add create filters");
            }

            if (s.getHentDataFilter() != null)
                System.out.println("Add fetch filters");


             */

            if (!s.getTabellnummer().equals("11211") && !s.getTabellnummer().equals("12247")) {
                System.out.println("[runApiCall] Adding " + s.getTabellnummer() + " to queue.");
                manager.addThreadToQueue(json);
                System.out.println("[runApiCall] Added " + s.getTabellnummer() + " to queue.");
            }
        }

        //TODO: Remove this
        while (true) {

        }
    }

    private static class ThreadManager implements Runnable {

        private List<String> threadQueries;
        private TaskExecutor taskExecutor;
        private DataFetcherService dfs;
        private int queryCounter;

        public ThreadManager(TaskExecutor taskExecutor) {
            this.taskExecutor = taskExecutor;
            dfs = new DataFetcherService();
            threadQueries = new ArrayList<>();
            queryCounter = 0;
        }

        synchronized public void addThreadToQueue(String json) {
            threadQueries.add(json);
        }

        synchronized public int getQueryCounter() {
            return queryCounter;
        }

        synchronized public String getFirstJsonInQueue() {
            return threadQueries.get(0);
        }

        synchronized public void resetQueryCounter() {
            queryCounter = 0;
        }

        synchronized public void increaseQueryCounter(int querySize) {
            queryCounter += querySize;
        }

        @Override
        public void run() {
            System.out.println("[ThreadManager] Starting up the thread manager.");
            ObjectMapper mapper = new ObjectMapper();
            while (true) {
                try {
                    while (threadQueries.size() == 0) {
                        System.out.println("[ThreadManager] Waiting for more threads.");
                        TimeUnit.SECONDS.sleep(10);
                    }

                    if (getQueryCounter() >= 30 || getQueryCounter() + 1 > 30) {
                        TimeUnit.SECONDS.sleep(60);
                        resetQueryCounter();
                    }

                    System.out.println("Wut?");
                    increaseQueryCounter(1);
                    JsonNode jsonObject = mapper.readTree(getFirstJsonInQueue());
                    SsbApiCall ssbApiCall = new SsbApiCall(jsonObject.get("tableCode").asText(), 5, "131",
                            "104", "214", "231", "127");
                    SsbApiCallTask sact = new SsbApiCallTask(dfs, ssbApiCall, getFirstJsonInQueue(), jsonObject.get("tableCode").asText(),
                            ssbApiCall.getQuerySize(), System.nanoTime(), this);

                    if (getQueryCounter() + sact.querySize >= 30) {
                        System.out.println("[ThreadManager] Sleeping for 60s");
                        TimeUnit.SECONDS.sleep(60);
                        resetQueryCounter();
                    }

                    taskExecutor.execute(sact);
                    threadQueries.remove(0);
                } catch (InterruptedException ie) {
                    System.out.println("OH NO!");
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    System.out.println("[ERROR] JsonProcessingException when fetching table code from json object.");
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
        private DataFetcherService dfs;
        private final String json;

        public SsbApiCallTask(DataFetcherService dfs, SsbApiCall ssbApiCall, String json, String tableCode,
                              int querySize, long startTime, ThreadManager monitor) {
            this.dfs = dfs;
            this.ssbApiCall = ssbApiCall;
            this.json = json;
            this.tableCode = tableCode;
            this.querySize = querySize;
            this.monitor = monitor;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            System.out.println("[" + tableCode + "] STARTING. " + ssbApiCall.getMetadata().getTitle() + ". Size: " + ssbApiCall.getQuerySize());

            // Both createTable and insertData uses the SsbApiCall class which means two extra queries
            monitor.increaseQueryCounter(querySize + 2);
            System.out.println(dfs.createTable(json));
            System.out.println(dfs.insertData(json));
            System.out.println("[" + tableCode + "] DONE. " + ssbApiCall.getMetadata().getTitle());
            /*try {
                System.out.println("[" + tableCode + "] STARTING. " + ssbApiCall.getMetadata().getTitle() + ". Size: " + ssbApiCall.getQuerySize());
                monitor.increaseQueryCounter(querySize);
                dfs.createTable(json);
                dfs.insertData(json);
                //ssbApiCall.tableApiCall();
                System.out.println("[" + tableCode + "] DONE. " + ssbApiCall.getMetadata().getTitle());
            } catch (IOException e) {
                System.err.println(ssbApiCall.getMetadata().getTitle());
                e.printStackTrace();
            }

             */
        }
    }
}
