package no.ks.fiks.data.fetcher.api.v1.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ks.fiks.data.fetcher.api.v1.service.DataFetcherService;
import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TabellFilter;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadata;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadataVariables;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableScheduling
public class Scheduler {

    private final TaskExecutor taskExecutor;
    private final ThreadManager manager;
    private final List<TabellFilter> tabellFilters;

    public Scheduler() {
        //manager = new ThreadManager(new SchedulerConfig().taskExecutorConfiguration());
        this.taskExecutor = new SchedulerConfig().taskExecutorConfiguration();
        this.manager = new ThreadManager(taskExecutor);
        CsvReader csvReader = new CsvReader();
        try {
            csvReader.readFromCsv();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        tabellFilters = csvReader.getTablesAndFilters();
    }

    @Scheduled(cron = "0 0 9 * * MON-FRI", zone = "Europe/Paris")
    public void runApiCall() {
        //30 reqs per 60s
        ObjectMapper om = new ObjectMapper();
        //ThreadManager manager = new ThreadManager(taskExecutor);
        String json;
        List<String> filters = new ArrayList<>();

        for (TabellFilter s : tabellFilters) {
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
             */

            /*if (s.getHentDataFilter() != null) {
                System.out.println("Add fetch filters");
                for (String filterCode : s.getHentDataFilter().keySet()) {
                    String jsonFilter = "\"filters\":[{\"code\":\"%s\", \"values\":[%l]}]";
                    String filterList = s.getHentDataFilter().get(filterCode).stream().collect(Collectors.joining("\",\"", "\"", "\""));
                    jsonFilter = jsonFilter.replaceAll("%s", filterCode);
                    jsonFilter = jsonFilter.replaceAll("%l", filterList);
                    System.out.println(jsonFilter);
                }
            }*/


            if (!s.getTabellnummer().equals("11211") && !s.getTabellnummer().equals("12247")) {
                System.out.println("[runApiCall] Adding " + s.getTabellnummer() + " to queue.");
                String tableName = String.format("%s.[%s]", s.getKildeId().toLowerCase(), s.getTabellnummer());
                manager.addThreadToQueue(new ThreadQuery(s.getTabellnummer(), tableName, "create", null));
                manager.addThreadToQueue(new ThreadQuery(s.getTabellnummer(), tableName, "truncate", null));
                manager.addThreadToQueue(new ThreadQuery(s.getTabellnummer(), tableName, "insert", null));
                System.out.println("[runApiCall] Added " + s.getTabellnummer() + " to queue.");
            }
        }
        taskExecutor.execute(manager);

        //TODO: Remove this
        /*while (true) {

        }*/
    }

    synchronized public void addThreadToQueue(ThreadQuery threadQuery) {
        manager.addThreadToQueue(threadQuery);
    }

    private static class ThreadManager implements Runnable {

        //private final List<String> threadQueries;
        private final List<ThreadQuery> threadQueries;
        private final TaskExecutor taskExecutor;
        private final DataFetcherService dfs;
        private final String username;
        private final String password;
        private int queryCounter;
        private LocalDateTime lastTokenFetch;
        private String apiToken;

        public ThreadManager(TaskExecutor taskExecutor) {
            this.taskExecutor = taskExecutor;
            dfs = new DataFetcherService();
            //threadQueries = new ArrayList<>();
            threadQueries = new ArrayList<>();
            queryCounter = 0;

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream("data-fetcher/src/main/resources/login.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }

            username = properties.getProperty("username");
            password = properties.getProperty("password");
        }

        synchronized public void addThreadToQueue(ThreadQuery query) {
            threadQueries.add(query);
        }

        synchronized public int getQueryCounter() {
            return queryCounter;
        }

        synchronized public String getToken() {
            long expirationSeconds = 10800;
            if (lastTokenFetch == null || lastTokenFetch.isBefore(LocalDateTime.now().minus(expirationSeconds, ChronoUnit.SECONDS)))
                fetchToken();

            return apiToken;
        }

        synchronized public ThreadQuery getFirstThreadQueryInQueue() {
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
            while (!threadQueries.isEmpty()) {
                try {
                    /*while (threadQueries.size() == 0) {
                        System.out.println("[ThreadManager] Waiting for more threads.");
                        TimeUnit.SECONDS.sleep(10);
                    }*/

                    ThreadQuery threadQuery = getFirstThreadQueryInQueue();

                    SsbApiCall ssbApiCall = null;
                    // Only create and insert needs to fetch data from SSB to work
                    if (threadQuery.getQueryType().equals("create") || threadQuery.getQueryType().equals("insert")) {
                        // If by fetching SsbApiCall data makes the manager reach the limit, wait 1min
                        if (getQueryCounter() + 1 >= 30) {
                            System.out.println("[ThreadManager] Sleeping for 60s 1");
                            TimeUnit.SECONDS.sleep(60);
                            resetQueryCounter();
                        }
                        ssbApiCall = new SsbApiCall(threadQuery.getTableCode(), 5, "131",
                                "104", "214", "231", "127");
                    }


                    threadQuery.setSsbApiCall(ssbApiCall);
                    increaseQueryCounter(ssbApiCall.getQuerySize());

                    /*
                    JsonNode jsonObject = mapper.readTree(getFirstJsonInQueue());
                    SsbApiCall ssbApiCall = new SsbApiCall(jsonObject.get("tableCode").asText(), 5, "131",
                            "104", "214", "231", "127");
                    increaseQueryCounter(ssbApiCall.getQuerySize() + 3);

                     */

                    if (getQueryCounter() >= 30) {
                        System.out.println("[ThreadManager] Sleeping for 60s 2");
                        TimeUnit.SECONDS.sleep(60);
                        resetQueryCounter();
                    }

                    //taskExecutor.execute(new SsbApiCallTask(dfs, ssbApiCall, getFirstJsonInQueue(), jsonObject.get("tableCode").asText(), this));
                    taskExecutor.execute(new SsbApiCallTask(threadQuery, this));
                    threadQueries.remove(0);
                } catch (InterruptedException ie) {
                    System.out.println("OH NO!");
                }
            }
        }

        private void fetchToken() {
            System.out.println("[fetchToken] Fetching token!");

            try {

                lastTokenFetch = LocalDateTime.now();
                URL url;
                url = new URL("http://localhost:8080/public/users/login"/*?email=" + username + "&password=" + password*/);
                String encoding = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json; utf-8");
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestProperty("Authorization", "Basic" + encoding);
                connection.setDoOutput(true);

                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                        StandardCharsets.UTF_8));

                String responseString;
                StringBuilder response = new StringBuilder();

                while (true) {
                    String responseLine;
                    if ((responseLine = br.readLine()) == null) {
                        responseString = response.toString();
                        break;
                    }

                    response.append(responseLine.trim());
                }
                connection.disconnect();

                System.out.println("Check out this responseString!: " + responseString);
                apiToken = responseString;
                //return responseString;
            } catch (MalformedURLException e) {
                e.printStackTrace();
                System.out.println("[ERROR]: MalformedURLException in apiCall.");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("[ERROR]: IOException in apiCall.");
            }
        }
    }

    private static class SsbApiCallTask implements Runnable {

        private final ThreadManager manager;
        /*
        private final SsbApiCall ssbApiCall;
        private final String tableCode;
        private final DataFetcherService dfs;
        private final String json;
        */
        //private LocalDateTime lastTokenFetch;
        private ThreadQuery threadQuery;
        private String apiToken;

        /*
        public SsbApiCallTask(DataFetcherService dfs, SsbApiCall ssbApiCall, String json, String tableCode, ThreadManager manager) {
            this.dfs = dfs;
            this.ssbApiCall = ssbApiCall;
            this.json = json;
            this.tableCode = tableCode;
            this.manager = manager;
        }
         */

        public SsbApiCallTask(ThreadQuery threadQuery, ThreadManager manager) {
            this.threadQuery = threadQuery;
            this.manager = manager;
        }

        @Override
        public void run() {
            apiToken = manager.getToken();

            switch (threadQuery.getQueryType()) {
                case "create":
                    System.out.println("create");
                    String columnDeclarations = createColumnDeclarations(threadQuery.getSsbApiCall().getMetadata());
                    String query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))",
                            threadQuery.getTableName(), columnDeclarations);
                    apiCall("create-table", query);
                    break;
                case "insert":
                    System.out.println("insert");
                    break;
                case "drop":
                case "truncate":
                    System.out.println("drop/truncate");
                    break;
                default:
                    System.out.println("default");
                    break;
            }
            /*
            System.out.println("[" + tableCode + "] STARTING. " + ssbApiCall.getMetadata().getTitle() + ". Size: " + ssbApiCall.getQuerySize());
            System.out.println("[" + tableCode + "] createTable: " + dfs.createTable(json));
            System.out.println("[" + tableCode + "] truncateTable: " + dfs.truncateTable(json));
            System.out.println("[" + tableCode + "] insertData: " + dfs.insertData(json));
            System.out.println("[" + tableCode + "] DONE. " + ssbApiCall.getMetadata().getTitle());
             */
        }

        /**
         * Creates the column declarations needed for the create query based on the metadata provided.
         * <p>
         * For each metadata code in the metadata fetched creates a pair of columns on the form:
         * [[metadata name]navn] [varchar] (largestValue)
         * [[metadata name]kode] [varchar] (largestValue)
         * e.g
         * [Regionkode] [varchar] (5)
         * [Regionnavn] [varchar] (24)
         * <p>
         * If the metadata column is "Tid" then it uses the metadata code instead.
         *
         * @param metadata the object containing all the metadata information
         * @return the column declarations on the mentioned form
         */
        private String createColumnDeclarations(SsbMetadata metadata) {
            StringBuilder columnDeclarations = new StringBuilder();

            Iterator<SsbMetadataVariables> iterator = metadata.getVariables().iterator();

            while (iterator.hasNext()) {
                SsbMetadataVariables smv = iterator.next();

                if (smv.getCode().equals("Tid")) {
                    columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getCode()),
                            smv.getLargestValue()));
                    columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getCode()),
                            smv.getLargestValueText()));
                } else {
                    columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValue()));
                    columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValueText()));
                }

                if (iterator.hasNext()) {
                    columnDeclarations.append(", ");
                }
            }
            return columnDeclarations.toString();
        }

        /**
         * Connects and posts to the database-service API.
         * Connects to the endpoint given and posts the payload.
         *
         * @param endpoint the endpoint to where to do the post request
         * @param payload  the payload to be posted
         * @return the response from the API
         */
        private String apiCall(String endpoint, String payload) {


        /*if (Calendar.getInstance().getTime().getTime() - lastTokenFetch.getTime() >= 10600000 || lastTokenFetch == null) {
            fetchToken();
        }*/

            try {
                URL url = new URL("http://localhost:8080/api/v1/" + endpoint);

                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json; utf-8");
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestProperty("Authorization", apiToken);
                connection.setDoOutput(true);

                OutputStream os = connection.getOutputStream();
                byte[] input = payload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);

                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                        StandardCharsets.UTF_8));

                String responseString;
                StringBuilder response = new StringBuilder();

                while (true) {
                    String responseLine;
                    if ((responseLine = br.readLine()) == null) {
                        responseString = response.toString();
                        break;
                    }

                    response.append(responseLine.trim());
                }
                connection.disconnect();

                return responseString;
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return "[ERROR]: MalformedURLException in apiCall.";
            } catch (IOException e) {
                e.printStackTrace();
                return "[ERROR]: IOException in apiCall.";
                //return addRedColorToString("[ERROR]: IOException in apiCall.");
            }
        }
    }
}
