package no.ks.fiks.data.fetcher.api.v1.scheduler;

import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import no.ks.fiks.data.fetcher.tableinfo.TableFilterAndGroups;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

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

        for (TableFilterAndGroups s : tableFilterAndGroups) {
            if (!s.getTabellnummer().equals("11211"))
                taskExecutor.execute(new SsbApiCallTask(s.getTabellnummer()));
        }
    }

    private static class SsbApiCallTask implements Runnable {

        private String tablenumber;
        private SsbApiCall ssbApiCall;

        public SsbApiCallTask(String tablenumber) {
            this.tablenumber = tablenumber;
            ssbApiCall = new SsbApiCall(tablenumber, 5, "131", "104", "214", "231", "127");
        }

        public void run() {
            try {
                ssbApiCall.tableApiCall();
                System.out.println(tablenumber + " " + ssbApiCall.getQuerySize());
            } catch (IOException e) {
                System.err.println(tablenumber);
                e.printStackTrace();
            }
        }
    }
}
