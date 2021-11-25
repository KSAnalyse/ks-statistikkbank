package no.ks.fiks.data.fetcher.api.v1.scheduler;

import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TableFilterAndGroups;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
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

        SsbApiCall ssbApiCall;
        for (TableFilterAndGroups s : tableFilterAndGroups) {
            ssbApiCall = new SsbApiCall(s.getTabellnummer(), 5, "131", "104", "214", "231", "127");
            if (!s.getTabellnummer().equals("11211"))
                taskExecutor.execute(new SsbApiCallTask(ssbApiCall));
        }
    }

    private static class SsbApiCallTask implements Runnable {

        private final SsbApiCall ssbApiCall;

        public SsbApiCallTask(SsbApiCall ssbApiCall) {
            this.ssbApiCall = ssbApiCall;
        }

        @Override
        public void run() {
            try {
                System.out.println(ssbApiCall.getMetadata().getTitle() + " " + ssbApiCall.getQuerySize());
                ssbApiCall.tableApiCall();
            } catch (IOException e) {
                System.err.println(ssbApiCall.getMetadata().getTitle());
                e.printStackTrace();
            }
        }
    }
}
