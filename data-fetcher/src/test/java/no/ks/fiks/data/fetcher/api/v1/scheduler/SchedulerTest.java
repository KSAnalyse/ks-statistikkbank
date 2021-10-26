package no.ks.fiks.data.fetcher.api.v1.scheduler;


import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TableFilterAndGroups;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.FileNotFoundException;
import java.util.List;

class SchedulerTest {

    @Test
    public void printMessage() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(30);
        taskExecutor.setMaxPoolSize(30);
        taskExecutor.initialize();
        new Scheduler(taskExecutor).runApiCall();
    }

    @Test
    void splitStringIntoMap() {
        TableFilterAndGroups test1 = new TableFilterAndGroups(
                "SSB",
                "12367",
                "Økonomi",
                "KOKregnskapsomfa0000=None",
                "KOKregnskapsomfa0000=A"
        );
        TableFilterAndGroups test2 = new TableFilterAndGroups(
                "SSB",
                "12905",
                "Eiendomsforvaltning",
                "",
                "KOKkjoenn0000!=0&ContentsCode=KOSantlarergrsk0000"
        );
        System.out.println(test1.getLagTabellFilter());
        System.out.println(test2.getLagTabellFilter());
        System.out.println(test1.getHentDataFilter());
        System.out.println(test2.getHentDataFilter());
    }

    @Test
    void readFromCsv() {
        CsvReader csvReader = new CsvReader();
        try {
            csvReader.readFromCsv();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }
        List<TableFilterAndGroups> test = csvReader.getTablesAndFilters();

        test.stream().map((k) -> (k.getKildeId() + " " + k.getTabellnummer() + " " + k.getGruppenavn() + " " +
                k.getLagTabellFilter() + " " + k.getHentDataFilter())).forEach(System.out::println);
    }
}