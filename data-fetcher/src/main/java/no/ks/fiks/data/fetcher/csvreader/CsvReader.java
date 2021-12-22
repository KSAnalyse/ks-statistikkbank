package no.ks.fiks.data.fetcher.csvreader;

import no.ks.fiks.data.fetcher.tableinfo.TabellFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CsvReader {
    private List<TabellFilter> tablesAndFilters;

    public CsvReader() {
        this.tablesAndFilters = new ArrayList<>();
    }

    public void readFromCsv() throws FileNotFoundException {
        try (Scanner scanner = new Scanner(new File("data-fetcher/src/main/resources/Tabellfilter.csv"))) {
            scanner.nextLine();
            while (scanner.hasNextLine()) {
                String[] splitter = scanner.nextLine().split(";", 5);

                String kildeId = splitter[0];
                String tabellnummer = splitter[1];
                String lagTabellFilter = splitter[2];
                String hentDataFilter = splitter[3];
                if (lagTabellFilter.equals("NULL"))
                    lagTabellFilter = "";
                if (hentDataFilter.equals("NULL"))
                    hentDataFilter = "";
                tablesAndFilters.add(new TabellFilter(kildeId, tabellnummer, lagTabellFilter, hentDataFilter));
            }
        }
    }

    public List<TabellFilter> getTablesAndFilters() {
        return tablesAndFilters;
    }
}
