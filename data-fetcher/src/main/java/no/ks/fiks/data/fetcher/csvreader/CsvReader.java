package no.ks.fiks.data.fetcher.csvreader;

import no.ks.fiks.data.fetcher.tableinfo.TableFilterAndGroups;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CsvReader {
    private List<TableFilterAndGroups> tablesAndFilters;

    public CsvReader() {
        this.tablesAndFilters = new ArrayList<>();
    }

    public void readFromCsv() throws FileNotFoundException {
        try (Scanner scanner = new Scanner(new File("data-fetcher/src/main/resources/Tabellfilter og grupper.csv"));) {
            scanner.nextLine();
            while (scanner.hasNextLine()) {
                String[] splitter = scanner.nextLine().split(";", 5);

                String kildeId = splitter[0];
                String tabellnummer = splitter[1];
                String gruppe = splitter[2];
                String lagTabellFilter = splitter[3];
                String hentDataFilter = splitter[4];

                tablesAndFilters.add(new TableFilterAndGroups(kildeId, tabellnummer, gruppe, lagTabellFilter, hentDataFilter));
            }
        }
    }

    public List<TableFilterAndGroups> getTablesAndFilters() {
        return tablesAndFilters;
    }
}
