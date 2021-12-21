package no.ks.fiks.data.fetcher.tableinfo;

import java.util.*;

public class TabellFilter {
    private final String kildeId;
    private final String tabellnummer;
    private final Map<String, List<String>> lagTabellFilter;
    private final Map<String, List<String>> hentDataFilter;

    public TabellFilter(String kildeId, String tabellnummer, String lagTabellFilterString, String hentDataFilterString) {
        this.kildeId = kildeId;
        this.tabellnummer = tabellnummer;
        this.lagTabellFilter = splitStringIntoMap(lagTabellFilterString);
        this.hentDataFilter = splitStringIntoMap(hentDataFilterString);

    }

    private Map<String, List<String>> splitStringIntoMap(String filterString) {
        String[] mainSplit = filterString.split("&");
        if (mainSplit[0].isBlank())
            return null;
        Map<String, List<String>> filter = new LinkedHashMap<>();
        for (String s : mainSplit) {
            List<String> filterList = new ArrayList<>();
            if (s.contains("Add")) {
                String[] sSplit = s.split("=");
                filterList.addAll(Arrays.asList(sSplit[1].split(":")));
                filter.put(sSplit[0], filterList);
            } else {
                if (s.contains("!=")) {
                    String[] sSplit = s.split("!=");
                    filterList.addAll(Arrays.asList(sSplit[1].split(",")));
                    filter.put("!" + sSplit[0], filterList);
                } else if (s.contains("=")) {
                    String[] sSplit = s.split("=");
                    filterList.addAll(Arrays.asList(sSplit[1].split(",")));
                    filter.put(sSplit[0], filterList);
                }
            }
        }
        return filter;
    }

    public String getKildeId() {
        return kildeId;
    }

    public String getTabellnummer() {
        return tabellnummer;
    }

    public Map<String, List<String>> getLagTabellFilter() {
        return lagTabellFilter;
    }

    public Map<String, List<String>> getHentDataFilter() {
        return hentDataFilter;
    }
}
