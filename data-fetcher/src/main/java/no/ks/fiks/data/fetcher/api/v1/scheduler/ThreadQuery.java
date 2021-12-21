package no.ks.fiks.data.fetcher.api.v1.scheduler;

import no.ks.fiks.ssbAPI.APIService.SsbApiCall;

import java.util.List;
import java.util.Map;

public class ThreadQuery {
    private final String tableName, tableCode;
    private final String queryType;
    private final Map<String, List<String>> filters;
    private SsbApiCall ssbApiCall;

    public ThreadQuery(String tableCode, String tableName, String queryType, Map<String, List<String>> filters) {
        this.tableCode = tableCode;
        this.tableName = tableName;
        this.queryType = queryType;
        this.filters = filters;
    }

    public String getQueryType() {
        return queryType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableCode() {
        return tableCode;
    }

    public Map<String, List<String>> getFilters() {
        return filters;
    }

    public SsbApiCall getSsbApiCall() {
        return ssbApiCall;
    }

    public void setSsbApiCall(SsbApiCall ssbApiCall) {
        this.ssbApiCall = ssbApiCall;
    }
}
