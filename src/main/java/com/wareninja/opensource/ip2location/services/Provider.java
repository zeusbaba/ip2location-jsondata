package com.wareninja.opensource.ip2location.services;


import java.util.List;

import com.google.gson.JsonObject;
import com.wareninja.opensource.ip2location.config.YamlConfiguration;

public interface Provider {

    default void transfer(final BulkService bulkService,YamlConfiguration config,  final Runnable closeConnections) {
        long count = this.getCount();
        final int limit = config.getElastic().batchSize;
        int skip = 0;

        if (count != 0 && config.getElastic().dropDataset)
            bulkService.dropDataSet();

        while (count >= limit) {
            List content = this.buildJSONContent(skip, limit);
            bulkService.proceed(content);
            count -= limit;
            skip += limit;
        }

        if (count > 0) {
            List content = this.buildJSONContent(skip, (int) count);
            bulkService.proceed(content);
        }

        closeConnections.run();
    }

    long getCount();

    List<JsonObject> buildJSONContent(int skip, int limit);
}
