package com.wareninja.opensource.ip2location.services;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import com.google.gson.JsonObject;
import com.wareninja.opensource.ip2location.config.ElasticConfiguration;
import com.wareninja.opensource.ip2location.config.MyUtils;
import com.wareninja.opensource.ip2location.config.YamlConfiguration;

public class ElasticBulkService implements BulkService {

	private final Logger logger = Logger.getLogger(ElasticBulkService.class);
    private final YamlConfiguration config;
    private final ElasticConfiguration client;
    private final BulkProcessor bulkProcessor;

    public ElasticBulkService(final YamlConfiguration config, final ElasticConfiguration client) {
        this.config = config;
        this.client = client;

        this.bulkProcessor = BulkProcessor.builder(client.getClient(), new BulkProcessorListener())
            .setBulkActions(config.getElastic().batchSize)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
    }

    @Override
    public void proceed(List content) {
        try {
        	logger.debug(">> Transferring data began to elasticsearch... " + content.size() + " records");
            final String indexName = config.getElastic().indexName;
            final String typeName = config.getElastic().typeName;
            for (Object o : content) {
                //JsonObject doc = (new JsonParser()).parse( MyUtils.getGson().toJson(o) ).getAsJsonObject();
            	JsonObject doc = (JsonObject) o;
                //-Object id = doc.has("ip_from")?doc.get("ip_from") : System.currentTimeMillis();//("_id");
            	Object id; // = MyUtils.generateGenericOid();// System.currentTimeMillis();// default value
            	if (doc.has("ip_from")) {
            		id = doc.get("ip_from");
            	}
            	else if (doc.has("_id")) {
            		id = doc.get("_id");
            		doc.remove("_id");
            	}
            	/*else if (doc.has("_created")) {
            		id = doc.get("_created");
            	}*/
            	else {
            		id = MyUtils.generateGenericOid();// System.currentTimeMillis();// default value
            	}
                IndexRequest indexRequest = new IndexRequest(indexName, typeName, String.valueOf(id));
                //doc.remove("_id");
                indexRequest.source(doc.toString().getBytes(Charset.forName("UTF-8")));
                bulkProcessor.add(indexRequest);
            }
        } catch (Exception ex) {
        	logger.error(ex.getMessage(), ex.fillInStackTrace());
        }
    }

    @Override
    public void close() {
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
        	logger.error(ex.getMessage(), ex.fillInStackTrace());
        }
    }

    @Override
    public void dropDataSet() {
        final String indexName = config.getElastic().indexName;
        IndicesAdminClient admin = client.getClient().admin().indices();
        IndicesExistsRequestBuilder builder = admin.prepareExists(indexName);
        if (builder.execute().actionGet().isExists()) {
            DeleteIndexResponse delete = admin.delete(new DeleteIndexRequest(indexName)).actionGet();
            if (delete.isAcknowledged())
            	logger.debug(String.format("The current index %s was deleted.", indexName));
            else
            	logger.debug(String.format("The current index %s was not deleted.", indexName));
        }
    }

}
