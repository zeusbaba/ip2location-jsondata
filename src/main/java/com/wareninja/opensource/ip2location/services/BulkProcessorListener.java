package com.wareninja.opensource.ip2location.services;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

public class BulkProcessorListener implements BulkProcessor.Listener {

	private final Logger logger = Logger.getLogger(BulkProcessorListener.class);

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
        	logger.error(response.buildFailureMessage());
        }
        else {
        	logger.debug(String.format("Data transfer successfully completed.(%d)", response.getItems().length));
        }

    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    	logger.error("Transfer failed.");
    	logger.error(failure.getMessage() +" | "+ failure.fillInStackTrace());
    }
}
