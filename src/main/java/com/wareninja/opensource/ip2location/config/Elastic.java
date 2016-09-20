package com.wareninja.opensource.ip2location.config;


public class Elastic {

    public String host;
    public Integer port;
    public String dateFormat;
    public Boolean dropDataset = true;
    public Integer batchSize = 400;
    public String indexName;
    public String typeName;
    public String clusterName;
    public Auth auth;

    @Override
	public String toString() {
		return "ElasticConfig [host=" + host + ", port=" + port + ", dateFormat=" + dateFormat 
				+ ", dropDataset=" + dropDataset + ", batchSize="+batchSize 
				+ ", indexName=" + indexName + ", typeName=" + typeName
				+ ", clusterName=" + clusterName + ", auth=" + auth + "]";
	}
   
}
