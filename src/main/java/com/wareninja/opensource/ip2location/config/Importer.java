package com.wareninja.opensource.ip2location.config;


public class Importer {

	private static Integer RECORDS_PER_FILE_DEFAULT = 50000;// items/records per output file
	private static Boolean WITH_IP_ADDRESS = true; // if true, then will generate also human-readable ip address
	public static String DEFAULT_FILE_TYPE = "ip2location";
	
    public String filePath;
    public String fileName;
    public String fileType = DEFAULT_FILE_TYPE;
    public Integer recordPerFile = RECORDS_PER_FILE_DEFAULT;
    public boolean withIpAddress = WITH_IP_ADDRESS;
    public String dbType;
    public boolean debugMode;
    
	@Override
	public String toString() {
		return "Importer [filePath=" + filePath + ", fileName=" + fileName + ", fileType=" + fileType 
				+ ", recordPerFile=" + recordPerFile
				+ ", withIpAddress=" + withIpAddress + ", dbType=" + dbType + ", debugMode=" + debugMode + "]";
	}
}
