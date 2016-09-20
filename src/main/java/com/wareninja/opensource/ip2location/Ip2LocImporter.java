
/***
 *   Copyleft 2016 - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yilmaz@guleryuz.net 
 *  @see https://github.com/WareNinja | http://www.BeerStorm.net 
 *  
 *  disclaimer: I code for fun, dunno what I'm coding about!
 *  
 */

package com.wareninja.opensource.ip2location;

import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.JsonObject;
import com.wareninja.opensource.ip2location.config.*;
import com.wareninja.opensource.ip2location.services.*;

/**
 * load & parse data from IP2LOCATION's CSV file
 * then it can 
 * - generate JSON files, ready for import into MongoDB!
 * - OR directly import into Elastic Search!  
 * 
 * 
 * mvn clean install
 * this will generate executable jar file
 * then ... 
 * Usage: 
 * 	java -jar ip2location-jsondata-1.0.x.jar -f importer.conf
 * 
 * see example.conf file for details.
 * 
 * 1) For MongoDB: 
 * the generated JSON files are ready for import to MongoDB using 'mongoimport' 
 * for example, using this command;
 * 		mongoimport -h mongo_db_url:port -d db_name -c collection_name -u db_username -p db_pwd --upsert --file IP2LOCATION-LITE-DB9-1.json
 * 
 * to learn more about mongoimport, see: http://docs.mongodb.org/manual/reference/program/mongoimport
 *  
 *  2) For ElasticSearch:  
 *  you directly import all ip2location data into ElasticSearch! :-) 
 *  
 */
public class Ip2LocImporter {

	final static String TAG = Ip2LocImporter.class.getSimpleName();
	private final Logger logger = Logger.getLogger(Ip2LocImporter.class);
	
	private final String parameter;
    public Ip2LocImporter(String parameter) {
        this.parameter = parameter;
    }
	
	public static void main(String[] args) {
		
		configAssertion(args);
		
		String parameter = args[1];
		Ip2LocImporter dataGenerator = new Ip2LocImporter(parameter);
		dataGenerator.startApp();
	}
	
	public void startApp() {
		
		FileConfiguration fConfig = new FileConfiguration(parameter);
		/*YamlConfiguration yamlConfig = fConfig.getFileContent();
		if (yamlConfig !=null) {
        	processImport(yamlConfig);
        }*/
		java.util.Optional<YamlConfiguration> yamlConfig = java.util.Optional.ofNullable(fConfig.getFileContent());
		long begin = System.currentTimeMillis();
        try {
            yamlConfig.ifPresent(this::processImport);
        } finally {
        	logger.debug("Load duration: " + (System.currentTimeMillis() - begin) + "ms");
        }
	}
	
	public void processImport(YamlConfiguration config) {
		
		String filePath = config.getImporter().filePath;
		String sourceFileName = filePath + config.getImporter().fileName;
		Integer recordsPerFile = config.getImporter().recordPerFile;
		
		if(config.getImporter().debugMode) {
			logger.debug( String.format("input params: %s %s", sourceFileName, recordsPerFile) );
			logger.debug( String.format("configs: %s", config) );
		}
		
		BulkService bulkService = null;
		ElasticConfiguration elastic = null;
		if ( ("elastic".equalsIgnoreCase(config.getImporter().dbType) 
				|| "es".equalsIgnoreCase(config.getImporter().dbType) )
				&& (config.getElastic()!=null && config.getElastic().host!=null && config.getElastic().port!=null)
				) {
			
			elastic = new ElasticConfiguration(config);
			bulkService = new ElasticBulkService(config, elastic);
			
			if (config.getElastic().dropDataset) {
				bulkService.dropDataSet();
			}
		}
		Boolean isMongoDb = (elastic==null); //true;// mongo by default
		
		String outputFileName = "";
		
		ICsvListReader listReader = null;
		File sourceFile;
		File outputFile;
		List<String> outputRecords;
		List<JsonObject> outputJsonObjects;
		Long currentTime = 0L;
		Integer fileCounter = 0;
		try {
			sourceFile = new File( sourceFileName );
			
			listReader = new CsvListReader(
					new InputStreamReader( Files.asByteSource(sourceFile).openBufferedStream() )
						, CsvPreference.STANDARD_PREFERENCE
						);
			
            listReader.getHeader(true); // skip the header (can't be used with CsvListReader)
            
            final CellProcessor[] processors = getCellProcessors(sourceFile.getName());
            
            if(config.getImporter().debugMode) {
            	logger.debug(">> CsvListReader :: Importing... "+sourceFile.getName());
            }
            
            // base file name for the json files
            outputFileName = sourceFile.getName().substring(0, sourceFile.getName().lastIndexOf(".")) + "-";
            
            List<Object> recordList;
            
            Ip2LocData ip2LocData;
            outputRecords = new LinkedList<String>();
            outputJsonObjects = new LinkedList<JsonObject>();
            currentTime = System.currentTimeMillis();
            
            while( (recordList = listReader.read(processors)) != null ) {
                
            	//if(config.getImporter().debugMode) {
            	//	System.out.println(String.format("lineNo=%s, rowNo=%s, recordList=%s", listReader.getLineNumber(),listReader.getRowNumber(), recordList));
            	//}
            	
            	ip2LocData = new Ip2LocData();
            	// -> DB1.LITE
            	ip2LocData.ip_from = (Long)recordList.get(0);
            	ip2LocData.ip_to = (Long)recordList.get(1);
            	if (config.getImporter().withIpAddress) {
            		ip2LocData.ipaddress_from = MyUtils.ipNumber2ipAddress(ip2LocData.ip_from);
            		ip2LocData.ipaddress_to = MyUtils.ipNumber2ipAddress(ip2LocData.ip_to);
            	}
            	ip2LocData.country_code = (String)recordList.get(2);
            	ip2LocData.country_name = (String)recordList.get(3);
            	if (recordList.size()>=6) { // -> DB3.LITE
	            	ip2LocData.region_name = (String)recordList.get(4);
	            	ip2LocData.city_name = (String)recordList.get(5);
            	}
            	if (recordList.size()>=8) {// -> DB5.LITE
            		ip2LocData.setLocation( (Double)recordList.get(6), (Double)recordList.get(7) );
            	}
            	if (recordList.size()>=9) {// -> DB9.LITE
            		ip2LocData.zip_code = (String)recordList.get(8);
            	}
            	if (recordList.size()>=10) {// -> DB11.LITE
            		ip2LocData.time_zone = (String)recordList.get(9);
            	}
            	ip2LocData.fillInTimestamp( currentTime );
            	//if(config.getImporter().debugMode) {
            	//	System.out.println(String.format("the JSON= %s", ip2LocData.toJsonStringPrinting()));
            	//}
            	
            	outputRecords.add( ip2LocData.toJsonObject(isMongoDb).toString() );
            	outputJsonObjects.add( ip2LocData.toJsonObject(isMongoDb) );
            	if (outputRecords.size() >= recordsPerFile ) {

            		if (isMongoDb) {
	            		fileCounter++;
	            		outputFile = new File(
	            				filePath+outputFileName + fileCounter +".json"
	        					);
	            		Files.asCharSink(outputFile,  Charsets.UTF_8).writeLines( outputRecords );
            		}
            		else { // elastic search
            			bulkService.proceed(outputJsonObjects);
            		}
            		
            		outputJsonObjects.clear();
            		outputRecords.clear();
            	}
            }
            
            // ensure that all data is processed! 
            if (outputRecords.size() > 0 || outputJsonObjects.size()>0) {
            	if (isMongoDb) {
	            	fileCounter++;
	        		outputFile = new File(
	        				filePath+outputFileName + fileCounter +".json"
	    					);
	        		
	        		Files.asCharSink(outputFile,  Charsets.UTF_8).writeLines( outputRecords );
            	}
            	else { // elastic search
        			bulkService.proceed(outputJsonObjects);
        		}
            	
            	outputJsonObjects.clear();
        		outputRecords.clear();
            }
		}
		catch (Exception ex) {
			logger.error("ex: "+ ex.toString());
		}
		finally {
			logger.debug( String.format("Job completed!!! %s json-data files generated...", ""+fileCounter) );
		}
		
	}
	
	
	/**
	 * Sets up the processors used for the examples. If there are 10 CSV columns, so 10 processors must be defined. 
	 * Empty columns are read as null (hence the NotNull() for mandatory columns).
	 * @return the cell processors
	 */
	private static CellProcessor[] getCellProcessors() {
		return getCellProcessors("");
	}
	private static CellProcessor[] getCellProcessors(String sourceFileName) {
	        
	        final String emailRegex = "[a-z0-9\\._]+@[a-z0-9\\.]+"; // just an example, not very robust!
	        StrRegEx.registerMessage(emailRegex, "must be a valid email address");
	        
	        CellProcessor[] processors = null;
	        
	        sourceFileName = sourceFileName.toUpperCase();
	        if (sourceFileName.contains("DB1.")) {
	        	
	        	processors = new CellProcessor[] { 
	            		new NotNull(new ParseLong()), // ip_from
	            		new NotNull(new ParseLong()), // ip_to
	                    new NotNull(), // country_code
	                    new NotNull() // country_name
	            };
	        }
	        else if (sourceFileName.contains("DB3.")) {
	        	
	        	processors = new CellProcessor[] { 
	        			new NotNull(new ParseLong()), // ip_from
	            		new NotNull(new ParseLong()), // ip_to
	                    new NotNull(), // country_code
	                    new NotNull(), // country_name
	                    new NotNull(), // region_name
	                    new NotNull() // city_name
	            };
	        }
	        else if (sourceFileName.contains("DB5.")) {
	        	
	        	processors = new CellProcessor[] { 
	        			new NotNull(new ParseLong()), // ip_from
	            		new NotNull(new ParseLong()), // ip_to
	                    new NotNull(), // country_code
	                    new NotNull(), // country_name
	                    new NotNull(), // region_name
	                    new NotNull(), // city_name
	                    new Optional(new ParseDouble()), //  latitude
	                    new Optional(new ParseDouble()) // longitude
	            };
	        }
	        else if (sourceFileName.contains("DB9.")) {
	        	
	        	processors = new CellProcessor[] { 
	        			new NotNull(new ParseLong()), // ip_from
	            		new NotNull(new ParseLong()), // ip_to
	                    new NotNull(), // country_code
	                    new NotNull(), // country_name
	                    new NotNull(), // region_name
	                    new NotNull(), // city_name
	                    new Optional(new ParseDouble()), //  latitude
	                    new Optional(new ParseDouble()), // longitude
	        			new Optional(), // zip_code
	            };
	        }
	        else { // default DB11
	        	processors = new CellProcessor[] { 
	            		new NotNull(new ParseLong()), // ip_from
	            		new NotNull(new ParseLong()), // ip_to
	                    new NotNull(), // country_code
	                    new NotNull(), // country_name
	                    new NotNull(), // region_name
	                    new NotNull(), // city_name
	                    new Optional(new ParseDouble()), //  latitude
	                    new Optional(new ParseDouble()), // longitude
	                    new Optional(), // zip_code
	                    new Optional() // time_zone
	            };
	        }
	        
	        
	        return processors;
	}

	
	static void configAssertion(String[] args) {
		
        if (args.length == 0) {
        	System.err.println(TAG+" " + "about usage... Read the source Luke!!! ");
			System.err.println(TAG+" required parameters: " + "configfile_fullpath_and_name");
			System.err.println(TAG+" example: " + "java -jar ip2location.jar -f /full-path-to-config-file/myconfig.yml");
			System.err.println(TAG+" plz reuse the example config.yml under resources folder");
			
            System.exit(-1);
        }
        if (!args[0].equals("-f")) {
        	System.err.println("Please specify the -f parameter with a correct yaml file");
            System.exit(-1);
        }
        if (args.length != 2) {
        	System.err.println("Incorrect syntax. Pass max 2 parameters");
            System.exit(-1);
        }
    }
}
