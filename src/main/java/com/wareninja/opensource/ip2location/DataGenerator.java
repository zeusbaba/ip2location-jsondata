
/***
 *   Copyleft 2015 - WareNinja - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yg@wareninja.com
 *  @see https://github.com/WareNinja
 *  disclaimer: I code for fun, dunno what I'm coding about! :-)
 *  
 *  Author: yg@wareninja.com / twitter: @WareNinja
 */

package com.wareninja.opensource.ip2location;

import java.io.File;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

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

/**
 * load & parse data from IP2LOCATION's CSV file
 * then generates JSON files, ready to import into MongoDB! 
 * 
 * 
 * mvn clean install
 * this will generate executable jar file
 * then ... 
 * Usage: 
 * 	java -jar ip2location-jsondata-0.0.x.jar file_path file_name record_per_outputfile
 * 	java -jar ip2location-jsondata-0.0.x.jar /full-path-to-data-files/ IP2LOCATION-LITE-DB9.CSV 10000
 * 
 *  you can also skip record_per_outputfile param, then default value (50000) will be used!
 * 
 * 
 * the generated JSON files are ready for use! you can easily import them to MongoDB with mongoimport 
 * for example, using this command;
 * 		mongoimport -h mongo_db_url:port -d db_name -c collection_name -u db_username -p db_pwd --upsert --file IP2LOCATION-LITE-DB9-1.json
 * 
 * to learn more about mongoimport, see: http://docs.mongodb.org/manual/reference/program/mongoimport
 *  
 */
public class DataGenerator {

	final static String TAG = DataGenerator.class.getSimpleName();
	final static boolean DEBUG = false;
	
	final static Integer RECORDS_PER_FILE_DEFAULT = 50000;// items/records per output file
	final static Boolean WITH_IP_ADDRESS = true; // if true, then will generate also human-readable ip address
	
	public static void main(String[] args) {
		
		if (args.length<2) {
			System.out.println(TAG+" " + "for usage... Read the source Luke!!! ");
			System.out.println(TAG+" required parameters: " + "file_path file_name record_per_outputfile");
			System.out.println(TAG+" example: " + "/full-path-to-csv-files/ IP2LOCATION-LITE-DB5.CSV 20000");
			System.out.println(TAG+" you may also ignore record_per_outputfile param, then default value ( "+RECORDS_PER_FILE_DEFAULT+" ) will be used!");
			System.out.println(TAG+" remember that: " + "file_name MUST contain DB keyword!! e.g. ...-DB5, etc ");
			return;
		}
		
		String filePath = args[0];
		String sourceFileName = filePath + args[1];
		
		Integer recordsPerFile = RECORDS_PER_FILE_DEFAULT;
		if (args.length>=3) {
			recordsPerFile = MyUtils.stringToInteger( args[2] );
			if (recordsPerFile==null) recordsPerFile = RECORDS_PER_FILE_DEFAULT;
		}
		if(DEBUG) System.out.println( String.format("input params: %s %s", sourceFileName, recordsPerFile) );
		
		
		String outputFileName = "";
		
		ICsvListReader listReader = null;
		File sourceFile;
		File outputFile;
		List<String> outputRecords;
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
            
            if(DEBUG) System.out.println(">> CsvListReader :: Importing... "+sourceFile.getName());
            
            // base file name for the json files
            outputFileName = sourceFile.getName().substring(0, sourceFile.getName().lastIndexOf(".")) + "-";
            
            List<Object> recordList;
            
            Ip2LocationRecord ip2LocationRecord;
            outputRecords = new LinkedList<String>();
            currentTime = System.currentTimeMillis();
            
            while( (recordList = listReader.read(processors)) != null ) {
                
            	if(DEBUG) System.out.println(String.format("lineNo=%s, rowNo=%s, recordList=%s", listReader.getLineNumber(),listReader.getRowNumber(), recordList));
            	
            	ip2LocationRecord = new Ip2LocationRecord();
            	// -> DB1.LITE
            	ip2LocationRecord.ip_from = (Long)recordList.get(0);
            	ip2LocationRecord.ip_to = (Long)recordList.get(1);
            	if (WITH_IP_ADDRESS) {
            		ip2LocationRecord.ipaddress_from = MyUtils.ipNumber2ipAddress(ip2LocationRecord.ip_from);
            		ip2LocationRecord.ipaddress_to = MyUtils.ipNumber2ipAddress(ip2LocationRecord.ip_to);
            	}
            	ip2LocationRecord.country_code = (String)recordList.get(2);
            	ip2LocationRecord.country_name = (String)recordList.get(3);
            	if (recordList.size()>=6) { // -> DB3.LITE
	            	ip2LocationRecord.region_name = (String)recordList.get(4);
	            	ip2LocationRecord.city_name = (String)recordList.get(5);
            	}
            	if (recordList.size()>=8) {// -> DB5.LITE
            		ip2LocationRecord.setLocation( (Double)recordList.get(6), (Double)recordList.get(7) );
            	}
            	if (recordList.size()>=9) {// -> DB9.LITE
            		ip2LocationRecord.zip_code = (String)recordList.get(8);
            	}
            	if (recordList.size()>=10) {// -> DB11.LITE
            		ip2LocationRecord.time_zone = (String)recordList.get(9);
            	}
            	ip2LocationRecord.fillInTimestamp( currentTime );
            	if(DEBUG) System.out.println(String.format("the JSON= %s", ip2LocationRecord.toJsonStringPrinting()));
            	
            	outputRecords.add( ip2LocationRecord.toJsonObject().toString() );
            	if (outputRecords.size() >= recordsPerFile ) {

            		fileCounter++;
            		outputFile = new File(
            				filePath+outputFileName + fileCounter +".json"
        					);
            		
            		Files.asCharSink(outputFile,  Charsets.UTF_8).writeLines( outputRecords );
            		outputRecords.clear();
            	}
            }
            
            // ensure that everything is used! 
            if (outputRecords.size() > 0 ) {
            	fileCounter++;
        		outputFile = new File(
        				filePath+outputFileName + fileCounter +".json"
    					);
        		
        		Files.asCharSink(outputFile,  Charsets.UTF_8).writeLines( outputRecords );
        		outputRecords.clear();
            }
		}
		catch (Exception ex) {
			System.err.println("ex: "+ ex.toString());
		}
		finally {
			System.out.println( String.format("Job completed!!! %s json-data files generated...", ""+fileCounter) );
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

}
