/***
 *   Copyleft 2016 - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yilmaz@guleryuz.net 
 *  @see https://github.com/WareNinja | http://www.BeerStorm.net 
 *  
 *  disclaimer: I code for fun, dunno what I'm coding about!
 *  
 */

package com.wareninja.opensource.ip2location.config;

import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.wareninja.opensource.ip2location.services.ElasticBulkService;

public final class MyUtils {
	
	protected static final String TAG = MyUtils.class.getSimpleName();
	private final static Logger logger = Logger.getLogger(ElasticBulkService.class);

	public static Gson getGson() {
		return new GsonBuilder()
		    .excludeFieldsWithModifiers( new int[] { 
		    		Modifier.STATIC, Modifier.TRANSIENT//, Modifier.FINAL 
		    		} )
		    .excludeFieldsWithoutExposeAnnotation()
		    .create();
	}
	public static Gson getGsonWithPrettyPrinting() {
		return new GsonBuilder()
		    .excludeFieldsWithModifiers( new int[] { 
		    		Modifier.STATIC, Modifier.TRANSIENT//, Modifier.FINAL 
		    		} )
		    .excludeFieldsWithoutExposeAnnotation()
		    .setPrettyPrinting()
		    .create();
	}
	public static Gson getGsonSimple() {
		return new GsonBuilder()
		    .excludeFieldsWithModifiers( new int[] { 
		    		Modifier.STATIC, Modifier.TRANSIENT//, Modifier.FINAL 
		    		} )
		    .create();
	}
	public static Gson getGsonSimpleWithPrettyPrinting() {
		return new GsonBuilder()
		    .excludeFieldsWithModifiers( new int[] { 
		    		Modifier.STATIC, Modifier.TRANSIENT//, Modifier.FINAL 
		    		} )
		    .setPrettyPrinting()
		    .create();
	}

	
	public static String generateGenericOid() {
		return generateGenericOid("");
	}
	public static String generateGenericOid(String baseid) {
		String output = "";
		
		try {
			
			baseid = baseid.trim();
			if (isNotEmpty(baseid)) {
				if (baseid.contains(" ")) baseid=baseid.replace(" ", "_");// avoid spaces!
				output += baseid + "-";
			}
			output += System.currentTimeMillis();
			//output += "-";
			output += normalizeID ( UUID.randomUUID().toString() );
    	}
    	catch (Exception ex) {
    		logger.warn("generateGenericOid ex:", ex);
    	}
    	
    	return output;
	}
	public static String normalizeID(String input) {
    	String output = "";
    	
    	try {
    		if (input.contains("-")) {
    			//String[] items = StringUtils.split(input, "-");
    			String[] items = input.split("-");
    			for (int i=0; i<items.length; i++) output += items[i];
    		}
    		else {
    			output = input;
    		}
    	}
    	catch (Exception ex) {
    		logger.warn("normalizeID ex:", ex);
    		output = input;
    	}
    	
    	return output;
    }
	
	// Conversion of IP address <-> IP Number
	/* http://www.ip2location.com/faqs/db1-ip-country
	IP Number = 16777216*w + 65536*x + 256*y + z     (1)
	where IP Address = w.x.y.z
	
	For example, if the IP address is "202.186.13.4", then its IP Number will be "3401190660", based on the formula (1).
	
	IP Address = 202.186.13.4
	So, w = 202, x = 186, y = 13 and z = 4
	IP Number = 16777216*202 + 65536*186 + 256*13 + 4
	          = 3388997632 + 12189696 + 3328 + 4
	          = 3401190660
	
	To reverse IP number to IP address,
	
	w = int ( IP Number / 16777216 ) % 256
	x = int ( IP Number / 65536    ) % 256
	y = int ( IP Number / 256      ) % 256
	z = int ( IP Number            ) % 256
	
	where % is the modulus operator and
	int returns the integer part of the division.
	 */
	public static Long ipAddress2ipNumber(String ipAddress) {
		Long ipNumber = null;
		
		try {
			String[] items = ipAddress.split("\\.");
			
			ipNumber = 0L;
			ipNumber += (Long.parseLong(items[0])*16777216);
			ipNumber += (Long.parseLong(items[1])*65536);
			ipNumber += (Long.parseLong(items[2])*256);
			ipNumber += (Long.parseLong(items[3]));
			
			System.out.println( String.format("ipAddress: %s | %s %s %s %s | ipNumber: %s", ipAddress, items[0], items[1], items[2], items[3], ipNumber) );
		}
		catch (Exception ex) {
			System.err.println( String.format("ipAddress2ipNumber - ex: %s | ipAddress: %s", ex.toString(), ipAddress) );
		}
		
		return ipNumber;
	}
	public static String ipNumber2ipAddress(Long ipNumber) {
		String ipAddress = null;
		
		try {
			
			int w = (int) (( ipNumber / 16777216 ) % 256);
			int x = (int) (( ipNumber / 65536 ) % 256);
			int y = (int) (( ipNumber / 256 ) % 256);
			int z = (int) (( ipNumber ) % 256);
			ipAddress = w+"."+x+"."+y+"."+z;
		}
		catch (Exception ex) {
			System.err.println( String.format("ipNumber2ipAddress - ex: %s | ipNumber: %s", ex.toString(), ipNumber) );
		}
		
		return ipAddress;
	}
	
	
	public static boolean isNotEmpty(Object input) {
		return !isEmpty(input);
	}
	public static boolean isEmpty(Object input) {
		boolean result = false;
		
		if (input!=null) {
			
			String inputStr = ""+input;
			result = ( "".equals(inputStr.trim()) );
		}
		
		return result;
	}
	
	/**
	 * getShortFormattedDate 
	 * 
	 * return in string format: yyyy-MM-dd
	 */
	public static String getShortFormattedDate() {
		return getShortFormattedDate(System.currentTimeMillis());
	}
	public static String getShortFormattedDate(long millis) {
		String resp = "";
		try {
			resp = getShortFormattedDate(new Date(millis));
		} catch (Exception ex){}
		return resp;
	}
	public static String getShortFormattedDate(Date date) {
		
		String resp = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
			resp = sdf.format(date);
		} catch (Exception ex){}
		return resp;
	}
	 
	/**
	 * getFormattedDate 
	 * 
	 * return in string format: yyyy-MM-dd'T'HH:mm:ssZ
	 */
	public static String getFormattedDate() {
		return getFormattedDate(System.currentTimeMillis());
	}
	public static String getFormattedDate(long millis) {
		String resp = "";
		try {
			resp = getFormattedDate(new Date(millis));
		} catch (Exception ex){}
		return resp;
	}
	public static String getFormattedDate(Date date) {
		
		String resp = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);
			resp = sdf.format(date);
		} catch (Exception ex){}
		return resp;
	}
	public static String getFormattedDate(Long millis, String timeZone) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US);
		if (timeZone!=null && !timeZone.trim().equals("")) sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		
		return millis!=null?sdf.format( new Date(millis) ):"";
	}
	

    /**
	 * Checks that the internal representation is a valid Geographical Point.
	 * 
	 * @return boolean true if valid for Earth, false otherwise.
	 */
	public static boolean isLatLngValid(Double lat, Double lng) {
		boolean isValid = false;
		try {
			isValid = Math.abs(lng) <= 180 && Math.abs(lat) <= 90;
		}
		catch (Exception ex) {
			System.err.println( String.format("isLatLngValid ex: %s | lat,lng: %s", ex.toString(), lat+","+lng) );
		}
		//return Math.abs(lng) <= 180 && Math.abs(lat) <= 90;
		return isValid;
	}
    
 // -> source from CastUtils: https://github.com/apache/pig/blob/89c2e8e76c68d0d0abe6a36b4e08ddc56979796f/src/org/apache/pig/impl/util/CastUtils.java
    private static Integer mMaxInt = Integer.valueOf(Integer.MAX_VALUE);
    private static Long mMaxLong = Long.valueOf(Long.MAX_VALUE);

    public static Double stringToDouble(String str) {
	    if (str == null) {
	    	return null;
	    } else {
		    try {
		    return Double.parseDouble(str);
		    } catch (NumberFormatException e) {
		    	System.err.println(TAG + "|Unable to interpret value "
		    		    + str
		    		    + " in field being "
		    		    + "converted to double, caught NumberFormatException <"
		    		    + e.getMessage() + "> field discarded");
		    	return null;
		    }
	    }
    }
    public static Float stringToFloat(String str) {
	    if (str == null) {
	    	return null;
	    } else {
		    try {
		    	return Float.parseFloat(str);
		    } catch (NumberFormatException e) {
		    	System.err.println(TAG + "|Unable to interpret value "
		    		    + str
		    		    + " in field being "
		    		    + "converted to float, caught NumberFormatException <"
		    		    + e.getMessage() + "> field discarded");
		    	return null;
		    }
	    }
    }
    public static Integer stringToInteger(String str) {
	    if (str == null) {
	    	return null;
	    } else {
		    try {
		    	return Integer.parseInt(str);
		    } catch (NumberFormatException e) {
			    // It's possible that this field can be interpreted as a double.
			    // Unfortunately Java doesn't handle this in Integer.valueOf. So
			    // we need to try to convert it to a double and if that works
			    // then
			    // go to an int.
			    try {
				    Double d = Double.valueOf(str);
				    // Need to check for an overflow error
				    if (d.doubleValue() > mMaxInt.doubleValue() + 1.0) {
				    	System.err.println(TAG + "|Value " + d
							    + " too large for integer");
				    	return null;
				    }
				    return Integer.valueOf(d.intValue());
			    } catch (NumberFormatException nfe2) {
			    	System.err.println(TAG + "|Unable to interpret value "
						    + str
						    + " in field being "
						    + "converted to int, caught NumberFormatException <"
						    + e.getMessage()
						    + "> field discarded");
			    	return null;
			    }
		    }
	    }
    }
    public static Long stringToLong(String str) {
	    if (str == null) {
	    	return null;
	    } else {
		    try {
		    	return Long.parseLong(str);
		    } catch (NumberFormatException e) {
			    // It's possible that this field can be interpreted as a double.
			    // Unfortunately Java doesn't handle this in Long.valueOf. So
			    // we need to try to convert it to a double and if that works
			    // then
			    // go to an long.
			    try {
				    Double d = Double.valueOf(str);
				    // Need to check for an overflow error
				    if (d.doubleValue() > mMaxLong.doubleValue() + 1.0) {
				    	System.err.println(TAG + "|Value " + d
							    + " too large for long");
				    	return null;
				    }
				    return Long.valueOf(d.longValue());
			    } catch (NumberFormatException nfe2) {
			    	System.err.println(TAG + "|Unable to interpret value "
						    + str
						    + " in field being "
						    + "converted to long, caught NumberFormatException <"
						    + nfe2.getMessage()
						    + "> field discarded");
			    	return null;
			    }
		    }
	    }
    }
	// ---
}
