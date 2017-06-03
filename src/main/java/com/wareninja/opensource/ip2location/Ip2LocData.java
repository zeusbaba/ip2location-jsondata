/***
 *   Copyleft 2016-2017 - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yilmaz@guleryuz.net 
 *  @see https://github.com/ZeusBaba | http://www.BeerStorm.net 
 *  
 *  disclaimer: I code for fun, dunno what I'm coding about!
 *  
 */

package com.wareninja.opensource.ip2location;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.wareninja.opensource.ip2location.config.MyUtils;

public class Ip2LocData implements Serializable {//extends BaseLocations {
	
	private static final long serialVersionUID = 1L;
	final String TAG = Ip2LocData.class.getSimpleName();
	
	@Expose public String country_code;
	@Expose public String country_name;
	@Expose public String region_name;
	@Expose public String city_name;
	@Expose public String address;
	@Expose public String zip_code;
	@Expose public String time_zone;
	
	//@SerializedName("_id") 
	@Expose public Long ip_from;
  	@Expose public Long ip_to;
    
  	// optional; can be used to store human-readable ip address
  	@Expose public String ipaddress_from;
  	@Expose public String ipaddress_to;
  	
  	@Expose public Long _created;
  	@Expose public String createdAt;
  	public void fillInTimestamp() {
  		fillInTimestamp(System.currentTimeMillis());
  	}
  	public void fillInTimestamp(Long theTime) {
    	_created = theTime;
    	createdAt = MyUtils.getFormattedDate(_created, "GMT");
    }
  	
  	@Expose public Double lat;
	@Expose public Double lon;
	
	@Expose public Double[] location;
	public void updateLocation() {
		
		if (MyUtils.isLatLngValid(lat, lon)) {
			this.location = new Double[] { lat, lon};
		}
	}
	
	public void setLocation(String lat, String lon) {
    	setLocation(Double.parseDouble(lat), Double.parseDouble(lon));
    }
    public void setLocation(Double lat, Double lon) {
    	this.lat = lat;
    	this.lon = lon;
    	//if (location==null || location.length<2) updateLocation();
    	updateLocation();
    }
    public String getLocation() {
    	if (location!=null && location.length==2) {
    		return location[0]+","+location[1];
    	}
    	else if (lat!=null && lon!=null) {
			return String.valueOf(lat)+","+String.valueOf(lon);
		}
    	else return "";
    }
    public boolean isNotEmptyLocation() {
		return !isEmptyLocation();
	}
	public boolean isEmptyLocation() {
		return ( ( lat==null && lon==null )
				|| "".equals( Double.toString(lat) + Double.toString(lon) )
				|| ( lat==(Double)0d || lon==(Double)0d )
				|| (this.location==null || this.location.length!=2)
				//|| !LocoUtils.isLocationValid(lat, lon)
				);
	}
  	
	public Map<String, Object> toResponseMap() {
    	return toMap();
    }
    public Map<String, Object> toMap() {
        Map<String, Object> respMap = new HashMap<String, Object>();

        if (MyUtils.isNotEmpty(country_code)) respMap.put("country_code", country_code);
        if (MyUtils.isNotEmpty(country_name)) respMap.put("country_name", country_name);
        if (MyUtils.isNotEmpty(region_name)) respMap.put("region_name", region_name);
        if (MyUtils.isNotEmpty(city_name)) respMap.put("city_name", city_name);
        if (ip_from!=null) respMap.put("ip_from", ip_from);
        if (ip_to!=null) respMap.put("ip_to", ip_to);
        
        if (_created==null) fillInTimestamp(); 
        if (lat!=null) respMap.put("lat", lat);
		if (lon!=null) respMap.put("lon", lon);
		if ( MyUtils.isNotEmpty(getLocation()) ) respMap.put("location", getLocation());
        
        return respMap;
    }
	
	@Override
	public String toString() {
		if (_created==null) fillInTimestamp();
		return TAG+" ["
				+ "ip_from=" + ip_from + ", ip_to=" + ip_to
				+ "ipaddress_from=" + ipaddress_from + ", ipaddress_to=" + ipaddress_to 
				+ ", country_code=" + country_code + ", country_name="
				+ country_name + ", region_name=" + region_name
				+ ", city_name=" + city_name + ", address=" + address
				+ ", zip_code="+zip_code + " , time_zone="+time_zone
				+ ", _created="+_created
				+ ", location="+getLocation()
				+ "]";
	}
	
	public JsonObject toJsonObject() {
		return toJsonObject(false);
	}
	public JsonObject toJsonObject(boolean isMongoDb) {
		if (isMongoDb) { // for MongoDB; we set ip_from as _id, so it's indexed by default for better search 
			FieldNamingStrategy customPolicy = new FieldNamingStrategy() {  
			    
			    public String translateName(Field f) {
			        return f.getName().replace("ip_from", "_id");
			    }
			};

			GsonBuilder gsonBuilder = new GsonBuilder()
				    .excludeFieldsWithModifiers( new int[] { 
				    		Modifier.STATIC, Modifier.TRANSIENT//, Modifier.FINAL 
				    		} )
				    .excludeFieldsWithoutExposeAnnotation()
				    .setFieldNamingStrategy(customPolicy)
				    ;  
			Gson gson = gsonBuilder.create();
  
			return (new JsonParser()).parse( gson.toJson(this) ).getAsJsonObject();
		}
		else { // elastic search 
			
			JsonObject jsonObject = (new JsonParser()).parse( toJsonString() ).getAsJsonObject();
			JsonObject properties = (new JsonParser()).parse( "{'location' : {'type' : 'geo_point'}}" ).getAsJsonObject();
			jsonObject.add("properties", properties);
			
			return jsonObject;
		}
	}
    public String toJsonString() {
    	if (_created==null) fillInTimestamp();
		return "" + MyUtils.getGson().toJson(this);
	}
	public String toJsonStringPrinting() {
		if (_created==null) fillInTimestamp();
		return "" + MyUtils.getGsonWithPrettyPrinting().toJson(this);
	}
	
}

