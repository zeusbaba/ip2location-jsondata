package com.wareninja.opensource.ip2location;

/***
 *   Copyleft 2016 - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yilmaz@guleryuz.net 
 *  @see https://github.com/WareNinja | http://www.BeerStorm.net 
 *  
 *  disclaimer: I code for fun, dunno what I'm coding about!
 *  
 */


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

public class GenericData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	final String TAG = GenericData.class.getSimpleName();
	
	@Expose public Map<String, Object> row_data = new HashMap<String, Object>();
	
	@Expose public String _id;
  	@Expose public Long _created;
  	@Expose public String createdAt;
  	public void fillInTimestamp() {
  		fillInTimestamp(System.currentTimeMillis());
  	}
  	public void fillInTimestamp(Long theTime) {
  		// NOTE: dummy sleep just to enforce generation of unique IDs 
  		//-try {Thread.sleep(1);} catch (Exception ex1) {}
  		
    	_created = theTime;
    	createdAt = MyUtils.getFormattedDate(_created, "GMT");
    	if (MyUtils.isEmpty(_id)) {
    		_id = MyUtils.generateGenericOid();
    	}
    }
  	
	public Map<String, Object> toResponseMap() {
    	return toMap();
    }
    public Map<String, Object> toMap() {
        Map<String, Object> respMap = new HashMap<String, Object>();
        
        if (MyUtils.isNotEmpty(row_data)) respMap.put("row_data", row_data);
        
        if (_created==null) fillInTimestamp();
        if (MyUtils.isNotEmpty(_created)) respMap.put("_created", _created);
        
        return respMap;
    }
	
	@Override
	public String toString() {
		if (_created==null) fillInTimestamp();
		return TAG+" ["
				+ "_created="+_created
				+ "createdAt="+createdAt
				+ "_id="+_id
				+ ", row_data="+row_data
				+ "]";
	}
	
	public JsonObject toJsonObject() {
		return toJsonObject(false);
	}
	public JsonObject toJsonObject(boolean isMongoDb) {
			
		if (_created==null) fillInTimestamp();
		
		JsonObject jsonObject = (new JsonParser()).parse( toJsonString() ).getAsJsonObject();
		//JsonObject properties = (new JsonParser()).parse( "{'location' : {'type' : 'geo_point'}}" ).getAsJsonObject();
		//jsonObject.add("properties", properties);
		
		return jsonObject;
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

