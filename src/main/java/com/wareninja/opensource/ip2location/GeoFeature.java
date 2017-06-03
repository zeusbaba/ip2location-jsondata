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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.wareninja.opensource.ip2location.config.MyUtils;

public class GeoFeature implements Serializable {
	
	private static final long serialVersionUID = 1L;
	final String TAG = GeoFeature.class.getSimpleName();
	//private final Logger logger = Logger.getLogger(GeoFeature.class);
	
	/*
{
    "type": "FeatureCollection",
    "features": [
	{
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        8.80750533786452,
                        45.7188116070788
                    ],
                    [
                        8.80750533786452,
                        45.7188116070788
                    ]
                ]
            ]
        },
        "properties": {
            "stroke": "#ff0000",
            "stroke-opacity": 1,
            "fill-opacity": 0,
            "MIMA_ACE_I": "1",
            "PRO_COM": "12002",
            "ACE": "0",
            "POPULATION": "5292",
            "LAT": "45.7260984100",
            "LON": "8.8003956023",
            "COM_NAME": "Albizzate"
        }
	},
	...
}
	 */
	@Expose public String type;
	@Expose public Map<String, Object> geometry = new HashMap<String, Object>();
	@Expose public Map<String, Object> properties = new HashMap<String, Object>();
	
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
    	//if (MyUtils.isEmpty(_id)) {
    		//logger.debug("this.properties... " + properties);
    		//_id = "";
    		if (properties.containsKey("MIMA_ACE_I")) {
    			_id = "MIMA_ACE_ID_"+properties.get("MIMA_ACE_I");
    		}
    		else if (properties.containsKey("poi_id")) {
    			_id = "POI_ID_"+properties.get("poi_id");
    		}
    		
    		if (MyUtils.isEmpty(_id)) {// if still empty, set to default val
    			_id = MyUtils.generateGenericOid();
    		}
    		//logger.debug("_id... " + _id);
    	//}
    		
		//JsonObject extra = (new JsonParser()).parse( "{'type' : 'Polygon'}" ).getAsJsonObject();
		//properties.put("location", extra);
    }
	
	public JsonObject toJsonObject() {
		if (_created==null) fillInTimestamp();
		return toJsonObject(false);
	}
	public JsonObject toJsonObject(boolean isMongoDb) {
		if (_created==null) fillInTimestamp();
			
		JsonObject jsonObject = (new JsonParser()).parse( toJsonString() ).getAsJsonObject();
		if (jsonObject.has("geometry")) {
			// https://www.elastic.co/guide/en/elasticsearch/reference/2.4/geo-shape.html
			JsonObject geometry = jsonObject.getAsJsonObject("geometry");
			jsonObject.remove("geometry");
			String theType = geometry.has("type")?geometry.get("type").getAsString():"";
			if (theType.equalsIgnoreCase("GeometryCollection")) {
				geometry.addProperty("type", theType.toLowerCase());
			}
			else if (theType.equalsIgnoreCase("Polygon")) {
				geometry.addProperty("type", theType.toLowerCase());
			}
			else {
				geometry.addProperty("type", "polygon");
			}
			jsonObject.add("location", geometry);
		}
		
		if (jsonObject.has("properties")) {
			// https://www.elastic.co/guide/en/elasticsearch/reference/2.4/geo-shape.html
			JsonObject props = jsonObject.getAsJsonObject("properties");
			jsonObject.remove("properties");
			String latlon_str = "";
			JsonObject latlon = new JsonObject();
			if (props.has("LAT")&&(props.has("LON"))) {
				
				//latlon_str += props.get("LAT");
				//latlon_str += ","+props.get("LON");
				
				latlon.addProperty("lat", props.get("LAT").getAsNumber());
				latlon.addProperty("lon", props.get("LON").getAsNumber());
				
				props.remove("LAT");
				props.remove("LON");
			}
			//if (MyUtils.isNotEmpty(latlon_str)) props.addProperty( "latlon_str", latlon_str );
			if (latlon.size()>0) props.add( "latlon", latlon );	

			// align specific params
			if (props.has("POPULATION")) {
				Integer theId = MyUtils.stringToInteger( props.get("POPULATION").getAsString() );
				if (theId!=null) props.addProperty("POPULATION", theId);
			}
			/*if (props.has("poi_id")) {
				Integer theId = MyUtils.stringToInteger( props.get("poi_id").getAsString() );
				if (theId!=null) props.addProperty("poi_id", theId);
			}
			if (props.has("MIMA_ACE_I")) {
				Integer theId = MyUtils.stringToInteger( props.get("MIMA_ACE_I").getAsString() );
				if (theId!=null) props.addProperty("MIMA_ACE_I", theId);
			}*/
			
			
			jsonObject.add("props", props);
		}
		
		// add Mapping
		/*"properties": {
	        "location": {
	            "type": "geo_shape",
	            "tree": "quadtree",
	            "precision": "150m"
	        }
	    }*/
		/*
		JsonObject mappingProps = (new JsonParser()).parse( 
				//"{'location' : {'type':'geo_shape', 'tree':'quadtree', 'precision':'150m'}}"
				"{'location' : {'type':'geo_shape'}}"
				).getAsJsonObject();
		//JsonObject properties = (new JsonParser()).parse( "{'location' : {'type' : 'geo_point'}}" ).getAsJsonObject();
		jsonObject.add("properties", mappingProps);
		*/
		
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

