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

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.wareninja.opensource.ip2location.config.MyUtils;

public class GeoFeatureCollection implements Serializable {
	
	private static final long serialVersionUID = 1L;
	final String TAG = GeoFeatureCollection.class.getSimpleName();
	
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
	@Expose public List<GeoFeature> features = new LinkedList<GeoFeature>();

	public JsonObject toJsonObject() {
		return toJsonObject(false);
	}
	public JsonObject toJsonObject(boolean isMongoDb) {
		
		JsonObject jsonObject = (new JsonParser()).parse( toJsonString() ).getAsJsonObject();
		//JsonObject properties = (new JsonParser()).parse( "{'location' : {'type' : 'geo_point'}}" ).getAsJsonObject();
		//jsonObject.add("properties", properties);
		
		return jsonObject;
	}
    public String toJsonString() {
		return "" + MyUtils.getGson().toJson(this);
	}
	public String toJsonStringPrinting() {
		return "" + MyUtils.getGsonWithPrettyPrinting().toJson(this);
	}
	
}

