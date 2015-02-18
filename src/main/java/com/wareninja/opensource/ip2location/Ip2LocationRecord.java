package com.wareninja.opensource.ip2location;

/***
 *   Copyleft 2015 - WareNinja - BeerStorm / Rumble In The Jungle!
 * 
 *  @author: yg@wareninja.com
 *  @see https://github.com/WareNinja
 *  disclaimer: I code for fun, dunno what I'm coding about! :-)
 *  
 *  Author: yg@wareninja.com / twitter: @WareNinja
 */


import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Ip2LocationRecord implements Serializable {//extends BaseLocations {
	
	private static final long serialVersionUID = 1L;
	final String TAG = Ip2LocationRecord.class.getSimpleName();
	
	@Expose public String country_code;
	@Expose public String country_name;
	@Expose public String region_name;
	@Expose public String city_name;
	@Expose public String address;
	@Expose public String zip_code;
	@Expose public String time_zone;
	
	@SerializedName("_id") @Expose public Long ip_from;
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
	@Expose public Double lng;
	
	@Expose public Double[] latlng;
	public void updateLatlng() {
		
		if (MyUtils.isLatLngValid(lat, lng)) {
			this.latlng = new Double[] { lat, lng};
		}
	}
	
	public void setLocation(String lat, String lng) {
    	setLocation(Double.parseDouble(lat), Double.parseDouble(lng));
    }
    public void setLocation(Double lat, Double lng) {
    	this.lat = lat;
    	this.lng = lng;
    	//if (latlng==null || latlng.length<2) updateLatlng();
    	updateLatlng();
    }
    public String getLatLng() {
    	if (latlng!=null && latlng.length==2) {
    		return latlng[0]+","+latlng[1];
    	}
    	else if (lat!=null && lng!=null) {
			return String.valueOf(lat)+","+String.valueOf(lng);
		}
    	else return "";
    }
    public boolean isNotEmptyLatLng() {
		return !isEmptyLatLng();
	}
	public boolean isEmptyLatLng() {
		return ( ( lat==null && lng==null )
				|| "".equals( Double.toString(lat) + Double.toString(lng) )
				|| ( lat==(Double)0d || lng==(Double)0d )
				|| (this.latlng==null || this.latlng.length!=2)
				//|| !LocoUtils.isLatLngValid(lat, lng)
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
		if (lng!=null) respMap.put("lng", lng);
		if ( MyUtils.isNotEmpty(getLatLng()) ) respMap.put("latlng", getLatLng());
        
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
				+ ", latlng="+getLatLng()
				+ "]";
	}
	
	public JsonObject toJsonObject() {
		return (new JsonParser()).parse( toJsonString() ).getAsJsonObject();
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

