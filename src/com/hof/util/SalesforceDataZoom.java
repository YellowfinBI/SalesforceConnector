package com.hof.util;

public class SalesforceDataZoom {

	// data = <API KEY>
	// data = <API SECRET>

	private final static char[] data={'K','E','Y'};
	private final static char[] zoom={'S','E','C','R','E','T'};
	private final static String API_VERSION="v30.0";
	private final static String API_VERSION_BULK="30.0";
	
	public static String getAPI_VERSION()
	{
		return API_VERSION;
	}
	
	public static String getAPI_VERSION_BULK()
	{
		return API_VERSION_BULK;
	}
	
	public static String getData() {
		return new String(data);
	}
	
	public static String getZoom() {
		return new String(zoom);
	}
	
	public static String getText(String englishText, String translationKey) {

	    String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    return text;

	}
	
	public static String getText(String englishText, String translationKey, String paramKey) {
		String key="";
		if(UtilString.getResourceString(paramKey)!=null)
		{
			key=UtilString.getResourceString(paramKey);
		}
		String text = UtilString.getResourceString(translationKey);
	    if (text==null) return englishText;
	    
	    else if (text.contains("{0}"))
	    {
	    	text=text.replace("{0}", "'"+key+"'");
	    }
	    else if (text.contains("{1}"))
	    {
	    	text=text.replace("{1}", "'"+key+"'");
	    }
	    
	    return text;
	}

}
