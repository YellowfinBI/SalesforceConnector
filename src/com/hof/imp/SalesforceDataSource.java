package com.hof.imp;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


import com.hof.jdbc.metadata.SalesforceMetaData;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AbstractDataSource;

import com.hof.mi.thirdparty.interfaces.ScheduleDefinition;
import com.hof.mi.thirdparty.interfaces.ScheduleDefinition.FrequencyTypeCode;
import com.hof.mi.thirdparty.interfaces.ThirdPartyException;
import com.hof.pool.JDBCMetaData;
import com.hof.util.CustomHttpsSocketFactory;
import com.hof.util.ProxyUtils;
import com.hof.util.SalesforceDataZoom;
import com.hof.util.SalesforceTable;

public class SalesforceDataSource extends AbstractDataSource {

	
	private static String APIVERSION=SalesforceDataZoom.getAPI_VERSION();
	
	public String getDataSourceName() {
		
		return SalesforceDataZoom.getText("Salesforce", "mi.text.salesforce.datasource.name");
		
	}
	
	
	public ScheduleDefinition getScheduleDefinition() { 
		return new ScheduleDefinition(FrequencyTypeCode.MINUTES, null, 5); 
	};
	
	
	public List<AbstractDataSet> getDataSets() {
		
		List<AbstractDataSet> p = new ArrayList<AbstractDataSet>();
		String tbls="[]";
		byte[] tblsByte=loadBlob("TABLES");
		if (tblsByte==null && areBlobsAvailable())
		{
			cacheTables();
			try {
				tblsByte=loadBlob("TABLES");
				tbls=new String(tblsByte, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(tblsByte==null && !areBlobsAvailable())
		{
			tbls=getTablesJSON();
			
		} else
			try {
				tbls=new String(tblsByte, "UTF-8");
				
			} catch (UnsupportedEncodingException e) {
				
				e.printStackTrace();
			}
		
		JSONArray tables=new JSONArray(tbls);
		int i;
		String url, token, tableName;
		url = getInstanceURL();
		token = getAccessToken();
		//Boolean transformsEnabled;
		//transformsEnabled = Boolean.valueOf((String)getAttribute("USEFORTRANSFORMATIONS"));
		for (i=0; i<tables.length(); i++)
		{
			tableName = tables.getJSONObject(i).getString("name");
			p.add(new SalesforceTable(url, token, tableName, tableName, this));
			
		}
		
		return p;
		
	}
	
	public void saveData(String key, byte[] data)
	{
		saveBlob(key, data);
	}
	
	public byte[] getData(String key)
	{
		return loadBlob(key); 
	}
	
	private void cacheTables()
	{
		String response=getTablesJSON();
		
		if (!response.equals("[]"))
		{
			try {
				saveBlob("TABLES", response.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private String getTablesJSON()
	{
		JSONObject response=getAvailableTables();
		
		if (response.has("sobjects"))
		{
			JSONArray sobjects=response.getJSONArray("sobjects");
			String tablesToSave="[";
			int i;
			
			for (i=0; i<sobjects.length(); i++)
			{
				if (sobjects.getJSONObject(i).has("label") && sobjects.getJSONObject(i).has("name") && sobjects.getJSONObject(i).has("queryable"))
				{
					boolean queryable=sobjects.getJSONObject(i).getBoolean("queryable");
					JSONObject table=new JSONObject();
					if (queryable)
					{
						table.put("label", sobjects.getJSONObject(i).getString("label"));
						table.put("name", sobjects.getJSONObject(i).getString("name"));
						tablesToSave=tablesToSave+table.toString()+",";
					}
				}
			}
			tablesToSave=tablesToSave+"]";
			return tablesToSave;
		}
		
		else return "[]";
	}

	public JSONObject getAvailableTables()
	{
		//JSONObject result;
		
		String scheme = "https";
		Protocol baseHttps = Protocol.getProtocol(scheme);
		int defaultPort = baseHttps.getDefaultPort();

		ProtocolSocketFactory baseFactory = baseHttps.getSocketFactory();
		ProtocolSocketFactory customFactory = new CustomHttpsSocketFactory(baseFactory);

		Protocol customHttps = new Protocol(scheme, customFactory, defaultPort);
		Protocol.registerProtocol(scheme, customHttps);
		
		/*02-02-17 modified by kelly */
    	HttpClient httpclient = ProxyUtils.getHttpClientProxy();				
		/*02-02-17 modified by kelly */
		
		String instanceURL=getInstanceURL();
		//System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
		GetMethod get = new GetMethod(instanceURL+"/services/data/"+APIVERSION+"/sobjects");
		get.setRequestHeader("Authorization", "OAuth " + getAccessToken());
		
		try {
			httpclient.executeMethod(get);
			Object tempResult=new JSONTokener(new InputStreamReader(get.getResponseBodyAsStream(), "UTF-8")).nextValue();
			JSONObject result=new JSONObject();
			if (tempResult instanceof JSONObject)
			{
				result=(JSONObject)tempResult;
			}
						
			return result;
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			return new JSONObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return new JSONObject();
		}
		
		
		
		
	}
	
	public JDBCMetaData getDataSourceMetaData() {
		return new SalesforceMetaData();
	}


	public boolean authenticate() 
	{
		return true;
	}
	
	public void disconnect(){
		
	}

	public Map<String,Object> testConnection(){
		
		try {
			//System.out.println("Test Connection Function");
		Map<String,Object> p = new HashMap<String, Object>();
		String instanceURL=getInstanceURL();
		GetMethod get = new GetMethod(instanceURL+"/services/data/"+APIVERSION+"/sobjects");
		get.setRequestHeader("Authorization", "OAuth " + getAccessToken());
		//System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
		
		String scheme = "https";
		Protocol baseHttps = Protocol.getProtocol(scheme);
		int defaultPort = baseHttps.getDefaultPort();

		ProtocolSocketFactory baseFactory = baseHttps.getSocketFactory();
		ProtocolSocketFactory customFactory = new CustomHttpsSocketFactory(baseFactory);

		Protocol customHttps = new Protocol(scheme, customFactory, defaultPort);
		Protocol.registerProtocol(scheme, customHttps);
		
		/*02-02-17 modified by kelly */
    	HttpClient httpclient = ProxyUtils.getHttpClientProxy();				
		/*02-02-17 modified by kelly */
    	
		httpclient.executeMethod(get);
		InputStreamReader reader;
		
		reader = new InputStreamReader(get.getResponseBodyAsStream(), "UTF-8");
		
		//System.out.println(get.getResponseBodyAsString());
		JSONObject response=new JSONObject();
		Object tempResult=new JSONTokener(reader).nextValue();
		
		if (tempResult instanceof JSONObject)
		{
			response=(JSONObject)tempResult;
		}
		else if (tempResult instanceof JSONArray)
		{
			JSONArray arr=(JSONArray) tempResult;
			int i;
			for (i=0; i<arr.length(); i++)
			{
				if (arr.getJSONObject(i).has("message"))
				{
					throw new ThirdPartyException(arr.getJSONObject(i).getString("message"));
					//p.put("ERROR", arr.getJSONObject(i).getString("message"));
				}
				
			}
			return p;
			
		}
		
		if (response.has("sobjects"))
		{
			JSONArray sobjects=response.getJSONArray("sobjects");
			//String availableTables="";
			int i, counter=0;
			
			if (sobjects.length()==0)
			{
				p.put("ERROR", SalesforceDataZoom.getText(SalesforceDataZoom.getText("No Available Tables Found!", "mi.text.salesforce.error.message1"), "mi.text.salesforce.error.message1"));
				return p;
			}
			
			for (i=0; i<sobjects.length(); i++)
			{
				if (sobjects.getJSONObject(i).has("label") && sobjects.getJSONObject(i).has("name") && sobjects.getJSONObject(i).has("queryable"))
				{
					boolean queryable=sobjects.getJSONObject(i).getBoolean("queryable");
					
					if (queryable)
					{
						//availableTables=availableTables+sobjects.getJSONObject(i).getString("label")+"\n";
						counter++;
					}
				}
			}
			
			p.put("Number of Available Tables", counter);

		}
		
		else
		{
			p.put("ERROR", SalesforceDataZoom.getText("No Available Tables Found!", "mi.text.salesforce.error.message1"));
		}
		
		return p;
		} catch (IOException e) {
			Map<String,Object> p = new HashMap<String, Object>();
			p.put("ERROR", SalesforceDataZoom.getText("No Available Tables Found!", "mi.text.salesforce.error.message1"));
			return p;
		}	
		/*Map<String,Object> p = new HashMap<String, Object>();
		p.put("All good", "All good");
		return p;*/
	}
	
	public boolean autoRun(){
		
		byte[] lastRunByte=loadBlob("LASTRUN");
		if(lastRunByte==null)
		{
			saveBlob("LASTRUN", String.valueOf(new java.util.Date().getTime()).getBytes());
			cacheTables();
		}
		else
		{
			cacheTables();
			java.util.Date curDt=new java.util.Date();
			java.util.Date lastrun=new java.util.Date(Long.valueOf(new String(lastRunByte)));
			long diff = curDt.getTime() - lastrun.getTime();
			double hDiff=diff / (60*60 * 1000.0);
			
			if (hDiff>=0.8)
			{
				String environment="https://login.salesforce.com";
				String tokenUrl = environment + "/services/oauth2/token";
				
				String DATA=new String(com.hof.util.Base64.decode(SalesforceDataZoom.getData()));
				String ZOOM=new String(com.hof.util.Base64.decode(SalesforceDataZoom.getZoom()));
		        
				
				/*02-02-17 modified by kelly */
	        	HttpClient httpclient = ProxyUtils.getHttpClientProxy();				
				/*02-02-17 modified by kelly */
				PostMethod post=new PostMethod(tokenUrl);
				post.addParameter("grant_type", "refresh_token");
				post.addParameter("refresh_token", getRefreshToken());
				post.addParameter("client_id", DATA);
				post.addParameter("client_secret", ZOOM);
				post.addParameter("format", "json");

				try {
					httpclient.executeMethod(post);
					
					JSONObject response = new JSONObject(new JSONTokener(new InputStreamReader(post.getResponseBodyAsStream(), "UTF-8")));
					if (response!=null && response.has("access_token") && response.has("instance_url"))
					{
						saveBlob("ACCESS_TOKEN", response.getString("access_token").getBytes());
						saveBlob("INSTANCE_URL", response.getString("instance_url").getBytes());
						
						saveBlob("LASTRUN", String.valueOf(new java.util.Date().getTime()).getBytes());
					}
				} catch (HttpException e) {
					return false;
				} catch (IOException e) {
					return false;
				}
				
			}
		}		
		
		return true;
	}
	
	private String getAccessToken() {
		String accessToken;
		byte[] accessTokenByte=loadBlob("ACCESS_TOKEN");
		byte[] refreshTokenFormByte=loadBlob("REFRESH_TOKEN_FORM");
		byte[] accessTokenFormByte=loadBlob("ACCESS_TOKEN_FORM");
		if (accessTokenByte!=null)
		{
			if (refreshTokenFormByte!=null && accessTokenFormByte!=null && new String(refreshTokenFormByte).equals((String) getAttribute("REFRESH_TOKEN")) && new String(accessTokenFormByte).equals((String) getAttribute("ACCESS_TOKEN")))
			{
				accessToken=new String(accessTokenByte);
			}
			
			else if (refreshTokenFormByte!=null && accessTokenFormByte!=null && !(new String(refreshTokenFormByte).equals((String) getAttribute("REFRESH_TOKEN")) && new String(accessTokenFormByte).equals((String) getAttribute("ACCESS_TOKEN"))))
			{
				saveBlob("REFRESH_TOKEN", ((String) getAttribute("REFRESH_TOKEN")).getBytes());
				saveBlob("REFRESH_TOKEN_FORM", ((String) getAttribute("REFRESH_TOKEN")).getBytes());
				saveBlob("ACCESS_TOKEN", ((String) getAttribute("ACCESS_TOKEN")).getBytes());
				saveBlob("ACCESS_TOKEN_FORM", ((String) getAttribute("ACCESS_TOKEN")).getBytes());
				accessToken=new String(accessTokenByte);
			}
			
			else
			{
				accessToken=new String(accessTokenByte);
			}
			
		}
		
		else 
		{
			accessToken=(String) getAttribute("ACCESS_TOKEN");
		}
		return accessToken;
	}

	private String getRefreshToken()
	{
		String refreshToken;
		byte[] refreshTokenByte=loadBlob("REFRESH_TOKEN");
		byte[] refreshTokenFormByte=loadBlob("REFRESH_TOKEN_FORM");
		byte[] accessTokenFormByte=loadBlob("ACCESS_TOKEN_FORM");
		if (refreshTokenByte!=null)
		{
			if (refreshTokenFormByte!=null && accessTokenFormByte!=null && new String(refreshTokenFormByte).equals((String) getAttribute("REFRESH_TOKEN")) && new String(accessTokenFormByte).equals((String) getAttribute("ACCESS_TOKEN")))
			{
				refreshToken=new String(refreshTokenByte);
			}
			
			else if (refreshTokenFormByte!=null && accessTokenFormByte!=null && !(new String(refreshTokenFormByte).equals((String) getAttribute("REFRESH_TOKEN")) && new String(accessTokenFormByte).equals((String) getAttribute("ACCESS_TOKEN"))))
			{
				saveBlob("REFRESH_TOKEN", ((String) getAttribute("REFRESH_TOKEN")).getBytes());
				saveBlob("REFRESH_TOKEN_FORM", ((String) getAttribute("REFRESH_TOKEN")).getBytes());
				saveBlob("ACCESS_TOKEN", ((String) getAttribute("ACCESS_TOKEN")).getBytes());
				saveBlob("ACCESS_TOKEN_FORM", ((String) getAttribute("ACCESS_TOKEN")).getBytes());
				refreshToken=new String(refreshTokenByte);
			}
			
			else
			{
				refreshToken=new String(refreshTokenByte);
			}
			
		}
		
		else 
		{
			refreshToken=(String) getAttribute("REFRESH_TOKEN");
		}
		return refreshToken;
	}
	
	private String getInstanceURL() {
		String instanceURL;
		byte[] instanceURLByte=loadBlob("INSTANCE_URL");
		if (instanceURLByte!=null)
		{
			instanceURL=new String(instanceURLByte);
		}
		
		else 
		{
			instanceURL=(String) getAttribute("INSTANCE_URL");
		}
		return instanceURL;
	}
	
	
	public boolean isTransformationCompatible()
	{
		return true;
	}
	
}
