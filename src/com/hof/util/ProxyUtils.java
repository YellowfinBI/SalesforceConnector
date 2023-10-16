package com.hof.util;

import java.net.InetSocketAddress;
import java.net.Proxy;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import com.hof.data.SessionBean;

/**
 * class provides methods of setting proxy for HttpClient 
 * */
public class ProxyUtils {
	

	/** set user proxy for http client
	 * @param client client for connection
	 * @return void
	 */
	public static HttpClient getHttpClientProxy(){
		HttpClient client=new HttpClient();
		String proxyAddress= "";
		String proxyPort = "";
		
		OrgCache orgCache = null;
		try{
			orgCache = OrgCache.getInstance();
			Integer ipOrg = Const.UNO;
			SessionBean sb = i4RequestRegistry.getInstance().getCurrentSessionBean();
			
			if(sb!=null)
			{
				ipOrg = sb.getPersonSearchIpOrg();
			}
			proxyAddress = orgCache.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYSERVER);
			proxyPort  = orgCache.getOrgParm(ipOrg, Const.C_OUTGOINGPROXYPORT);
		}catch(Exception e){
			e.printStackTrace();
		}

		if(proxyAddress!=null && proxyPort!=null)
		{	
			int port = Integer.parseInt(proxyPort);
			HostConfiguration config = client.getHostConfiguration();
			config.setProxy(proxyAddress, port);
		}
		
		return client;
	}
}
