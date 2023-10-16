package com.hof.jdbc.metadata;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.hof.mi.interfaces.UserInputParameters.Parameter;
import com.hof.mi.servlet.MIStartup;
import com.hof.pool.DBConnectionManager;
import com.hof.pool.DBType;
import com.hof.pool.JDBCMetaData;
import com.hof.process.BuildDataProcess;
import com.hof.util.Const;
import com.hof.util.ProxyUtils;
import com.hof.util.SalesforceDataZoom;
import com.hof.util.UtilString;

public class SalesforceMetaData extends JDBCMetaData {

	public SalesforceMetaData() {
		
		super();
		sourceName = SalesforceDataZoom.getText("Salesforce", "mi.text.salesforce.datasource.name");
		sourceCode = "SALESFORCE_THIRDPARTY";
		driverName = "com.hof.imp.SalesforceDataSource";
		sourceType = DBType.THIRD_PARTY;
	}
	
	/*public  void initialiseParameters() {
		
		super.initialiseParameters();
		
		addParameter(new Parameter("PARAMETER1", "Example Parameter", "Example Parameter description",TYPE_TEXT, DISPLAY_TEXT_LONG,  null, true));
		addParameter(new Parameter("PARAMETER2", "Another Example Paramter", "Another Example Paramter description",TYPE_TEXT, DISPLAY_TEXT_MED,  null, true));
		

	}*/
	
	
	boolean initialised = false;
	String url;
	String redirectUri="https://tpconnect.yellowfin.bi/getToken.jsp";
	String environment="https://login.salesforce.com";
	

	
	public  void initialiseParameters() {
		
		super.initialiseParameters();
		
		/*addParameter(new Parameter("ACCESS_TOKEN", "Access Token", "First Parameter for LinkedIn",TYPE_TEXT, DISPLAY_TEXT_MEDLONG,  null, true));
		addParameter(new Parameter("ACCESS_SECRET", "Token Secret" , "Second Parameter for LinkedIn" ,TYPE_TEXT, DISPLAY_TEXT_MEDLONG, null, true));
		addParameter(new Parameter("COMPANY", "Company" , "Third Parameter for LinkedIn" ,TYPE_TEXT, DISPLAY_TEXT_MEDLONG, null, true));*/
		
		if (!initialised)
		{
			String API_KEY=new String(com.hof.util.Base64.decode(SalesforceDataZoom.getData()));
	       	try {
				url = environment+"/services/oauth2/authorize?response_type=code&client_id="+API_KEY + "&redirect_uri="+ URLEncoder.encode(redirectUri, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				url=null;
			}
	       	initialised=true;
		}
		
		
		String inst=SalesforceDataZoom.getText("1. Click the 'Authorize Salesforce' button, login, and 'Allow' access to your account.", "mi.text.salesforce.connection.instructions.line1", "mi.text.salesforce.connection.request.pin.button.text")+"<br>"+  
				SalesforceDataZoom.getText("2. Copy the PIN provided and paste it into Yellowfin.", "mi.text.salesforce.connection.instructions.line2")+"<br>"+ 
				SalesforceDataZoom.getText("3. Click the 'Validate PIN' button.", "mi.text.salesforce.connection.instructions.line3", "mi.text.salesforce.connection.validate.pin.button.text");
        addParameter(new Parameter("HELP", SalesforceDataZoom.getText("Connection Instructions", "mi.text.salesforce.connection.instructions.label"),  inst, TYPE_NUMERIC, DISPLAY_STATIC_TEXT, null, true));
        
        
        Parameter p = new Parameter("URL", SalesforceDataZoom.getText("1. Request Access PIN", "mi.text.salesforce.connection.request.pin.button.label"), SalesforceDataZoom.getText("Connect to Salesforce to receive a PIN for data access", "mi.text.salesforce.connection.request.pin.button.description"),TYPE_UNKNOWN, DISPLAY_URLBUTTON,  null, true);
        p.addOption("BUTTONTEXT",SalesforceDataZoom.getText("Authorize Salesforce", "mi.text.salesforce.connection.request.pin.button.text"));
        p.addOption("BUTTONURL", url);
        addParameter(p);
        addParameter(new Parameter("PIN", SalesforceDataZoom.getText("2. Enter PIN", "mi.text.salesforce.connection.request.pin.field.label"),  SalesforceDataZoom.getText("Enter the PIN received from Salesforce", "mi.text.salesforce.connection.request.pin.field.description"), TYPE_TEXT, DISPLAY_TEXT_MED, null, true));
        p = new Parameter("POSTPIN", SalesforceDataZoom.getText("3. Validate Pin", "mi.text.salesforce.connection.validate.pin.button.label"),  SalesforceDataZoom.getText("Validate the PIN", "mi.text.salesforce.connection.validate.pin.button.description"), TYPE_TEXT, DISPLAY_BUTTON, null, true);
        p.addOption("BUTTONTEXT", SalesforceDataZoom.getText("Validate PIN", "mi.text.salesforce.connection.validate.pin.button.text"));
        addParameter(p);
        
        addParameter(new Parameter("ACCESS_TOKEN", SalesforceDataZoom.getText("Access Token", "mi.text.salesforce.connection.access.token.field.label"), SalesforceDataZoom.getText("Access Token", "mi.text.salesforce.connection.access.token.field.description"),TYPE_PASSWORD, DISPLAY_PASSWORD,  null, true));
		addParameter(new Parameter("REFRESH_TOKEN", SalesforceDataZoom.getText("Refresh Token", "mi.text.salesforce.connection.refresh.token.field.label") , SalesforceDataZoom.getText("Refresh Token", "mi.text.salesforce.connection.refresh.token.field.description") ,TYPE_PASSWORD, DISPLAY_PASSWORD, null, true));
		addParameter(new Parameter("INSTANCE_URL", SalesforceDataZoom.getText("Instance URL", "mi.text.salesforce.connection.instance.url.field.label") , SalesforceDataZoom.getText("Instance URL", "mi.text.salesforce.connection.instance.url.field.description") ,TYPE_TEXT, DISPLAY_TEXT_MEDLONG, null, true));
		
	}

	    
	    public String buttonPressed(String buttonName) throws Exception {
	    
	    	String DATA=new String(com.hof.util.Base64.decode(SalesforceDataZoom.getData()));
			String ZOOM=new String(com.hof.util.Base64.decode(SalesforceDataZoom.getZoom()));
	        
	        
	        if (buttonName.equals("POSTPIN") && getParameterValue("PIN")!=null)
	        {
	        	SSLContext context = SSLContext.getInstance("TLS");
				context.init(null,null,null);
	        	SSLSocketFactory factory = (SSLSocketFactory)context.getSocketFactory();
				SSLSocket socket;
				socket = (SSLSocket)factory.createSocket();
				socket.setEnabledProtocols(new String[] { "TLSv1.1" });
				
	        	String ver=(String)getParameterValue("PIN");
	        	String tokenUrl = environment + "/services/oauth2/token";
	        	PostMethod post = new PostMethod(tokenUrl);
	        	
	        	
				/*02-02-17 modified by kelly */
	        	HttpClient httpclient = ProxyUtils.getHttpClientProxy();				
				/*02-02-17 modified by kelly */

	    		post.addParameter("code", URLDecoder.decode(ver, "UTF-8"));
	    		post.addParameter("grant_type", "authorization_code");
	    		post.addParameter("client_id", DATA);
	    		post.addParameter("client_secret", ZOOM);
	    		post.addParameter("redirect_uri", redirectUri);
	    		
	    		
	    		httpclient.executeMethod(post);
	    		JSONObject response = new JSONObject(new JSONTokener(new InputStreamReader(post.getResponseBodyAsStream())));
	    		
	    		String accessToken="ERROR";
	    		String refreshToken="ERROR";
	    		
	    		String instanceURL="ERROR";
	    		
	    		
	    		if (response.has("access_token"))
	    		{
	    			accessToken=response.getString("access_token");
	    		}
	    		
	    		if (response.has("refresh_token"))
	    		{
	    			refreshToken=response.getString("refresh_token");
	    		}
	    		
	    		if (response.has("instance_url"))
	    		{
	    			instanceURL=response.getString("instance_url");
	    		}
	    		
	    		
	    		setParameterValue("ACCESS_TOKEN", accessToken);
	    		setParameterValue("REFRESH_TOKEN", refreshToken);
	    		setParameterValue("INSTANCE_URL", instanceURL);
	    		
	        }      
	        return null;
	        
	    }
		
	    @Override
		public byte[] getDatasourceIcon() {
			String str = "iVBORw0KGgoAAAANSUhEUgAAAFAAAABQCAYAAACOEfKtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAALEgAACxIB0t1+/AAAABZ0RVh0Q3JlYXRpb24gVGltZQAxMS8yMC8xNBC2SnIAAAAcdEVYdFNvZnR3YXJlAEFkb2JlIEZpcmV3b3JrcyBDUzbovLKMAAAO60lEQVR4nO2deXRT153Hv2/R+iRL8r4bb+CHHSCeJFAS"+
					"lixtgglwEkiApMm0xQqZTM/MmZJmkmnazmnKtE1PkjNZOqEiTYeBLimdQhIgkA2yGIewmcXP2Czebck28qInydLTe/OHbLAdW3pPi43JfM4BS8/3/u7VV/e+d5ff/ZmQJAn/T+QQU10BxsZlAbgBQBGAPAAZAJIA6EYkkwD0DP1rAdAI4CyAWt7KDk5mfcc2uEkXkLFxBQAqANwO4FYAaVGYCwA4DuAzAPsBHOKtrDfqSoZg"+
					"SgRkbFwOgEcBrANQFseieADvANgG4ABvZQOxLmBSBWRs3BIAmwAsB0DGs6xxaAXwGwCv81bWGSujkyIgY+PuAvBzAPPjYV8hPICXATzPW9neaI3FVUDGxrEAXgRwTyztxggngJ8g2CKFSI3ERUDGxqkB/BuAZwCoY2EzjhwHsIG3sicjyRxzAYda3Q4AN0ZraxIRAPwYwW4tKskYUwEZG7cOwBsA9NHYmUL2A3iIt7KX5WaI"+
					"iYDa188QFEVtRrDLTncuAqjgrew5OYmjFpCxcTSANwF8W2neaxgngGW8lf0iXMKoBBx6WOwEsEJJvmkCD2Alb2U/CpUoYgEZG0cB+BOANZHUbprAA7gzVEscK6CS2cGruL7FAwAGwD7Gxs2Sm0GWgIyN+wGAxyOt1TTDAmAvY+MS5SQO24UZG3c7gPcBUFFWbLqxH8Gn86hxoqIuzNi4ZAQHyV838QDgbgBPhUsUrgu/juAC"+
					"59eV5xgbNy9Uggm7MGPj7gfw10hKXZShb9vAmtvmp+p0SVrKSBOExhuQ3B1uof+jNj6wpdZZdL7PZ47E9hRwDMCC4QUIWcMYxsbpAdQDyFJS0tqihIaXFqbTCWoyP0xS6WK/7/QjH7Zn1PR4UyZKVJaouVyRZ2gqtWh8jIqE3S0Q1XaP8b1mV0GXN6BRUrco+T5vZV8D5Av47wB+Kte6UUUK792be3ZuknauwooFttQ6Tz9Z"+
					"ZZ83XC2KAL5flnjmmfJko0FF5k2QT7jQ76vdeKgjudruyVRYZiQ4ARTwVrY3rIBDD45GBMdEYTGrKf/ptQVNFg1VFGntPutwn1y2p3lefoKq7+CqGY5EDVUsN+++Ztfxhz9oK/eJcd9d3Mxb2WflCPgfkLlIQJME6tYV1qXr6ZJoa1dt99TcnKrNpQjCojRvs8t/9qadl1i3IMZz24AHkOOqLBm1PTCqQMbGGQD8g1yLL9+a"+
					"fjwW4gHAgjTd3EjEA4Bcg6r0w5V5p8deT9HR/pkmdW9Bgro/QU0qWvcbBwbAxrEXR7VAxsY9BmCLHGsZetpd/1ARRQCTeTMPyS+Od9f6RMn/yEwTZhjVBSQB48jf+0Wpq87pa3nlzGXmrQv9swTl3b4VQJ6rsuTKlzFWwGMAyuVY2nZH1pf3FxhvVlqDawWPILU9e8TRv6XWySrMWuGqLNk3/OZKF2ZsXAlkigcAy/MM2QoL"+
					"vqbQ0UTWCwvT2KNr8k+Z1ZRfQdZR66Aj74EPyLWQa1DxGoq4LmYoJWbNnLr1ha2pOtojM8sqw9Y67fCbkQIul1toeYq2TW7a6YBBReYffyC/naFlPWgYAEuG35AAMLR0I/t+lq6n4+p/MhWY1VThX+/JlrvV+c3hF8Mt8BtQsLjqFaTJdtOYFG5L15cvzdS3yEi6aPjFSAFl0+zyG8Onmp5sWZLZLyNZuWFrnQa4KuAcJYUc"+
					"6/JkAYh2YBopHgTd2uJCFkOXFpnUfWGS0QBY4KqAilzO+nwi3e0NNERQPxxo4S8ZttZFklUq/sP5WsvvzvX86kRPdSQG5LJxtkXOZysDAHpoty1HSQEUATS7/J5krfKFaglSRC33v8/1nhzwiwbn92ZlA4jrGPSubEbOB5sBBJti2tBPWVg0lO/I6vyLGXp61Ept72DAs2xP8/HGAb8mUUuJ7y7Lza52uPt/WOWg3AExIUNP"+
					"O95ZljPqi2ro8zlX7mtps3uEJNasse9bnluaoCapxbsbjzb0+kgtTWq3LM4wnL7s7XnuWHe6KEnGij3Nn7y2OGPuvXubmzvcQnKBUe3YszynOE1HEzO2NzTMS9a6T3Z79TUPFrBPVzvOvNvkgihB+uWCVNW8ZG3aA/tbHQ6vkDTbouncW5FblqAmVWM/Y6aeljMnzwGCXThJrngMTYqn1xY0Z4yzgPBUteNslzdg7vj7meyP"+
					"ypONiVrSVGxSJ7xxe4aq+v58rSQBj3/SOaprLNnd2JvF0P0fr8xr7XALqu9+3H7yxZqemvpen4VbV1jyi/mpumKz2rC6IGGWnia864pMdZvnp85Zsquxi6FJ1wcr8lpdflG48+2mCwB83d7AnN7BALV1aaZ516WBMzsa+gq3Ls0wvr0sJ3PlDEPRN99p8hSb1Z6PVuS1tvF+5rFD7eMOW9QUIWcpL1GxgO9U5NSY1eOv+y3J"+
					"1DOdbqF04d8aaywaijCpKUZHkapnj3Q5l+xq7O5wC6kOj3ClpTs8gr/fJ+bb3YJhU5VdNRiQ6FM9Xv2iDL3F5ReLF+1uPGdQkf35RlXyDKPK4PKLqWsKEpJnmtTmy4OBos3zU7Xlydqbn7slRd044L8yn33zjqyiu7KZXBvXO7g4gzl1d46h6O9StOluQSJdfjGvzeVnnjxsV/kCwLEur268z4KgU3s4kgAFXfe2DH3bLam6"+
					"CV3YHi42sd9I07X/8LBD9eCB1oxXF6WfePZIl2FOkiaw486s1MpD7Uf7feKVfRCCICQAeKjYRM5J0uqdg4HBPKPKcHOqLu/0g4U9r525zD/0Qeu8J0oTv3j6xqQyUYLxWzmMjheCt9CAJBEA4BclgiSujgjyjSoLABAAIeGrZzjWFpmkskSNYai8cYdjvoDEy5CEHP5PlkPkK7eld4f6/e7GgepPO9z2J0oteUlaqvmsc9Dr"+
					"FyWaAKQur3DOH5BG3WtStJQ6SUs1bG/okwAQJAHSoqG02+v7av/3Uv+l9cWmmflG9fk23u+tsnsa1RTRDoBmaBIZepp76rCD2N/iOvGTL7sw06ThxtZn42wL80mHe+6bdb1fvtfiatBRRJ9FQ13YVt8rEkSwvEQNpR2bDwBaeUG2KzAJwBcuUYKaDBSb1KWh0mgpIvm3tb2eDQfbOwoT1P4flSff9JdvZVMtLkG99kCbacAv"+
					"Gr8zy0ym6GhDaaLmLAAcvi8/I0NP+zYe6hCfO9btr3MO9pVY1IZdlwaE1ftb7CYN6X91UXr5rksDnmxG1TFc1sFVM7LNGlLY8HEHU2RSDx5YkVsCQJPNqK6Mj74901T28q3pdZuPd+uf+KTT+3mnp736/vzUdB3te+xgsLz6Pt+4g+b9LS7ZIwWCsXELABwOleieHMPFnXdnF8g1GmsKd5w/u6YwwfWrBamT4rR+w58v9F8a"+
					"8CeESbbPVVlSQSJ4+ickpYkaOdObmLPtXF9N2u/rG1yCqHrmxiRFs6VIaRrwn5EhHjCkGw2gM1xKs4aM2Ks9Gh6dZZr96CxTNybPO0KqPNguy6kIQ7qRvJUdQHDfc0LaXMJUed6rMImuJe+38icOy99nbgKuzoVDzv2qHZ7rYvU5FN3eQP3691uVnDRoAK4K+JUtwZGc6vGmCKIk25N9uuEcDDTctPNirjcgKXF5PgVcFfBY"+
					"qJSiBLzb5LoYaQWvZY53e0+yf7pQ0O0NjDsmnIBOV2VJB3BVwM/D5fhBlb1MlDAlT+N4wAti04aD7Q2LdzXOc/lFpctKnw2/GJ7KnQHQBWBCTymHR9BuOmy/9NLCNDmP+ClHlOAiCRhGXnILYtvJ7kH7y6d7Evc2uwqicKc5OPziSp9nbNx2AA+Hy/nCwrSTG2dbQjodTjV+UerK295gEUTQFg3p9YugegcDqhg6IBW6Kksu"+
					"AqM3kv4mJ+emKvu8f6myc1LQ2eaaQwK89+5t8fX7RNotiGjjBa3DI8RSvFPD4gGjBdwLwCXHgq3WyRbuOE992MofvZaEDEiSc8Xe5q7PO92KHEMV8seRb8b6xvwWgFWJNR1NSEszmcYFabqeTIYO9A2KpMsvqp+cl6TU2TIUPilY2YkG9IEvHJ6a9e+3sQ6PMNEaXywQEXRxax++MFbAcoQZ0silbn3h2WxGFXIFRy67GweO"+
					"/fNnnTc8OsvM3Z3D+DL1KhVBAJ1uwf9hG0+/wfXOdniEyfASe9tVWbJq5IXxHCwPYoTrQqTMSdI6qu6bYUaUB7D9ouQo2HHe5BycVJ/oiVjsqiz5dOSF8TwMfhmLkk71eFN/drSrPkozvtX7Wz3XiHhf8Fb207EXvyIgb2XfA1AVixKfP9lT9srpyzWIYBNeAjwbPm5v/KiNn8jRfLJ5eryLE3nph11kVcJ9+cbzbyzNNKgp"+
					"Il1O+l5f4ELFnhbjqR5vaqzqECV7eSu7HFBw3JWxcdsAPBKrGuhpUnz6xqQa62yL2aga9xyJaPcI9c+f6BFtnHN2/J3uZeMDMGf4RLsSAVMA1AJIjnWN0nT04C1pupYshnYHRBBNLr+h2u7O7fcpnpNOBj/lrezPht8oOnDN2LgHALwVn3pNC04AmM9b2SsuwIpOa/JW9i8Ixkf4OsIDWD9SvPGQ4yj5jwh+E183vicnkkdY"+
					"AXkr6wFwH2RsPl1HbOatrKxblyxXXd7KNgG4F7h+FlRD8D+BQODHchMrDXuyBMA+jI4ueT2xG8DqUHEHo4naAd7KHgKwDNdnS/wzgAeVBm1U7G0/JOIduL7uib8B8DBvZcP6CY0l4uBjjI3LQ3AVezpFbRtLAMAm3sr+p9wMUXXhkQw9WG5FMHrbdKQdwB1KxBuPWAVgXAPgvxCHaV+c2Angcd7KhnWsGkvcQoAOhQp4ETFc"+
					"gIgDLQD+ibeyuyI1EPcgtENLYb8GcFusbUdBP4AXAPx6aGIQMZMWBpmxcfcA+FcAS+NVhgwuIxg86CXeyoZ0UZbLpAfiHtqo2ghgPYDJOmN3BMGH2w7eysZ023XKQsEzNk6HYAj4VQiGSZ7QjSQCRARF2wPgLd7KRrsXMyFTHksfABgbRwIoRXAYVI7gYcciyDuz4gfQjOAfIziF4NbD4VhGKw/FNSHgRDA2jkH4v+ZgVxq6"+
					"OJaMFfD/AEjnyNEeS4P5AAAAAElFTkSuQmCC";
			return str.getBytes();
		}
		
		
		@Override
		public String getDatasourceShortDescription(){
			return SalesforceDataZoom.getText("Connect to Salesforce.com", "mi.text.salesforce.short.description");
		}

		@Override
		public String getDatasourceLongDescription(){
			return SalesforceDataZoom.getText("Connect to your data in Salesforce.com and analyze your leads, opportunities and accounts.", "mi.text.salesforce.long.description");
		}		
		
}
