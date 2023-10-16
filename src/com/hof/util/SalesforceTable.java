package com.hof.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;


import com.hof.imp.SalesforceDataSource;
import com.hof.mi.thirdparty.interfaces.AbstractDataSet;
import com.hof.mi.thirdparty.interfaces.AggregationType;
import com.hof.mi.thirdparty.interfaces.ColumnMetaData;
import com.hof.mi.thirdparty.interfaces.DataType;
import com.hof.mi.thirdparty.interfaces.FilterData;
import com.hof.mi.thirdparty.interfaces.FilterMetaData;
import com.hof.mi.thirdparty.interfaces.FilterOperator;
import com.hof.mi.thirdparty.interfaces.ThirdPartyException;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.ws.ConnectorConfig;

public class SalesforceTable extends AbstractDataSet {
	

	private String instanceURL;
	private String accessToken;
	private String table;
	public String label;
	//public Boolean isTransformationsEnabled;
	private static String APIVERSION=SalesforceDataZoom.getAPI_VERSION();
	private static String APIVERSIONBULK=SalesforceDataZoom.getAPI_VERSION_BULK();
	private SalesforceDataSource datasource;
	private static String NO_OF_RECORDS="No_of_records";
	private static String DAY_ONLY_PREFIX="DAY_ONLY_";
	private static String CALENDAR_MONTH_PREFIX="CALENDAR_MONTH_";
	private static String CALENDAR_QUARTER_PREFIX="CALENDAR_QUARTER_";
	private static String CALENDAR_YEAR_PREFIX="CALENDAR_YEAR_";
	private static String DAY_IN_MONTH_PREFIX="DAY_IN_MONTH_";
	private static String DAY_IN_WEEK_PREFIX="DAY_IN_WEEK_";
	private static String DAY_IN_YEAR_PREFIX="DAY_IN_YEAR_";
	private static String FISCAL_MONTH_PREFIX="FISCAL_MONTH_";
	private static String FISCAL_QUARTER_PREFIX="FISCAL_QUARTER_";
	private static String FISCAL_YEAR_PREFIX="FISCAL_YEAR_";
	private static String HOUR_IN_DAY_PREFIX="HOUR_IN_DAY_";
	private static String WEEK_IN_MONTH_PREFIX="WEEK_IN_MONTH_";
	private static String WEEK_IN_YEAR_PREFIX="WEEK_IN_YEAR_";
	
	private static String FIRST_DAY_OF_MONTH_PREFIX="FIRST_DAY_OF_MONTH_";
	private static String FIRST_DAY_OF_QUARTER_PREFIX="FIRST_DAY_OF_QUARTER_";
	
	public SalesforceTable()
	{
		instanceURL="";
		accessToken="";
		table="";
		label="";
		//isTransformationsEnabled = false;
	}
	
	public SalesforceTable(String URL, String aToken, String tbl, String nm, SalesforceDataSource source)
	{
		instanceURL=URL;
		accessToken=aToken;
		table=tbl;
		label=nm;
		datasource=source;
		//isTransformationsEnabled = false;
	}
	
	public SalesforceTable(String URL, String aToken, String tbl, String nm, SalesforceDataSource source, Boolean isTransform)
	{
		instanceURL=URL;
		accessToken=aToken;
		table=tbl;
		label=nm;
		datasource=source;
		//isTransformationsEnabled = isETLContext();
	}
			
			public List<FilterMetaData> getFilters() {
				//System.out.println("Get Filters Function");
				List<FilterMetaData> fm = new ArrayList<FilterMetaData>();
				
				return fm;
				
			}
			
			
			public String getDataSetName() 
			{
				return label;
			}
			
			private void cacheColumns()
			{
				java.util.Date curDt=new java.util.Date();
				long lastSave=0L;
				byte[] colSaveTimeByte=datasource.getData(table+"_COLUMNS_SAVE_TIME");
				if (colSaveTimeByte!=null)
				{
					lastSave=Long.valueOf(new String(colSaveTimeByte));
				}
				
				
				long diff = curDt.getTime() - lastSave;
				long secsDiff=diff / (1000);
				
				if (secsDiff>=120)
				{
					JSONObject description=describeTable(table);
					JSONArray fields=description.getJSONArray("fields");
					Calendar cal=Calendar.getInstance();
					try {
						datasource.saveData(table, fields.toString().getBytes("UTF-8"));
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					datasource.saveData(table+"_COLUMNS_SAVE_TIME", String.valueOf(cal.getTimeInMillis()).getBytes());
				}
			}
			
			public List<ColumnMetaData> getColumns() {

				//System.out.println("Get Columns Function");
				List<ColumnMetaData> cm = new ArrayList<ColumnMetaData>();
				
				JSONObject description;
				JSONArray fields=new JSONArray("[]");
				
				cacheColumns();
				byte[] getDataTableByte=datasource.getData(table);
				if (getDataTableByte!=null)
				{
					fields=new JSONArray(new String(getDataTableByte));
				}
				else
				{
					description=describeTable(table);
					fields=description.getJSONArray("fields");
				}
				
				int i;
				DataType type;
				boolean isETLContext = false;
				
				try {

					isETLContext = isTransformationContext();

				} catch (NoSuchMethodError e) {


				}

				
				for (i=0; i<fields.length(); i++)
				{
					if (fields.getJSONObject(i).has("name") && fields.getJSONObject(i).has("type"))
					{
						String tp=fields.getJSONObject(i).getString("type");
						AggregationType[] agg=new AggregationType[2];
						
						agg[0]=AggregationType.COUNT;
						agg[1]=AggregationType.COUNTDISTINCT;
						
						AggregationType defaultAggregation=AggregationType.SUM;
						
						FilterOperator[] fo;
						
						
						if (tp.equals("boolean"))
						{
							if (!isETLContext)
							{
								type=DataType.BOOLEAN;
								fo=new FilterOperator[3];
								fo[0]=FilterOperator.EQUAL;
								fo[1]=FilterOperator.NOTEQUAL;
								fo[2]=FilterOperator.INLIST;
							}
							
							else 
							{
								type=DataType.INTEGER;
								fo=new FilterOperator[3];
								fo[0]=FilterOperator.EQUAL;
								fo[1]=FilterOperator.NOTEQUAL;
							}
														
						}
						
						else if (tp.equals("int"))
						{
							type=DataType.INTEGER;
							
							agg=new AggregationType[6];
							agg[0]=AggregationType.COUNT;
							agg[1]=AggregationType.COUNTDISTINCT;
							agg[2]=AggregationType.MAX;
							agg[3]=AggregationType.MIN;
							agg[4]=AggregationType.AVG;
							agg[5]=AggregationType.SUM;
							
							fo=new FilterOperator[12];
							fo[0]=FilterOperator.EQUAL;
							fo[1]=FilterOperator.NOTEQUAL;
							fo[2]=FilterOperator.INLIST;
							fo[3]=FilterOperator.BETWEEN;
							fo[4]=FilterOperator.GREATER;
							fo[5]=FilterOperator.GREATEREQUAL;
							fo[6]=FilterOperator.ISNULL;
							fo[7]=FilterOperator.ISNOTNULL;
							fo[8]=FilterOperator.LESS;
							fo[9]=FilterOperator.LESSEQUAL;
							fo[10]=FilterOperator.NOTBETWEEN;
							fo[11]=FilterOperator.NOTINLIST;
						}
						else if (tp.equals("currency") || tp.equals("percent") || tp.equals("double"))
						{
							type=DataType.NUMERIC;
							
							agg=new AggregationType[6];
							agg[0]=AggregationType.COUNT;
							agg[1]=AggregationType.COUNTDISTINCT;
							agg[2]=AggregationType.MAX;
							agg[3]=AggregationType.MIN;
							agg[4]=AggregationType.AVG;
							agg[5]=AggregationType.SUM;
							
							fo=new FilterOperator[12];
							fo[0]=FilterOperator.EQUAL;
							fo[1]=FilterOperator.NOTEQUAL;
							fo[2]=FilterOperator.INLIST;
							fo[3]=FilterOperator.BETWEEN;
							fo[4]=FilterOperator.GREATER;
							fo[5]=FilterOperator.GREATEREQUAL;
							fo[6]=FilterOperator.ISNULL;
							fo[7]=FilterOperator.ISNOTNULL;
							fo[8]=FilterOperator.LESS;
							fo[9]=FilterOperator.LESSEQUAL;
							fo[10]=FilterOperator.NOTBETWEEN;
							fo[11]=FilterOperator.NOTINLIST;
							
							if (tp.equals("percent"))
							{
								defaultAggregation=AggregationType.AVG;
							}
						}
						else if (tp.equals("datetime"))
						{
							type=DataType.TIMESTAMP;
							
							fo=new FilterOperator[12];
							fo[0]=FilterOperator.EQUAL;
							fo[1]=FilterOperator.NOTEQUAL;
							fo[2]=FilterOperator.INLIST;
							fo[3]=FilterOperator.BETWEEN;
							fo[4]=FilterOperator.GREATER;
							fo[5]=FilterOperator.GREATEREQUAL;
							fo[6]=FilterOperator.ISNULL;
							fo[7]=FilterOperator.ISNOTNULL;
							fo[8]=FilterOperator.LESS;
							fo[9]=FilterOperator.LESSEQUAL;
							fo[10]=FilterOperator.NOTBETWEEN;
							fo[11]=FilterOperator.NOTINLIST;
						}
						else if (tp.equals("date"))
						{
							type=DataType.DATE;
							
							fo=new FilterOperator[12];
							fo[0]=FilterOperator.EQUAL;
							fo[1]=FilterOperator.NOTEQUAL;
							fo[2]=FilterOperator.INLIST;
							fo[3]=FilterOperator.BETWEEN;
							fo[4]=FilterOperator.GREATER;
							fo[5]=FilterOperator.GREATEREQUAL;
							fo[6]=FilterOperator.ISNULL;
							fo[7]=FilterOperator.ISNOTNULL;
							fo[8]=FilterOperator.LESS;
							fo[9]=FilterOperator.LESSEQUAL;
							fo[10]=FilterOperator.NOTBETWEEN;
							fo[11]=FilterOperator.NOTINLIST;
						}
						else
						{
							type=DataType.TEXT;
							
							fo=new FilterOperator[20];
							fo[0]=FilterOperator.BETWEEN;
							fo[1]=FilterOperator.CONTAINS;
							fo[2]=FilterOperator.ENDSWITH;
							fo[3]=FilterOperator.EQUAL;
							fo[4]=FilterOperator.GREATER;
							fo[5]=FilterOperator.GREATEREQUAL;
							fo[6]=FilterOperator.INLIST;
							fo[7]=FilterOperator.ISEMPTYSTRING;
							fo[8]=FilterOperator.ISNOTEMPTYSTRING;
							fo[9]=FilterOperator.ISNOTNULL;
							fo[10]=FilterOperator.ISNULL;
							fo[11]=FilterOperator.LESS;
							fo[12]=FilterOperator.LESSEQUAL;
							fo[13]=FilterOperator.NOTBETWEEN;
							fo[14]=FilterOperator.NOTCONTAINS;
							fo[15]=FilterOperator.NOTENDSWITH;
							fo[16]=FilterOperator.NOTEQUAL;
							fo[17]=FilterOperator.NOTINLIST;
							fo[18]=FilterOperator.NOTSTARTSWITH;
							fo[19]=FilterOperator.STARTSWITH;
						}
						
						boolean isCompundType = false;
						if ("location".equals(tp) || "address".equals(tp))
						{
							isCompundType = true;
						}
						
						ColumnMetaData col=new ColumnMetaData(fields.getJSONObject(i).getString("name"), type, null, agg, fo);
						if (type.equals(DataType.INTEGER) || type.equals(DataType.NUMERIC))
						{
							col.setSelectedAggregation(defaultAggregation);
						}
						if (!isETLContext || (isETLContext && !isCompundType))
						{
							cm.add(col);
						}
						
						
						if (type.equals(DataType.TIMESTAMP) && !isETLContext)
						{
							col=new ColumnMetaData(DAY_ONLY_PREFIX+fields.getJSONObject(i).getString("name"), DataType.DATE, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(CALENDAR_MONTH_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(CALENDAR_QUARTER_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(CALENDAR_YEAR_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(DAY_IN_MONTH_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(DAY_IN_WEEK_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(DAY_IN_YEAR_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(FISCAL_MONTH_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(FISCAL_QUARTER_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(FISCAL_YEAR_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(HOUR_IN_DAY_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(WEEK_IN_MONTH_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							col=new ColumnMetaData(WEEK_IN_YEAR_PREFIX+fields.getJSONObject(i).getString("name"), DataType.INTEGER, null, null, fo);
							cm.add(col);
							
							col=new ColumnMetaData(FIRST_DAY_OF_MONTH_PREFIX+fields.getJSONObject(i).getString("name"), DataType.DATE, null, null, null);
							cm.add(col);
							col=new ColumnMetaData(FIRST_DAY_OF_QUARTER_PREFIX+fields.getJSONObject(i).getString("name"), DataType.DATE, null, null, null);
							cm.add(col);
							
							
						}
						
					}
				}
				FilterOperator[] fo=new FilterOperator[12];
				fo[0]=FilterOperator.EQUAL;
				fo[1]=FilterOperator.NOTEQUAL;
				fo[2]=FilterOperator.INLIST;
				fo[3]=FilterOperator.BETWEEN;
				fo[4]=FilterOperator.GREATER;
				fo[5]=FilterOperator.GREATEREQUAL;
				fo[6]=FilterOperator.ISNULL;
				fo[7]=FilterOperator.ISNOTNULL;
				fo[8]=FilterOperator.LESS;
				fo[9]=FilterOperator.LESSEQUAL;
				fo[10]=FilterOperator.NOTBETWEEN;
				fo[11]=FilterOperator.NOTINLIST;
				
				if (!isETLContext)
				{
					cm.add(new ColumnMetaData(NO_OF_RECORDS, DataType.INTEGER, null, null, fo));
				}
				
				
				/*if (table.equals("Opportunity"))
				{
					cm.add(new ColumnMetaData("ADJUSTED_AMOUNT", DataType.NUMERIC, null, null, null));
					
				}*/
				
				return cm;
			}
			
			
			public Object[][] execute(List<ColumnMetaData> columns, List<FilterData> filters) 
			{
				Object data[][]=null;
				
				boolean isETLContext = false;
				
				try {

					isETLContext = isTransformationContext();

				} catch (NoSuchMethodError e) {


				}
				
				String query="SELECT ";
				ArrayList<String> fields=new ArrayList<String>();
				ArrayList<String> aggregatedFields=new ArrayList<String>();
				ArrayList<String> aggregatedDayOnlyFields=new ArrayList<String>();
				ArrayList<String> dayOnlyFields=new ArrayList<String>();
				ArrayList<String[]> FieldsMapping=new ArrayList<String[]>();
				
				//boolean hasRECORDS_YELLOWFIN=false, hasId=false;
				int i, j;
				
				if (columns.size()>1)
				{
					for (i=0; i<columns.size(); i++)
					{
						String aggCol;
						String colName=columns.get(i).getColumnName();
						if (colName.equals(NO_OF_RECORDS) && !aggregatedFields.contains("count(id) "+NO_OF_RECORDS))
						{
							//hasRECORDS_YELLOWFIN=true;
							aggregatedFields.add("count(id) "+NO_OF_RECORDS);
							aggCol="count(id) "+NO_OF_RECORDS;
							
						}
						else
						{
							aggCol=getAggregatedColumName(columns.get(i), FieldsMapping);
							
							if (colName.equals(aggCol) && !fields.contains(aggCol))
							{
								fields.add(aggCol);
								
								/*String[] fld=new String[2];
								fld[0]=colName;
								fld[1]=aggCol;
								FieldsMapping.add(fld);*/
							}
							
							else if(columnStartsWithDateFunction(aggCol))
							{
								dayOnlyFields.add(aggCol);
								aggregatedDayOnlyFields.add(getColumnDateFunction(aggCol)+colName.replaceFirst(getColumnDateFunctionPrefix(aggCol), "")+")");
								
								/*String[] fld=new String[2];
								fld[0]=colName;
								fld[1]=aggCol;
								FieldsMapping.add(fld);*/
							}
								
							else if (!colName.equals(aggCol) && !fields.contains(aggCol) && !aggregatedFields.contains(aggCol)) 
							{
								aggregatedFields.add(aggCol);
								
								/*String[] fld=new String[2];
								fld[0]=colName;
								fld[1]=aggCol;
								FieldsMapping.add(fld);*/
							}
							
							
							
							
							/*if (colName.equals("Id"))
							{
								//hasId=true;
							}*/
						}
						String[] fld=new String[2];
						fld[0]=colName;
						fld[1]=aggCol;
						FieldsMapping.add(fld);
						
					
					}
				}
				
				else if (columns.size()==1 && columns.get(0).getColumnName().equals(NO_OF_RECORDS))
				{
					//hasRECORDS_YELLOWFIN=true;
					aggregatedFields.add("count(id) "+NO_OF_RECORDS);
					
					String[] fld=new String[2];
					fld[0]=columns.get(0).getColumnName();
					fld[1]="count(id) "+NO_OF_RECORDS;
					FieldsMapping.add(fld);
				}
				
				else if (columns.size()==1 && !columns.get(0).getColumnName().equals(NO_OF_RECORDS))
				{
					String aggCol=getAggregatedColumName(columns.get(0), FieldsMapping);
					
					/*if (columns.get(0).getColumnName().equals(aggCol))
					{
						fields.add(aggCol);
					}
					
					else aggregatedFields.add(aggCol);*/
					
					if (columns.get(0).getColumnName().equals(aggCol) && !fields.contains(aggCol))
					{
						fields.add(aggCol);
						
						String[] fld=new String[2];
						fld[0]=columns.get(0).getColumnName();
						fld[1]=aggCol;
						FieldsMapping.add(fld);
					}
					
					else if(columnStartsWithDateFunction(aggCol))
					{
						dayOnlyFields.add(aggCol);
						aggregatedDayOnlyFields.add(getColumnDateFunction(aggCol)+columns.get(0).getColumnName().replaceFirst(getColumnDateFunctionPrefix(aggCol), "")+")");
						
						String[] fld=new String[2];
						fld[0]=columns.get(0).getColumnName();
						fld[1]=aggCol;
						FieldsMapping.add(fld);
					}
						
					else 
					{
						aggregatedFields.add(aggCol);
						
						String[] fld=new String[2];
						fld[0]=columns.get(0).getColumnName();
						fld[1]=aggCol;
						FieldsMapping.add(fld);
					}
				}
				
				else return null;
				
				/*if (hasId && hasRECORDS_YELLOWFIN)
				{
					return null;
				}*/
				
				String fieldsStr="";
				String dayOnlyFieldsStr="";
				String aggregatedDayOnlyFieldsStr="";
				if(fields.size()>0)
				{
					fieldsStr=fields.get(0);
					
					for (i=1; i<fields.size(); i++)
					{
						fieldsStr=fieldsStr+", "+fields.get(i);
					}
					
				}
				
				if (dayOnlyFields.size()>0)
				{
					dayOnlyFieldsStr=dayOnlyFields.get(0);
					
					for (i=1; i<dayOnlyFields.size(); i++)
					{
						dayOnlyFieldsStr=dayOnlyFieldsStr+", "+dayOnlyFields.get(i);
					}
				}
				
				if(aggregatedDayOnlyFields.size()>0)
				{
					aggregatedDayOnlyFieldsStr=aggregatedDayOnlyFields.get(0);
					
					for (i=1; i<aggregatedDayOnlyFields.size(); i++)
					{
						if (!aggregatedDayOnlyFieldsStr.contains(aggregatedDayOnlyFields.get(i)))
						{
							aggregatedDayOnlyFieldsStr=aggregatedDayOnlyFieldsStr+", "+aggregatedDayOnlyFields.get(i);
						}
					}
				}
				
				if(fieldsStr.length()>0 && dayOnlyFieldsStr.length()>0)
				{
					query=query+fieldsStr+", "+dayOnlyFieldsStr;
				}
				else if(fieldsStr.length()>0 && dayOnlyFieldsStr.length()==0)
				{
					query=query+fieldsStr;
				}
				else if(fieldsStr.length()==0 && dayOnlyFieldsStr.length()>0)
				{
					query=query+dayOnlyFieldsStr;
				}
				
				
				String aggregatedFieldsStr="";
				
				if(aggregatedFields.size()>0)
				{
					aggregatedFieldsStr=aggregatedFields.get(0);
					
					for (i=1; i<aggregatedFields.size(); i++)
					{
						aggregatedFieldsStr=aggregatedFieldsStr+", "+aggregatedFields.get(i);
					}
					
					if (fields.size()>0 || dayOnlyFields.size()>0)
					{
						query=query+", ";
					}
					query=query+" "+aggregatedFieldsStr;
				}
				
				if(fields.size()+dayOnlyFields.size()+aggregatedFields.size()==0)
				{
					return null;
				}
								
				query=query+" FROM "+table;
				
				ArrayList<FilterData> whereFilters=new ArrayList<FilterData>();
				ArrayList<FilterData> havingFilters=new ArrayList<FilterData>();
				for (FilterData fd:filters)
				{
					if (fd.getFilterName().equals(NO_OF_RECORDS))
					{
						havingFilters.add(fd);
					}
					else whereFilters.add(fd);
				}
				ArrayList<String> whereFiltersSOQL=getFiltersSOQL(whereFilters);
				ArrayList<String> havingFiltersSOQL=getHavingFiltersSOQL(havingFilters);
						
				if (whereFiltersSOQL.size()>0)
				{
					query=query+" WHERE ";
					
					query=query+"("+whereFiltersSOQL.get(0)+")";
					
					for(i=1; i<whereFiltersSOQL.size(); i++)
					{
						query=query+" AND ("+whereFiltersSOQL.get(i)+")";
					}
				}
				
				
				/*String columnsToGroupBy="";
				for (ColumnMetaData cm:columns)
				{
					if (cm.getSelectedAggregation()==null && cm.getColumnType()!=DataType.INTEGER && cm.getColumnType()!=DataType.NUMERIC && !cm.getColumnName().startsWith("DAY_ONLY_"))
					{
						if (columnsToGroupBy.equals(""))
						{
							columnsToGroupBy=columnsToGroupBy+cm.getColumnName();
						}
						else
						{
							columnsToGroupBy=columnsToGroupBy+", "+cm.getColumnName();
						}
					}
				}*/
				
				if (((aggregatedFields.size()+aggregatedDayOnlyFields.size())>0) && ((fields.size() + aggregatedDayOnlyFields.size())>0))
				{
					
					query=query+" GROUP BY "+fieldsStr;
					
					if (aggregatedDayOnlyFieldsStr.length()>0)
					{
						if (fieldsStr.length()>0)
						{
							query=query+", ";
						}
						query=query+aggregatedDayOnlyFieldsStr;
					}
				}
				
								
				if (havingFiltersSOQL.size()>0 && query.contains("GROUP BY"))
				{
					query=query+" HAVING ";
					
					query=query+havingFiltersSOQL.get(0);
					
					for(i=1; i<havingFiltersSOQL.size(); i++)
					{
						query=query+" AND ("+havingFiltersSOQL.get(i)+")";
					}
				}
				
				
				if (!isETLContext)
				{
					JSONObject result=runQuery(query/*"SELECT "+fields+" from "+table*/);
					
					if (result!=null && result.has("totalSize") && result.getInt("totalSize")>0)
					{
						int rows=result.getInt("totalSize");
						data=new Object[rows][columns.size()];
						
						JSONArray records=result.getJSONArray("records");
						
						for (i=0; i<rows; i++)
						{
							for (j=0; j<columns.size(); j++)
							{
								ColumnMetaData columnOriginal=columns.get(j);
								String columnAlias=getAlias(j, FieldsMapping);
								
								if(columnAlias.contains(" "))
								{
									columnAlias=columnAlias.split(" ")[1];
								}
								if (columnOriginal.getColumnType().equals(DataType.TEXT) && !records.getJSONObject(i).isNull(columnAlias))
								{
									data[i][j]=records.getJSONObject(i).getString(columnAlias);
								}
								
								
								
								else if (columnOriginal.getColumnType().equals(DataType.BOOLEAN) && !records.getJSONObject(i).isNull(columnAlias))
								{
									data[i][j]=records.getJSONObject(i).getBoolean(columnAlias);
								}
								
								else if (columnOriginal.getColumnType().equals(DataType.INTEGER) && !records.getJSONObject(i).isNull(columnAlias))
								{
									data[i][j]=records.getJSONObject(i).getInt(columnAlias);
								}
								
								else if (columnOriginal.getColumnType().equals(DataType.NUMERIC) && !records.getJSONObject(i).isNull(columnAlias))
								{
									data[i][j]=records.getJSONObject(i).getDouble(columnAlias);
								}
								
								else if (columnOriginal.getColumnType().equals(DataType.DATE) && !records.getJSONObject(i).isNull(columnAlias))
								{
									DateFormat df= new SimpleDateFormat("yyy-MM-dd", Locale.ENGLISH);
									java.util.Date dt;
									String dateStr=records.getJSONObject(i).getString(columnAlias);
								    try 
								    {
								    	dt=df.parse(dateStr);
								    	if (columnAlias.contains(FIRST_DAY_OF_MONTH_PREFIX))
								    	{
								    		java.sql.Timestamp tm=new java.sql.Timestamp(dt.getTime());
								    		DateFields dateF=new DateFields(tm);
								    		dt=dateF.getMonthStartDate();
								    	}
								    	else if (columnAlias.contains(FIRST_DAY_OF_QUARTER_PREFIX))
								    	{
								    		java.sql.Timestamp tm=new java.sql.Timestamp(dt.getTime());
								    		DateFields dateF=new DateFields(tm);
								    		dt=dateF.getQuarterStartDate();
								    	}
									} 
								    catch (ParseException e) 
								    {
										dt=new java.util.Date(0L);
										throw new ThirdPartyException(SalesforceDataZoom.getText(SalesforceDataZoom.getText("Bad date format returned!", "mi.text.salesforce.error.message2"), "mi.text.salesforce.error.message2"));
									}
									data[i][j]=new java.sql.Date(dt.getTime());
								}
								
								else if (columnOriginal.getColumnType().equals(DataType.TIMESTAMP) && !records.getJSONObject(i).isNull(columnAlias))
								{
									DateFormat df= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH);
									java.util.Date dt;
									String dateStr=records.getJSONObject(i).getString(columnAlias);
									//System.out.println(dateStr);
								    try 
								    {
								    	dt=df.parse(dateStr);
									} 
								    catch (ParseException e) 
								    {
										dt=new java.util.Date(0L);
										throw new ThirdPartyException(SalesforceDataZoom.getText("Bad timestamp format returned!", "mi.text.salesforce.error.message3"));
									}
									data[i][j]=new java.sql.Timestamp(dt.getTime());
								}
								
								else data[i][j]=null;
							}
						}
						
						return data;
					}
				}
				else
				{
					String result = runBulkJob(query);
					String[] dataRows = result.split("\n");
					int rows=dataRows.length;
					data=new Object[rows-1][columns.size()];
					
					
					for (i=1; i<rows; i++)
					{
						String[] row = dataRows[i].split("\",\"");
						if (row.length>0)
						{
							row[0] = row[0].replaceFirst("\"", "");
							
							row[row.length - 1] = (String)row[row.length - 1].subSequence(0, row[row.length - 1].length()-1);
						}
						
						for (j=0; j<row.length; j++)
						{
							data[i-1][j] = row[j];
						}
					}
					
					for (i=0; i<data.length; i++)
					{
						for (j=0; j<columns.size(); j++)
						{
							ColumnMetaData columnOriginal=columns.get(j);
							String columnAlias=getAlias(j, FieldsMapping);
							
							if(columnAlias.contains(" "))
							{
								columnAlias=columnAlias.split(" ")[1];
							}
							if (columnOriginal.getColumnType().equals(DataType.TEXT) && data[i][j] != null && !("").equals(data[i][j]))
							{
								data[i][j]=(String)data[i][j];
							}						
							
							
							else if (columnOriginal.getColumnType().equals(DataType.BOOLEAN) && data[i][j] != null && !("").equals(data[i][j]))
							{
								data[i][j]=Boolean.valueOf((String)data[i][j]);
							}
							
							else if (columnOriginal.getColumnType().equals(DataType.INTEGER) && data[i][j] != null && !("").equals(data[i][j]))
							{
								if (data[i][j] instanceof String)
								{
									if ("TRUE".equals(((String)data[i][j]).toUpperCase()))
									{
										data[i][j] = 1;
									}
									else if ("FALSE".equals(((String)data[i][j]).toUpperCase()))
									{
										data[i][j] = 0;
									}
									else
									{
										data[i][j] = null;
									}
								}
								else 
								{
									data[i][j]=Integer.valueOf((String)data[i][j]);
								}
								
							}
							
							else if (columnOriginal.getColumnType().equals(DataType.NUMERIC) && data[i][j] != null && !("").equals(data[i][j]))
							{
								data[i][j]=Double.valueOf((String)data[i][j]);
							}
							
							else if (columnOriginal.getColumnType().equals(DataType.DATE) && data[i][j]!=null && !("").equals(data[i][j]))
							{
								DateFormat df= new SimpleDateFormat("yyy-MM-dd", Locale.ENGLISH);
								java.util.Date dt;
								String dateStr=(String)data[i][j];
							    try 
							    {
							    	dt=df.parse(dateStr);
							    	if (columnAlias.contains(FIRST_DAY_OF_MONTH_PREFIX))
							    	{
							    		java.sql.Timestamp tm=new java.sql.Timestamp(dt.getTime());
							    		DateFields dateF=new DateFields(tm);
							    		dt=dateF.getMonthStartDate();
							    	}
							    	else if (columnAlias.contains(FIRST_DAY_OF_QUARTER_PREFIX))
							    	{
							    		java.sql.Timestamp tm=new java.sql.Timestamp(dt.getTime());
							    		DateFields dateF=new DateFields(tm);
							    		dt=dateF.getQuarterStartDate();
							    	}
								} 
							    catch (ParseException e) 
							    {
									dt=new java.util.Date(0L);
									throw new ThirdPartyException(SalesforceDataZoom.getText(SalesforceDataZoom.getText("Bad date format returned!", "mi.text.salesforce.error.message2"), "mi.text.salesforce.error.message2"));
								}
								data[i][j]=new java.sql.Date(dt.getTime());
							}
							
							else if (columnOriginal.getColumnType().equals(DataType.TIMESTAMP) && data[i][j] != null && !("").equals(data[i][j]))
							{
								//System.out.println(data[i][j]);
								DateFormat df= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ENGLISH);
								java.util.Date dt;
								String dateStr=(String)data[i][j];
								//System.out.println(dateStr);
							    try 
							    {
							    	dt=df.parse(dateStr);
								} 
							    catch (ParseException e) 
							    {
									dt=new java.util.Date(0L);
									throw new ThirdPartyException(SalesforceDataZoom.getText("Bad timestamp format returned!", "mi.text.salesforce.error.message3"));
								}
								data[i][j]=new java.sql.Timestamp(dt.getTime());
							}
							
							else data[i][j]=null;
						}
						
						
					}
					
					//System.out.println(result);
				}
				
				
				return data;

				
			}
			
			
			private String getAlias(int j, ArrayList<String[]> fieldsMapping) {
				String[] column=new String[2];
				column=fieldsMapping.get(j);
				return column[1];
			}

			private boolean columnStartsWithDateFunction(String columnn)
			{
				if (columnn.startsWith("DAY_ONLY("))
				{
					return true;
				}
				else if (columnn.startsWith("CALENDAR_MONTH("))
				{
					return true;
				}
				else if (columnn.startsWith("CALENDAR_QUARTER("))
				{
					return true;
				}
				else if (columnn.startsWith("CALENDAR_YEAR("))
				{
					return true;
				}
				else if (columnn.startsWith("DAY_IN_MONTH("))
				{
					return true;
				}
				else if (columnn.startsWith("DAY_IN_WEEK("))
				{
					return true;
				}
				else if (columnn.startsWith("DAY_IN_YEAR("))
				{
					return true;
				}
				else if (columnn.startsWith("FISCAL_MONTH("))
				{
					return true;
				}
				else if (columnn.startsWith("FISCAL_QUARTER("))
				{
					return true;
				}
				else if (columnn.startsWith("FISCAL_YEAR("))
				{
					return true;
				}
				else if (columnn.startsWith("HOUR_IN_DAY("))
				{
					return true;
				}
				else if (columnn.startsWith("WEEK_IN_MONTH("))
				{
					return true;
				}
				else if (columnn.startsWith("WEEK_IN_YEAR("))
				{
					return true;
				}
				return false;
			}
			
			private String getColumnDateFunction(String columnn)
			{
				if (columnn.startsWith("DAY_ONLY("))
				{
					return "DAY_ONLY(";
				}
				else if (columnn.startsWith("CALENDAR_MONTH("))
				{
					return "CALENDAR_MONTH(";
				}
				else if (columnn.startsWith("CALENDAR_QUARTER("))
				{
					return "CALENDAR_QUARTER(";
				}
				else if (columnn.startsWith("CALENDAR_YEAR("))
				{
					return "CALENDAR_YEAR(";
				}
				else if (columnn.startsWith("DAY_IN_MONTH("))
				{
					return "DAY_IN_MONTH(";
				}
				else if (columnn.startsWith("DAY_IN_WEEK("))
				{
					return "DAY_IN_WEEK(";
				}
				else if (columnn.startsWith("DAY_IN_YEAR("))
				{
					return "DAY_IN_YEAR(";
				}
				else if (columnn.startsWith("FISCAL_MONTH("))
				{
					return "FISCAL_MONTH(";
				}
				else if (columnn.startsWith("FISCAL_QUARTER("))
				{
					return "FISCAL_QUARTER(";
				}
				else if (columnn.startsWith("FISCAL_YEAR("))
				{
					return "FISCAL_YEAR(";
				}
				else if (columnn.startsWith("HOUR_IN_DAY("))
				{
					return "HOUR_IN_DAY(";
				}
				else if (columnn.startsWith("WEEK_IN_MONTH("))
				{
					return "WEEK_IN_MONTH(";
				}
				else if (columnn.startsWith("WEEK_IN_YEAR("))
				{
					return "WEEK_IN_YEAR(";
				}
				return null;
			}
			
			private String getColumnDateFunctionPrefix(String columnn)
			{
				if (columnn.startsWith("DAY_ONLY(") && columnn.contains(FIRST_DAY_OF_MONTH_PREFIX))
				{
					return FIRST_DAY_OF_MONTH_PREFIX;
				}
				if (columnn.startsWith("DAY_ONLY(") && columnn.contains(FIRST_DAY_OF_QUARTER_PREFIX))
				{
					return FIRST_DAY_OF_QUARTER_PREFIX;
				}
				else if (columnn.startsWith("DAY_ONLY("))
				{
					return DAY_ONLY_PREFIX;
				}
				else if (columnn.startsWith("CALENDAR_MONTH("))
				{
					return CALENDAR_MONTH_PREFIX;
				}
				else if (columnn.startsWith("CALENDAR_QUARTER("))
				{
					return CALENDAR_QUARTER_PREFIX;
				}
				else if (columnn.startsWith("CALENDAR_YEAR("))
				{
					return CALENDAR_YEAR_PREFIX;
				}
				else if (columnn.startsWith("DAY_IN_MONTH("))
				{
					return DAY_IN_MONTH_PREFIX;
				}
				else if (columnn.startsWith("DAY_IN_WEEK("))
				{
					return DAY_IN_WEEK_PREFIX;
				}
				else if (columnn.startsWith("DAY_IN_YEAR("))
				{
					return DAY_IN_YEAR_PREFIX;
				}
				else if (columnn.startsWith("FISCAL_MONTH("))
				{
					return FISCAL_MONTH_PREFIX;
				}
				else if (columnn.startsWith("FISCAL_QUARTER("))
				{
					return FISCAL_QUARTER_PREFIX;
				}
				else if (columnn.startsWith("FISCAL_YEAR("))
				{
					return FISCAL_YEAR_PREFIX;
				}
				else if (columnn.startsWith("HOUR_IN_DAY("))
				{
					return HOUR_IN_DAY_PREFIX;
				}
				else if (columnn.startsWith("WEEK_IN_MONTH("))
				{
					return WEEK_IN_MONTH_PREFIX;
				}
				else if (columnn.startsWith("WEEK_IN_YEAR("))
				{
					return WEEK_IN_YEAR_PREFIX;
				}
				return null;
			}
			

			@Override
			public boolean getAllowsDuplicateColumns() {
				
				return true;
			}


			@Override
			public boolean getAllowsAggregateColumns() {
				
				return true;
			}
			
			private ArrayList<String> getHavingFiltersSOQL(ArrayList<FilterData> havingFilters) 
			{
				ArrayList<String> filtersSt=new ArrayList<String>();
				int i;
				for (i = 0; i <havingFilters.size(); i++) 
				{
					FilterData fData=havingFilters.get(i);
					String columnName="";
					
					if(fData.getFilterName().equals(NO_OF_RECORDS))
					{
						columnName="count(id)";
					}
					else columnName=fData.getFilterName();
					
					filtersSt.add(getQueryForFilter(fData, columnName));
					
					
				}
				return filtersSt;
			}
			
			private ArrayList<String> getFiltersSOQL(List<FilterData> filters) 
			{
				
				boolean isETLContext = false;
				
				try {

					isETLContext = isTransformationContext();

				} catch (NoSuchMethodError e) {


				}
				ArrayList<String> filtersSt=new ArrayList<String>();
				int i;
				for (i = 0; i < filters.size(); i++) 
				{
					FilterData fData=filters.get(i);
					String columnName="";
					
					if (isETLContext
							&& fData.getFilterMetaData().getFilterType() == DataType.NUMERIC
							&& (fData.getFilterOperator() == FilterOperator.EQUAL
							|| fData.getFilterOperator() == FilterOperator.NOTEQUAL)
							)
					{
						byte[] getDataTableByte=datasource.getData(table);
						JSONArray fields = new JSONArray();
						JSONObject description;
						if (getDataTableByte!=null)
						{
							fields=new JSONArray(new String(getDataTableByte));
						}
						else
						{
							description=describeTable(table);
							fields=description.getJSONArray("fields");
						}
						
						int j;
						
						
						for (j=0; j<fields.length(); j++)
						{
							if (fields.getJSONObject(j).has("name") && fields.getJSONObject(j).has("type"))
							{
								String tp=fields.getJSONObject(j).getString("type");
								String nm = fields.getJSONObject(j).getString("name");
								
								if ("boolean".equals(tp) && fData.getFilterName().equals(nm))
								{
									Integer currentValue = Integer.valueOf((String)fData.getFilterValue());
									Object newValue;
									if (currentValue == 0)
									{
										newValue = "false";
									}
									else
									{
										newValue = "true";
									}
									
									FilterData tempFData = new FilterData(fData.getFilterMetaData(), newValue);
									fData = tempFData;									
								}
									
							}
						}
					}
					
					if(fData.getFilterName().startsWith(DAY_ONLY_PREFIX))
					{
						columnName="DAY_ONLY("+fData.getFilterName().replaceFirst(DAY_ONLY_PREFIX,"")+")";
					}
					else columnName=fData.getFilterName();
					
					filtersSt.add(getQueryForFilter(fData, columnName));
					
										
				}
								
				
				return filtersSt;
			}
			
			private String getQueryForFilter(FilterData fData, String columnName)
			{
				String q="";
				
				if (fData.getFilterOperator().equals(FilterOperator.EQUAL))
				{
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"='"+fData.getFilterValue()+"'";
					}
					else if(fData.getFilterMetaData().getFilterType().equals(DataType.TIMESTAMP))
					{
						String time=fData.getFilterValue().toString();
						time=time.replaceFirst(" ", "T");
						time=time+"Z";
						q=columnName+"="+time;
					}					
					else q=columnName+"="+fData.getFilterValue();
					
					return q;
				}
				
				if (fData.getFilterOperator().equals(FilterOperator.NOTEQUAL))
				{
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"!='"+fData.getFilterValue()+"'";
					}
					else if(fData.getFilterMetaData().getFilterType().equals(DataType.TIMESTAMP))
					{
						String time=(String)fData.getFilterValue();
						time=time.replaceFirst(" ", "T");
						time=time+"Z";
						q=columnName+"!="+time;
					}
					else q=columnName+"!="+fData.getFilterValue();
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.BETWEEN))
				{
					List<Object> values;
					try
					{
						if (fData.getFilterValue() instanceof List<?>)
						{
							values=(List<Object>)fData.getFilterValue();
						}
						else throw new ThirdPartyException(SalesforceDataZoom.getText("Bad filter values received", "mi.text.salesforce.error.message4"));
						
					
					
						Object MIN, MAX;
						
						MIN=values.get(0);
						MAX=values.get(1);
						
						//System.out.println(MIN+"\t"+MAX);
						
						if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
						{
							q=columnName+">='"+MIN+"'"+" AND "+columnName+"<='"+MAX+"'";
						}
						
						else if(fData.getFilterMetaData().getFilterType().equals(DataType.TIMESTAMP))
						{
							String minTime=(String)MIN;
							String maxTime=(String)MAX;
							minTime=minTime.replaceFirst(" ", "T");
							minTime=minTime+"Z";
							
							maxTime=maxTime.replaceFirst(" ", "T");
							maxTime=maxTime+"Z";
							q=columnName+">="+minTime+" AND "+columnName+"<="+maxTime;
						}
						
						else q=columnName+">="+MIN+" AND "+columnName+"<="+MAX;
						
						return q;
					}
					catch(Exception e)
					{
						throw new ThirdPartyException(SalesforceDataZoom.getText("Bad Filter value!", "mi.text.salesforce.error.message5"));
					}
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.INLIST))
				{
					List<Object> values;
					if (fData.getFilterValue() instanceof List<?>)
					{
						values=(List<Object>)fData.getFilterValue();
					}
					else throw new ThirdPartyException(SalesforceDataZoom.getText("Bad filter values received", "mi.text.salesforce.error.message4"));
					String subQ="(";
					if (values.size()>0)
					{
						if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
						{
							int j;
							subQ=subQ+"'"+values.get(0)+"'";
							
							for(j=1; j<values.size(); j++)
							{
								subQ=subQ+", '"+values.get(j)+"'";
							}
							subQ=subQ+")";
						}
						
						else 
						{
							int j;
							subQ=subQ+values.get(0);
							
							for(j=1; j<values.size(); j++)
							{
								subQ=subQ+", "+values.get(j);
							}
							subQ=subQ+")";
						}
						q=columnName+" in "+subQ;
						return q;
					}
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.NOTINLIST))
				{
					List<Object> values;
					
					if (fData.getFilterValue() instanceof List<?>)
					{
						values=(List<Object>)fData.getFilterValue();
					}
					else throw new ThirdPartyException(SalesforceDataZoom.getText("Bad filter values received", "mi.text.salesforce.error.message4"));
					
					String subQ="(";
					if (values.size()>0)
					{
						if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
						{
							int j;
							subQ=subQ+"'"+values.get(0)+"'";
							
							for(j=1; j<values.size(); j++)
							{
								subQ=subQ+", '"+values.get(j)+"'";
							}
							subQ=subQ+")";
						}
						
						else 
						{
							int j;
							subQ=subQ+values.get(0);
							
							for(j=1; j<values.size(); j++)
							{
								subQ=subQ+", "+values.get(j);
							}
							subQ=subQ+")";
						}
						q=columnName+" NOT IN "+subQ;
						return q;
					}
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.GREATER))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+">'"+value+"'";
					}
					
					else q=columnName+">"+value;
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.LESS))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"<'"+value+"'";
					}
					
					else q=columnName+"<"+value;
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.GREATEREQUAL))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+">='"+value+"'";
					}
					
					else q=columnName+">="+value;
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.LESSEQUAL))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"<='"+value+"'";
					}
					
					else q=columnName+"<="+value;
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.NOTCONTAINS))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q="not ("+columnName+" like '%"+value+"%')";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.CONTAINS))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+" like '%"+value+"%'";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.STARTSWITH))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+" like '"+value+"%'";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.ENDSWITH))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+" like '%"+value+"'";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.NOTSTARTSWITH))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q="not ("+columnName+" like '"+value+"%')";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.NOTENDSWITH))
				{
					Object value=fData.getFilterValue();
										
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q="not ("+columnName+" like '%"+value+"')";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.ISEMPTYSTRING))
				{
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"=''";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.ISNOTEMPTYSTRING))
				{
					if(fData.getFilterMetaData().getFilterType().equals(DataType.TEXT))
					{
						q=columnName+"!=''";
					}
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.ISNOTNULL))
				{
					q=columnName+"!=null";
					
					return q;
				}
				
				else if (fData.getFilterOperator().equals(FilterOperator.ISNULL))
				{
					q=columnName+"==null";
					
					return q;
				}
				
				return q;
			}

			public JSONObject runQuery(String query)
			{
				JSONObject result;
				
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
				
				GetMethod get = new GetMethod(instanceURL+"/services/data/"+APIVERSION+"/query");
				get.setRequestHeader("Authorization", "OAuth " + accessToken);
				
				NameValuePair[] params = new NameValuePair[1];
				
				/* test: print SOQL statements 10 times on console */				
				
				/*for (int i = 0; i < 10; i++) 
				{
					System.out.println(query);
				}*/
				
				//query = query+" LIMIT 2000";
				params[0] = new NameValuePair("q",query);
				get.setQueryString(params);
				try 
				{
					httpclient.executeMethod(get);
					
					//result=new JSONObject(new JSONTokener(new InputStreamReader(get.getResponseBodyAsStream())));
					//String st=new String(get.getResponseBodyAsString());
					//result=new JSONObject(new JSONTokener(st));
					
					InputStreamReader reader;
					
					reader = new InputStreamReader(get.getResponseBodyAsStream(), "UTF-8");
					
					//System.out.println(get.getResponseBodyAsString());
					result=new JSONObject();
					Object tempResult=new JSONTokener(reader).nextValue();
					
					if (tempResult instanceof JSONObject)
					{
						
						result=(JSONObject)tempResult;
						return result;
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
						
						return null;
						
						
					}
					
					return result;
					
				
				} catch (HttpException e) {
					// TODO Auto-generated catch block
					throw new ThirdPartyException(e.getMessage());
					//return new JSONObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					throw new ThirdPartyException(e.getMessage());
					//return new JSONObject();
				}
				catch (JSONException e) {
					// TODO Auto-generated catch block
					//throw new ThirdPartyException(SalesforceDataZoom.getText("Malformed Query", "mi.text.salesforce.error.message6"));
					throw new ThirdPartyException(e.getMessage());
					//return new JSONObject();
				}
				

			}		
			
			private String runBulkJob(String query)
			{
				try
				{
					String result;
					if (maxResults!=-1)
					{
						query = query + " LIMIT "+maxResults;
					}
					//System.out.println(query);
					ConnectorConfig config = new ConnectorConfig();
					config.setSessionId(accessToken);
					config.setPrettyPrintXml(true);
					config.setRestEndpoint(instanceURL+"/services/async/"+APIVERSIONBULK);
					
					BulkConnection bulkConnection = new BulkConnection(config);
					
					
					JobInfo job = new JobInfo();
					job.setObject(table);
					job.setOperation(OperationEnum.query);
					job.setConcurrencyMode(ConcurrencyMode.Parallel);
					job.setContentType(ContentType.CSV);
					
					job = bulkConnection.createJob(job);
					
					assert job.getId() != null;
					job = bulkConnection.getJobStatus(job.getId());
					
					result = job.getId();
					
					
					BatchInfo info = null;
					ByteArrayInputStream bout =	new ByteArrayInputStream(query.getBytes());
					info = bulkConnection.createBatchFromStream(job, bout);
					
					//Bulk Query Use Bulk Query
					String[] queryResults = null;
					for(int i=0; i<100; i++) 
					{
						Thread.sleep(3000); //30 sec
						info = bulkConnection.getBatchInfo(job.getId(),	info.getId());
						
						if (info.getState() == BatchStateEnum.Completed) {
							QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId());
							queryResults = list.getResult();
							break;					
						}
						else if (info.getState() == BatchStateEnum.Failed) {
							throw new ThirdPartyException(info.getStateMessage());
							
						}
						else {
							//("-------------- waiting ----------" + info);
							
						}
					}
					
					if (queryResults != null) {
						for (String resultId : queryResults) {
							InputStreamReader reader;
							reader = new InputStreamReader(bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId), "UTF-8");
							BufferedReader r = new BufferedReader(reader);
							String line = "";
							
							//System.out.println(String.valueOf(reader));
							result="";
							while((line = r.readLine()) !=null)
							{
								result = result + line + "\n";
							}
						}
						
					}				
					return result;
				}
				catch (AsyncApiException e)
				{
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;				
			}
			
			private String getAggregatedColumName(ColumnMetaData column, List<String[]> cols) 
			{
				/*if (agg!=null)
				{
					for (int i = 0; i < 10; i++) 
					{
						System.out.println(agg.name()+"\t"+agg.ordinal());
					}
				}*/
				String columnName=column.getColumnName();
				if (column.getSelectedAggregation()==null)
				{
					/*if(column.getColumnType().equals(DataType.NUMERIC) || column.getColumnType().equals(DataType.INTEGER))
					{
						column.setSelectedAggregation(AggregationType.SUM);
						return "SUM("+columnName+") "+columnName;
					}
					else return columnName;*/
					if(column.getColumnType().equals(DataType.DATE) && columnName.startsWith(DAY_ONLY_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(DAY_ONLY_PREFIX, "");
						
						return normalizeColumnName("DAY_ONLY("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(CALENDAR_MONTH_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(CALENDAR_MONTH_PREFIX, "");
						
						return normalizeColumnName("CALENDAR_MONTH("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(CALENDAR_QUARTER_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(CALENDAR_QUARTER_PREFIX, "");
						
						return normalizeColumnName("CALENDAR_QUARTER("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(CALENDAR_YEAR_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(CALENDAR_YEAR_PREFIX, "");
						
						return normalizeColumnName("CALENDAR_YEAR("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(DAY_IN_MONTH_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(DAY_IN_MONTH_PREFIX, "");
						
						return normalizeColumnName("DAY_IN_MONTH("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(DAY_IN_WEEK_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(DAY_IN_WEEK_PREFIX, "");
						
						return normalizeColumnName("DAY_IN_WEEK("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(DAY_IN_YEAR_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(DAY_IN_YEAR_PREFIX, "");
						
						return normalizeColumnName("DAY_IN_YEAR("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(FISCAL_MONTH_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(FISCAL_MONTH_PREFIX, "");
						
						return normalizeColumnName("FISCAL_MONTH("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(FISCAL_QUARTER_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(FISCAL_QUARTER_PREFIX, "");
						
						return normalizeColumnName("FISCAL_QUARTER("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(FISCAL_YEAR_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(FISCAL_YEAR_PREFIX, "");
						
						return normalizeColumnName("FISCAL_YEAR("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(HOUR_IN_DAY_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(HOUR_IN_DAY_PREFIX, "");
						
						return normalizeColumnName("HOUR_IN_DAY("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(WEEK_IN_MONTH_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(WEEK_IN_MONTH_PREFIX, "");
						
						return normalizeColumnName("WEEK_IN_MONTH("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.INTEGER) && columnName.startsWith(WEEK_IN_YEAR_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(WEEK_IN_YEAR_PREFIX, "");
						
						return normalizeColumnName("WEEK_IN_YEAR("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.DATE) && columnName.startsWith(FIRST_DAY_OF_MONTH_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(FIRST_DAY_OF_MONTH_PREFIX, "");
						
						return normalizeColumnName("DAY_ONLY("+tmpColumnName+") "+columnName, cols);
					}
					else if(column.getColumnType().equals(DataType.DATE) && columnName.startsWith(FIRST_DAY_OF_QUARTER_PREFIX))
					{
						String tmpColumnName=columnName.replaceFirst(FIRST_DAY_OF_QUARTER_PREFIX, "");
						
						return normalizeColumnName("DAY_ONLY("+tmpColumnName+") "+columnName, cols);
					}
					
					
					else return columnName;
				}
				
				if (column.getSelectedAggregation().equals(AggregationType.AVG) && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("AVG("+columnName+") "+columnName, cols);
				}
				
				else if (column.getSelectedAggregation().equals(AggregationType.COUNT) && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("COUNT("+columnName+") "+columnName, cols);
				}
				
				else if (column.getSelectedAggregation().equals(AggregationType.COUNTDISTINCT) && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("COUNT_DISTINCT() "+columnName, cols);
				}
				
				else if (column.getSelectedAggregation().equals(AggregationType.MIN) && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("MIN("+columnName+") "+columnName, cols);
				}
				
				else if (column.getSelectedAggregation().equals(AggregationType.MAX)  && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("MAX("+columnName+") "+columnName, cols);
				}
				
				else if (column.getSelectedAggregation().equals(AggregationType.SUM)  && !columnName.startsWith(DAY_ONLY_PREFIX))
				{
					return normalizeColumnName("SUM("+columnName+") "+columnName, cols);
				}
				
				else return normalizeColumnName(columnName +" "+ columnName, cols);
			}
			
			private String normalizeColumnName(String column, List<String[]> cols) 
			{
				int i=1;
				while(!check(column, cols))
				{
					i++;
					column=column+i;
				}
				return column;
			}
			
			private boolean check(String column, List<String[]> cols)
			{
				
				for (String[] col:cols)
				{
					if (col[1].contains(" "))
					{
						col[1]=col[1].split(" ")[1];
					}
					if (column.contains(" "))
					{
						column=column.split(" ")[1];
					}
					if (col[1].equals(column))
					{
						return false;
					}
				}
				
				return true;
			}
			
			
			public JSONObject describeTable(String table)
			{
				String scheme = "https";
				Protocol baseHttps = Protocol.getProtocol(scheme);
				int defaultPort = baseHttps.getDefaultPort();

				ProtocolSocketFactory baseFactory = baseHttps.getSocketFactory();
				ProtocolSocketFactory customFactory = new CustomHttpsSocketFactory(baseFactory);

				Protocol customHttps = new Protocol(scheme, customFactory, defaultPort);
				Protocol.registerProtocol(scheme, customHttps);
				JSONObject result;
				/*02-02-17 modified by kelly */
	        	HttpClient httpclient = ProxyUtils.getHttpClientProxy();				
				/*02-02-17 modified by kelly */
				
				
				GetMethod get = new GetMethod(instanceURL+"/services/data/"+APIVERSION+"/sobjects/"+table+"/describe");
				get.setRequestHeader("Authorization", "OAuth " + accessToken);
				
				try {
					httpclient.executeMethod(get);
					
					result=new JSONObject(new JSONTokener(new InputStreamReader(get.getResponseBodyAsStream(), "UTF-8")));
					
					return result;
				} catch (HttpException e) {
					// TODO Auto-generated catch block
					throw new ThirdPartyException(e.getMessage());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					throw new ThirdPartyException(e.getMessage());
				}
			}
			
		

}
