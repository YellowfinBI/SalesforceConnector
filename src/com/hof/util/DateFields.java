package com.hof.util;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class DateFields {

	private java.sql.Timestamp timestamp;
	private java.sql.Date dt;
	private java.sql.Date monthStartDate;
	private java.sql.Date yearStartDate;
	private java.sql.Date quarterStartDate;
	private java.sql.Date weekStartDate;
	
	private int dayOfWeek;
	private String dayOfWeekName;
	private int hour;
	private int monthNumber;
	private String monthName;
	
	private int quarter;
	private int year;
	private int dayOfMonth;
	
	
	public DateFields(java.sql.Timestamp time) 
	{
		setVariables(time);
	}
	
	public DateFields(java.sql.Timestamp time, int offset) 
	{
		setVariables(new java.sql.Timestamp(time.getTime()+offset));
	}

	private void setVariables(java.sql.Timestamp time)
	{
		Calendar cal=new GregorianCalendar();
		cal.set(Calendar.ERA, GregorianCalendar.AD);
		cal.setTimeInMillis(time.getTime());
		timestamp=time;
		
		
		hour=cal.get(Calendar.HOUR_OF_DAY);
		
		Calendar cal2=new GregorianCalendar();
		cal2.setTimeInMillis(cal.getTimeInMillis());
		cal2.set(Calendar.DAY_OF_MONTH, 1);
		cal2.set(Calendar.HOUR, 0);
		cal2.set(Calendar.MINUTE, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		monthStartDate=new java.sql.Date(cal2.getTimeInMillis());
		cal2.set(Calendar.MONTH, 0);
		yearStartDate=new java.sql.Date(cal2.getTimeInMillis());
		
		
		if (cal.get(Calendar.MONTH)>=0 && cal.get(Calendar.MONTH)<3)
		{
			cal2.set(Calendar.MONTH, 0);
			quarterStartDate=new java.sql.Date(cal2.getTimeInMillis());
			quarter=1;
		}
		else if (cal.get(Calendar.MONTH)>=3 && cal.get(Calendar.MONTH)<6)
		{
			cal2.set(Calendar.MONTH, 3);
			quarterStartDate=new java.sql.Date(cal2.getTimeInMillis());
			quarter=2;
		}
		else if (cal.get(Calendar.MONTH)>=6 && cal.get(Calendar.MONTH)<9)
		{
			cal2.set(Calendar.MONTH, 6);
			quarterStartDate=new java.sql.Date(cal2.getTimeInMillis());
			quarter=3;
		}
		else if (cal.get(Calendar.MONTH)>=9 && cal.get(Calendar.MONTH)<12)
		{
			cal2.set(Calendar.MONTH, 9);
			quarterStartDate=new java.sql.Date(cal2.getTimeInMillis());
			quarter=4;
		}
		
		
		
		dayOfWeek=cal.get(Calendar.DAY_OF_WEEK);
		dayOfWeekName=getWeekDayName(dayOfWeek);
		monthNumber=cal.get(Calendar.MONTH)+1;
		monthName=getMonthName(monthNumber-1);
		
		year=cal.get(Calendar.YEAR);
		
		
		dayOfMonth=cal.get(Calendar.DAY_OF_MONTH);
		
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		dt=new java.sql.Date(cal.getTime().getTime());
		while (cal.get(Calendar.DAY_OF_WEEK)!=cal.getFirstDayOfWeek())
		{
			cal.setTimeInMillis(cal.getTimeInMillis()-86400000);
		}
		weekStartDate=new java.sql.Date(cal.getTime().getTime());

	}
		
	
	public java.sql.Timestamp getTimestamp()
	{
		return timestamp;
	}
	
	public java.sql.Date getDate()
	{
		return dt;
	}
	
	public int getHour()
	{
		return hour;
	}

	public int getMonthNumber() {
		return monthNumber;
	}
	
	public String getMonthName() {
		return monthName;
	}
	
	public String getWeekdayName() {
		return dayOfWeekName;
	}
	
	public int getWeekdayNumber() {
		return dayOfWeek;
	}
	
	public java.sql.Date getWeekStartDate() {

		return weekStartDate;
	}
	
	public java.sql.Date getMonthStartDate() {
		return monthStartDate;
	}
	
	public java.sql.Date getQuarterStartDate() {

		return quarterStartDate;
	}
	
	public java.sql.Date getYearStartDate() {

		return yearStartDate;
	}
	
	public int getQuarter() {
		return quarter;
	}
	
	public int getYear() {
		return year;
	}
	
	public int getDayOfMonth() {
		return dayOfMonth;
	}
	
	
	private String getMonthName(int month)
	{
		switch (month)
		{
		case Calendar.JANUARY: return "January";
		case Calendar.FEBRUARY: return "February";
		case Calendar.MARCH: return "March";
		case Calendar.APRIL: return "April";
		case Calendar.MAY: return "May";
		case Calendar.JUNE: return "June";
		case Calendar.JULY: return "July";
		case Calendar.AUGUST: return "August";
		case Calendar.SEPTEMBER: return "September";
		case Calendar.OCTOBER: return "October";
		case Calendar.NOVEMBER: return "November";
		case Calendar.DECEMBER: return "December";
		default: return "";
		}
	}

	private String getWeekDayName(int day) {
		
		switch (day)
		{
		case Calendar.SUNDAY: return "Sunday";
		case Calendar.MONDAY: return "Monday";
		case Calendar.TUESDAY: return "Tuesday";
		case Calendar.WEDNESDAY: return "Wednesday";
		case Calendar.THURSDAY: return "Thursday";
		case Calendar.FRIDAY: return "Friday";
		case Calendar.SATURDAY: return "Saturday";
		default: return "";
		
		}
	}


}