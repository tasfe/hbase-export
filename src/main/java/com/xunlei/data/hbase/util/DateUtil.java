package com.xunlei.data.hbase.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static Date getPrevDate() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}
	
	public static Date getCurrDate() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public static String getPrevDateStr() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return new SimpleDateFormat("yyyyMMdd").format(getPrevDate());
	}

	public static long getPrevDateStartTimestamp() {
		Date prevDay = getPrevDate();
		return prevDay.getTime();
	}

	public static long getCurrDateStartTimestamp() {
		Date currDay = getCurrDate();
		return currDay.getTime();
	}
	
	public static void main(String[] args) {
		//
		System.out.println(getPrevDateStartTimestamp() + "\t" + getCurrDateStartTimestamp());
	}
}
