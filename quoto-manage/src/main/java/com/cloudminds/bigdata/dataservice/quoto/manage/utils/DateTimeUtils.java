package com.cloudminds.bigdata.dataservice.quoto.manage.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateTimeUtils {

	/**
	 * 
	 * @param day 当前日期的前几天（用负数）或后几天（用正数）
	 * @return
	 */
	public static String getLastDateByDay(int day,int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, day);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}

	/**
	 * 
	 * @param month 当前日期的前几月（用负数）或后几月（用正数）
	 * @return
	 */
	public static String getlastDateByMonth(int month,int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, month);
		cal.add(Calendar.DATE, 1);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}

	/**
	 * 
	 * @param year 当前日期的前几年（用负数）或后几年（用正数）
	 * @return
	 */
	public static String getlastDateByYear(int year,int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.YEAR, year);
		cal.add(Calendar.DATE, 1);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}

	/**
	 * 
	 * @param month 当前日期的前几月（用负数）的初始日期
	 * @return
	 */
	public static String getPreDateByMonth(int month,int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, month);
		String result = format.format(cal.getTime()) + "-01";
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
	
	public static String getPreDateByYear(int year,int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.YEAR, year);
		String result = format.format(cal.getTime()) + "-01-01";
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
	
	/**
	 * 
	 * @return 最近的一个小时候的时间
	 */
	public static String getlast1HOUR(int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
		String result = format.format(cal.getTime());
		if(type==2){
			result = result.substring(0,10);
		}
		return result;
	}
	
	/**
	 * 
	 * @return 返回本周的星期一的日期
	 */
	public static String ftDateWeek(int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.WEEK_OF_MONTH, 0);
		cal.set(Calendar.DAY_OF_WEEK, 2);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
	
	/**
	 * 
	 * @return 返回本月的初始日期
	 */
	public static String ftDateMonth(int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, 0);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
	
	/**
	 * 
	 * @return 返回本季度的初始日期
	 */
	public static String ftDateQuarter(int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		int currentMonth = cal.get(Calendar.MONTH) + 1;
		if (currentMonth >= 1 && currentMonth <= 3) {
			cal.set(Calendar.MONTH, 0);
		} else if (currentMonth >= 4 && currentMonth <= 6) {
			cal.set(Calendar.MONTH, 3);
		} else if (currentMonth >= 7 && currentMonth <= 9) {
			cal.set(Calendar.MONTH, 4);
		} else if (currentMonth >= 10 && currentMonth <= 12) {
			cal.set(Calendar.MONTH, 9);
		}
		cal.set(Calendar.DATE, 1);
		String result = format.format(cal.getTime());
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
	
	/**
	 * 
	 * @return 返回本年的初始日期
	 */
	public static String ftDateYear(int type) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy");
		Calendar cal = Calendar.getInstance();
		String result = format.format(cal.getTime()) + "-01-01";
		if(type==1){
			result = result + " 00:00:00";
		}
		return result;
	}
}
