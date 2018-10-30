package com.yumtao.clickflow.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 日期工具类
 *
 * @author YQT
 */
public class DateUtil {

	// 2012-01-01 12:33:06
	public static SimpleDateFormat srcSdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	public static SimpleDateFormat destSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * 获取两个时间的间隔毫秒值
	 *
	 * @param begin 开始时间
	 * @param end   结束时间
	 * @return
	 */
	public static long getMillSecBetween(Date begin, Date end) {
		return end.getTime() - begin.getTime();
	}

	public static long getSecBetween(Date begin, Date end) {
		return (long) Math.ceil((end.getTime() - begin.getTime()) * 1.0 / 1000);
	}

	public static long getMillMinBetween(Date begin, Date end) {
		return (long) Math.ceil((end.getTime() - begin.getTime()) * 1.0 / (1000 * 60));
	}

	public static Date parse(String dateStr, SimpleDateFormat sdf) {
		try {
			return sdf.parse(dateStr);
		} catch (ParseException e) {
		}
		return null;
	}

	public static String format(Date date, SimpleDateFormat sdf) {
		try {
			return sdf.format(date);
		} catch (Exception e) {
		}
		return null;
	}

	public static void main(String[] args) {

		Date date = DateUtil.parse("18/Sep/2013:13:40:35", srcSdf);
		Date date2 = DateUtil.parse("18/Sep/2013:13:41:34", srcSdf);
		long millSecBetween = getMillMinBetween(date, date2);
		System.out.println(millSecBetween);

	}

}
