package com.hortonworks.ZeroDowntimeDeployment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		SimpleDateFormat parseDate = new SimpleDateFormat("yyyy-MM-dd'T':HH:mm:ssZ");
		Date d = new Date();
		String s = parseDate.format(d);
		System.out.println(getSolrDate(s));
		
	}
	
	public static String getSolrDate(String input) {
		return input.substring(0, 20) + "Z";
	}
}
