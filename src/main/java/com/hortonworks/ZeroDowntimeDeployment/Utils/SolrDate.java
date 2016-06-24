package com.hortonworks.ZeroDowntimeDeployment.Utils;

public class SolrDate {
	// "yyyy-MM-dd'T':HH:mm:ssZ" to
	// 2016-06-23T:21:51:16Z
	public static String getSolrDate(String input) {
		return input.substring(0, 20) + "Z";
	}
}
