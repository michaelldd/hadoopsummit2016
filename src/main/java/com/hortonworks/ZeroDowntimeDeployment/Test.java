package com.hortonworks.ZeroDowntimeDeployment;


public class Test {

	public static void main(String[] args) {
		
		String log = "Sun Jun 12 03:00:57 EDT 2016|server004|search_module|6.0|GET|http://www.homepage.com/|200|1.335|referral_url";
		String[] parts = log.split("\\|");
		//String datetime = parts[0];
		String host = parts[1];
		String module = parts[2];
		String version = parts[3];
		String method = parts[4];
		//String endpoint = parts[5];
		String responseCode = parts[6];
		String latency = parts[7];
		//String content = parts[8];
		System.out.println(host + ":" + module + ":" + version + ":" + method + ":" + responseCode + ":" + latency);
	}

}
