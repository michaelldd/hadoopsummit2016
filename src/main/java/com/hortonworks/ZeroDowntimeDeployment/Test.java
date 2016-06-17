package com.hortonworks.ZeroDowntimeDeployment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		SimpleDateFormat parseDate = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
		Date d = new Date();
		String s = parseDate.format(d);
		System.out.println(s);
		
	}
}
