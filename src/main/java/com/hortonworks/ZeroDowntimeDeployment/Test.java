package com.hortonworks.ZeroDowntimeDeployment;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Test {

	public static void main(String[] args) {
		
		Test tool = new Test();
		String fileName = "acclog_dev.log";
		
		ClassLoader classLoader = tool.getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				System.out.println(line);
			}

			scanner.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
