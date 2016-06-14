package com.hortonworks.ZeroDowntimeDeployment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Test {

	public static void main(String[] args) {
		
		Map<MyObj, List<Integer>> map = new HashMap<>();
		
		map.put(new MyObj("a", "b"), new ArrayList<Integer>());
		
		System.out.println(map.containsKey(new MyObj("a", "b")));
		
	}

}

class MyObj{
	public String str1;
	public String str2;
	public MyObj(String str1, String str2) {
		this.str1 = str1;
		this.str2 = str2;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((str1 == null) ? 0 : str1.hashCode());
		result = prime * result + ((str2 == null) ? 0 : str2.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyObj other = (MyObj) obj;
		if (str1 == null) {
			if (other.str1 != null)
				return false;
		} else if (!str1.equals(other.str1))
			return false;
		if (str2 == null) {
			if (other.str2 != null)
				return false;
		} else if (!str2.equals(other.str2))
			return false;
		return true;
	}
	

	
}