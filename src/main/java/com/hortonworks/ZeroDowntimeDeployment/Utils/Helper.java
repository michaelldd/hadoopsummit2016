package com.hortonworks.ZeroDowntimeDeployment.Utils;

import java.util.List;

public class Helper {
	
	public static boolean isValidStd(double std) {
		return std > 0.00001;
	}

	public static double getStd(List<Double> rateList, double curMean) {

		if (rateList.size() < 2) {
			return 0;
		}

		double ret = 0;

		for (double d : rateList) {
			ret += (d - curMean) * (d - curMean);
		}

		return ret / (rateList.size() - 1);
	}

	public static double getMean(List<Double> rateList) {

		if (rateList.size() < 1) {
			return 0;
		}

		double ret = 0;
		for (double d : rateList) {
			ret += d;
		}
		return ret / rateList.size();
	}
}
