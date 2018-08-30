package com.sample.AccessLog;

import java.util.Comparator;

public class IPAddress implements Comparable<IPAddress> {

	String ip;
	int count;

	IPAddress(String ip, int count) {
		this.ip = ip;
		this.count = count;
	}

	public int compareTo(IPAddress o) {
		int o1=o.count;
		if (o1 < this.count)
			return 1;
		else if (o1 > this.count)
			return -1;
		else
			return 0;
	}

/*	public int compare(IPAddress c1, IPAddress c2) {
		int o1=c1.count;
		int o2=c2.count;
		if (o1 > o2)
			return 1;
		else if (o1 < o2)
			return -1;
		else
			return 0;
	}*/


}
