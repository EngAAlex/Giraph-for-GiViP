package com.unipg.givip.common.icmp;

import org.icmp4j.IcmpPingRequest;
import org.icmp4j.IcmpPingUtil;

public class Pinger {

	final static int timeout = 1000;
	
	public static long pingHostName(String hostname) {
		IcmpPingRequest request = IcmpPingUtil.createIcmpPingRequest();
		request.setTimeout(timeout);
		request.setHost(hostname);
		
		return IcmpPingUtil.executePingRequest(request).getDuration();
	
	}
	
}
