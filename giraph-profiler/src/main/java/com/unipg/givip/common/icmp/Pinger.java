package com.unipg.givip.common.icmp;

//import org.apache.log4j.Logger;
import org.icmp4j.IcmpPingRequest;
import org.icmp4j.IcmpPingResponse;
import org.icmp4j.IcmpPingUtil;

public class Pinger {
	
//	static Logger LOG = Logger.getLogger(Pinger.class);

	final static int timeout = 2000;
	
	public static long pingHostName(String hostname) {
		IcmpPingRequest request = IcmpPingUtil.createIcmpPingRequest();
		request.setTimeout(timeout);
		request.setHost(hostname);
		
		IcmpPingResponse response = IcmpPingUtil.executePingRequest(request);
		return response.getDuration();
	
	}
	
}
