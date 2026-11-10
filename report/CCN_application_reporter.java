/* 
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details. 
 */
package report;

import java.util.ArrayList;
import java.util.List;

import core.Application;
import core.ApplicationListener;
import core.DTNHost;
import core.SimClock;

public class CCN_application_reporter extends Report implements ApplicationListener {

	private int oppo_cahce_hit=0;
	private int oppo_cache_miss=0;
	private int query_count=0;
	private int static_cache_hit=0;
	private int static_cache_miss=0;
	private int response_count=0;
	private int total_interval=0;
	private int num_got_response = 0;
	private int msg_forwarded = 0;
	private int response_from_other = 0;
	private int not_response = 0;
	private int res_found = 0;
	private int duplicated_query = 0;
	private int forwarding_Stop_l= 0;
	private int forwarding_Stop_p= 0;
	private int forwarding_Stop_n= 0;

	private List<MessageRecoder> msg_record = new ArrayList<MessageRecoder>();
	
	public class MessageRecoder{
		private String query_key = "";
		private double sentTime;
		private double receivedTime;
		private String hostName = "";
		private boolean gotResponse = false;
		
		
		public double getInterval(){
			return this.receivedTime - this.sentTime;
		}
		
		public MessageRecoder(){
			this.sentTime = 0.0;
			this.receivedTime = 0.0;
		}
		
		public String getQueryKey(){
			return this.query_key;
		}
		public void setQueryKey(String k){
			this.query_key = k;
		}
		
		public double getSentTime(){
			return this.sentTime;
		}
		public void setSentTime(double time){
			this.sentTime = time;
		}
		
		public double getReceivedTime(){
			return this.receivedTime;
		}
		public void setReceivedTime(double time){
			this.receivedTime = time;
		}
		
		public String getHostName(){
			return this.hostName;
		}
		public void setHostName(String name){
			this.hostName = name;
		}
		
		public boolean getGotResponse(){
			return this.gotResponse;
		}
		public void setGotResponse(boolean t){
			this.gotResponse = t;
		}
	}
	
	public void gotEvent(String event, Object params, Application app,
			DTNHost host) {
		if (event.equals("oppoCacheHit")) {
			this.oppo_cahce_hit++;
		}
		
		if (event.equals("forwardingStopList")) {
			this.forwarding_Stop_l++;
		}
		if (event.equals("forwardingStopListPIT")) {
			this.forwarding_Stop_p++;
		}
		if (event.equals("forwardingStopNonce")) {
			this.forwarding_Stop_n++;
		}
		if (event.equals("oppoCacheMiss")) {
			this.oppo_cache_miss++;
		}
		if (event.equals("SentQuery")) {
			this.query_count++;
			
			MessageRecoder recorder = new MessageRecoder();
			recorder.setQueryKey((String)params);
			recorder.setSentTime(SimClock.getTime());
			recorder.setHostName(host.toString());
			
			msg_record.add(recorder);
		}
		if (event.equals("staticCacheHit")) {
			this.static_cache_hit++;
		}
		if (event.equals("staticCacheMiss")) {
			//this.delays.add((Double)params);
			this.static_cache_miss++;
		}
		
		if (event.equals("OriginalGotResponse")) {
			this.response_count++;
			
			int position = locate_query_key((String)params);
			if(position != -1){
				msg_record.get(position).setReceivedTime(SimClock.getTime());
				msg_record.get(position).setGotResponse(true);
				num_got_response ++;
			}
		}
		if (event.equals("MsgForwarded")){
			this.msg_forwarded ++;
		}
		if (event.equals("GotResponse")){
			this.response_from_other++;
		}
		
		if (event.equals("NotFound")){
			this.not_response++;
		}
		
		if (event.equals("ResFound")){
			this.res_found++;
		}
		
		if (event.equals("SentDuplicatedQuery"))
		{
			this.duplicated_query++;
		}
		
		
		//print state
		//print_state();
		
	}
	
	void print_state(){
		System.out.println("\r this.oppo_cahce_hit = " + this.oppo_cahce_hit);
		System.out.print("\r this.oppo_cache_miss = " + this.oppo_cache_miss);
	}
	
	public int locate_query_key(String key){
		for(MessageRecoder ms: msg_record){
			if(ms.getQueryKey().equals(key))
				return msg_record.indexOf(ms);
		}
		return -1;
	}
	
	
	
	public void calTotalInterval(){
		for(MessageRecoder ms : msg_record){
			if(ms.getGotResponse() == true){
				total_interval += ms.getInterval();
			}
		}
	}
	
	
	
	@Override
	public void done() {
		write("WebApp stats for scenario " + getScenarioName() + 
				"\nsim_time: " + format(getSimTime()));
		
		calTotalInterval();
		if(num_got_response == 0){
			num_got_response = 1;
		}
		
		String statsText = 
			"\noppo_cahce_hit: " + this.oppo_cahce_hit + 
			"\noppo_cache_miss: " + this.oppo_cache_miss +
			"\ndrop_list:  "		    + this.forwarding_Stop_l+
			"\ndrop_pit:  "		    + this.forwarding_Stop_p+
			"\ndrop_nonce:  "		    + this.forwarding_Stop_n+
			"\nquery_count: " + this.query_count +
			"\nduplicated_query: " + this.duplicated_query +
			"\nstatic_cache_hit: " + this.static_cache_hit +
			"\nstatic_cache_miss: " + this.static_cache_miss +
			"\nresponse_count: " + this.response_count +
			//"\nresource found: " + this.res_found + 
			//"\nmsg_forwarded: " + this.msg_forwarded +
			"\naverage_interval: " + this.total_interval/this.num_got_response
			;
		
		write(statsText);
		super.done();
	}

}
