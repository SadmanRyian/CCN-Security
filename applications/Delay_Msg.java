/**
 * Delayed msg format
 * @author:RUI
 * */
package applications;

import java.util.List;
import java.util.ArrayList;
import core.DTNHost;
import core.SimClock;

public class Delay_Msg{
	private String query_key;
	private double time_received;
	private double TTL;
	private List<String> resource_list;
	private HostToSend host_to_send;
	private boolean valid = false;
	private int max_to_carry = 0;
	
	public Delay_Msg(){
		this.query_key = "-1";
		this.time_received = SimClock.getTime();;
		this.TTL = 1000.0;
		this.resource_list = new ArrayList<String>();
		this.host_to_send = new HostToSend();
		valid = true;
	}
	
	public Delay_Msg(Delay_Msg a){
		this.query_key = a.getQueryKey();
		this.time_received = a.getReceivedTime();
		this.TTL = a.getTTL();
		this.resource_list = a.getResourceList();
		this.host_to_send = a.getHostsToSend();
		this.valid = a.getValid();
		this.max_to_carry = a.getMaxToCarry();
	}
	
	public void setQueryKey(String key){
		this.query_key = key;
	}
	public String getQueryKey(){
		return this.query_key;
	}
	
	public void setReceivedTime(double time){
		this.time_received = time;
	}
	public void updateReceivedTime(double time){
		this.time_received = time;
	}
	public double getReceivedTime(){
		return this.time_received;
	}
	
	public void setTTL(double ttl){
		this.TTL = ttl;
	}
	public double getTTL(){
		return this.TTL;
	}
	
	public void setResourceList(List<String> rl){
		this.resource_list = rl;
	}
	public List<String> getResourceList(){
		return this.resource_list;
	}
	public void clearResourceList(){
		this.resource_list.clear();
	}
	
	public void addResource(String r){
		if(!resource_list.contains(r))
			resource_list.add(r);
	}
	
	public HostToSend getHostsToSend(){
		return this.host_to_send;
	}
	public void addHostsToSend(DTNHost h){
		this.host_to_send.addHost(h);
	}
	
	public boolean getValid(){
		return this.valid;
	}
	public void setValid(boolean v){
		this.valid = v;
	}
	
	public int getMaxToCarry(){
		return this.max_to_carry;
	}
	public void setMaxToCarry(int m){
		this.max_to_carry = m;
	}
}