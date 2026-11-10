/**
 * host to send back structure
 * @author:RUI
 * */
package applications;

import java.util.List;
import java.util.ArrayList;
import core.DTNHost;

public class HostToSend{
	private static final int MAX_NUM_HOSTS = 5;
//	private String query_key;
	private List<DTNHost> host_to_send_list;
	private int max_num_hosts = MAX_NUM_HOSTS;
	private int current_num_hosts = 0;
	
	public HostToSend(){
//		this.query_key = qk;
		this.host_to_send_list = new ArrayList<DTNHost>();
		this.host_to_send_list.clear();
		this.max_num_hosts = MAX_NUM_HOSTS;
	}
	
	public HostToSend(HostToSend h){
//		this.query_key = h.query_key;
		this.host_to_send_list = new ArrayList<DTNHost>();
		this.host_to_send_list = h.host_to_send_list;
		this.max_num_hosts = h.max_num_hosts;
		this.current_num_hosts = h.current_num_hosts;
	}
	
//	public void setQueryKey(String qk){
//		this.query_key = qk;
//	}
//	public String getQueryKey(){
//		return this.query_key;
//	}
	
	public int getCurrentNumHosts(){
		return this.current_num_hosts;
	}
	
	public boolean lookUpHost(DTNHost h){
		if(this.host_to_send_list == null)
			return false;
		if(this.host_to_send_list.contains(h))
			return true;
		return false;
	}
	public void addHost(DTNHost h){
		if(this.host_to_send_list == null){
			this.host_to_send_list = new ArrayList<DTNHost>();
		}
		if(!this.host_to_send_list.contains(h)){
			this.host_to_send_list.add(h);
		}
	}
	
	public List<DTNHost> getHostToSendList(){
		return this.host_to_send_list;
	}
}