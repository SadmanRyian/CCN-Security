/* 
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details. 
 */
package applications;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import core.Application;
import core.DTNHost;
import core.Message;
import core.Settings;
import core.SimClock;
import core.SimScenario;
import core.World;

/**
 * @author mjpitka2
 * @author teemuk
 *
 */
public class WebApplication extends Application {
	/** Hotspot mode - simulates delays to access resources from infra server */
	public static final String WEB_HOTSPOT = "hotspot";
	/** Size of the opportunistic cache */
	public static final String WEB_CACHESIZE = "cacheSize";
	/** Static resources */
	public static final String WEB_STATICRESOURCES = "staticResources";
	/** Drop request message is response if found */
	public static final String WEB_DROPONRESPONSE = "dropRequestOnResponse";
	/** The resources that will be queried by this node */
	public static final String WEB_QUERYRESOURCES = "queryResources";
	/** Active = send queries */
	public static final String WEB_ACTIVE = "active";
	/** Query interval */
	public static final String WEB_QUERYINTERVAL = "queryInterval";
	/** Query destination - typically a node outside the simulation area */
	public static final String WEB_QUERYDESTINATION = "queryDestination";
	/** Seed for the RNG */
	public static final String WEB_RNGSEED = "rngSeed";
	/** Query size */
	public static final String WEB_QUERYSIZE = "querySize";
	/** Publish/Subscribe mode */
	public static final String WEB_PUBSUBMODE = "pubSubMode";

	
	/** Application ID */
	public static final String APP_ID = "fi.tkk.netlab.WebApplication";
	
	// XXX: Use concrete classes instead?
	private CacheModule cache;
	private CacheModule resources;
	
	// Queue for delayed responses
	private class DelayedResponse {
		public double			sendTime;
		public String			queryId;
		public DTNHost			destination;
		public ResourceWrapper	resource;
	}
	private List<DelayedResponse> delayedQueue;
	
	// Wrapper for resources when held in the resource database
	private class ResourceWrapper implements Cacheable {
		public double	delay;
		public int		size;
		public String	key;
		public Cacheable replicate() {
			ResourceWrapper r = new ResourceWrapper();
			r.delay = this.delay;
			r.size = this.size;
			r.key = this.key;
			return r;
		}
	}
	
	// Wrapper for message when held in the opportunistic cache
	private class MessageWrapper implements Cacheable {
		public Message	message;
		public MessageWrapper(){}
		public MessageWrapper(Message msg) {
			this.message = msg.replicate();
		}
		public Cacheable replicate() {
			MessageWrapper r = new MessageWrapper();
			r.message = this.message.replicate();
			return r;
		}
	}
	
	Map<String, Double>	sentQueries;
	
	// Settings and defaults
	private boolean	hotspot				= false;
	private int		cache_size			= 0;
	private boolean drop_on_response	= false;
	private boolean active				= false;
	private int		query_destination	= 0;
	private double	query_interval[]	= {10,20};
	private List<String> query_resources = null;
	private Random	rng					= null;
	private double	next_query			= 0;
	private int		query_size			= 1;
	private boolean pubSubMode			= false;
	
	/** 
	 * Creates a new web application with the given settings.
	 * 
	 * @param s	the Settings to use for initializing the application.
	 */
	public WebApplication(Settings s) {
		// Read settings
		if (s.contains(WEB_HOTSPOT)) {
			this.hotspot = s.getBoolean(WEB_HOTSPOT);
		}
		if (s.contains(WEB_CACHESIZE)) {
			this.cache_size = s.getInt(WEB_CACHESIZE);
		}
		if (s.contains(WEB_DROPONRESPONSE)) {
			this.hotspot = s.getBoolean(WEB_DROPONRESPONSE);
		}
		if (s.contains(WEB_QUERYRESOURCES)) {
			query_resources = Arrays.asList(
					s.getCsvSetting(WEB_QUERYRESOURCES));
		}
		if (s.contains(WEB_ACTIVE)) {
			this.active = s.getBoolean(WEB_ACTIVE);
		}
		if (s.contains(WEB_QUERYINTERVAL)) {
			this.query_interval = s.getCsvDoubles(WEB_QUERYINTERVAL, 2);
		}
		if (s.contains(WEB_QUERYDESTINATION)) {
			this.query_destination = s.getInt(WEB_QUERYDESTINATION);
		}
		if (s.contains(WEB_RNGSEED)) {
			this.rng = new Random(s.getInt(WEB_RNGSEED));
		} else {
			this.rng = new Random();
		}
		if (s.contains(WEB_QUERYSIZE)) {
			this.query_size = s.getInt(WEB_QUERYSIZE);
		}
		if (s.contains(WEB_PUBSUBMODE)) {
			this.pubSubMode = s.getBoolean(WEB_PUBSUBMODE);
		}
		
		// Setup
		this.cache = new LRUCache();
		this.cache.setSize(this.cache_size);
		this.resources = new StaticCache();
		this.delayedQueue = new ArrayList<DelayedResponse>();
		this.next_query = this.query_interval[0]
		                          + rng.nextDouble() * this.query_interval[1];
		this.sentQueries = new HashMap<String, Double>();
		
		// Setup static resources
		if (s.contains(WEB_STATICRESOURCES)) {
			String[] tmp = s.getCsvSetting(WEB_STATICRESOURCES);
			for (String str : tmp) {
				// Parse the resource settings
				String[] tmp2 = str.split("-");
				if (tmp2.length==3) {
					// Insert into the resource
					ResourceWrapper rw = new ResourceWrapper();
					rw.delay = Double.parseDouble(tmp2[2]);
					rw.size = Integer.parseInt(tmp2[1]);
					rw.key = tmp2[0];
					this.resources.insert(rw.key, rw, rw.size);
				} else {
					// XXX: Raise error
				}
			}
		}
		
		super.setAppID(APP_ID);
	}
	
	/** 
	 * Copy-constructor.
	 * 
	 * @param a
	 */
	public WebApplication(WebApplication a) {
		super(a);
		this.hotspot = a.isHotspot();
		this.cache = a.getCache().replicate();
		this.cache_size = a.cache_size;
		this.resources = a.getResources().replicate();
		this.drop_on_response = a.isDropOnResponse();
		this.query_destination = a.getQueryDestination();
		this.query_interval = new double[2];
		this.query_interval[0] = a.getQueryInterval()[0];
		this.query_interval[1] = a.getQueryInterval()[1];
		List<String> aList = null;
		if ( (aList = a.getQueryResources()) != null) {
			this.query_resources = new ArrayList<String>();
			for (String s : aList) {
				this.query_resources.add(new String(s));
			}
		}
		Map<String, Double> aMap = new HashMap<String, Double>();
		this.sentQueries = aMap;
		// XXX: Cheat and don't make a copy of the map
		this.active = a.isActive();
		this.next_query = a.getNextQuery();
		this.query_size = a.getQuerySize();
		this.rng = a.getRng();
		this.delayedQueue = new ArrayList<DelayedResponse>();
		this.pubSubMode = a.pubSubMode;
	}
	
	@Override
	public Message handle(Message msg, DTNHost host) {
		String type = (String)msg.getProperty("web_type");
		String URL = (String)msg.getProperty("web_URL");
		if (type==null || URL==null) return msg; // Not a web valid app message
		if (type.equalsIgnoreCase("request")) {
			// Check the resources
			ResourceWrapper resource =
				(ResourceWrapper)this.resources.lookup(URL);
			if (resource != null) {
				super.sendEventToListeners("StaticCacheHit", null, host);
				DelayedResponse r = new DelayedResponse();
				r.resource = resource;
				r.sendTime = SimClock.getTime() + resource.delay;
				r.destination = msg.getFrom();
				r.queryId = msg.getId();
				this.delayedQueue.add(r);
				return (this.drop_on_response)?(null):(msg);
			}
			// Check the opportunistic cache
			MessageWrapper aMessageWrapper =
				(MessageWrapper)this.cache.lookup(URL);
			//Message aMessage = (Message)this.cache.lookup(URL);
			if (aMessageWrapper != null) { // found a resource to return
				super.sendEventToListeners("SentResponseFromOpportunisticCache",
						null, host);
				String id = "response:" + URL + "-" + SimClock.getIntTime() +
					"-" + host.getAddress();
				Message m = new Message(host, msg.getFrom(), id,
						aMessageWrapper.message.getSize());
				m.addProperty("web_type", "resource");
				m.addProperty("web_queryId", msg.getId());
				m.setAppID(APP_ID);
				host.createNewMessage(m);
				return (this.drop_on_response)?(null):(msg);
			} else {
				if (this.cache_size>0) {
					super.sendEventToListeners( "OpportunisticCacheMiss", null,
							host);
				}
			}
		} else if (type.equalsIgnoreCase("resource")) {
			this.cache.insert(URL, new MessageWrapper(msg), msg.getSize());
			// If Publish/Subscribe mode is enabled, then accept requested~subscribed 
			// resources even when they are destined to another destination address (~published)
			if (msg.getTo()==host || this.pubSubMode) {

				String aSub = hasActiveSubscription((String)msg.getProperty("web_queryId"));
				if (this.sentQueries.containsKey(msg.getProperty("web_queryId")) || 
						(this.pubSubMode && aSub!=null)) {

					double delay = SimClock.getTime();				
					if (msg.getTo()==host){
						delay -= this.sentQueries.get(
								msg.getProperty("web_queryId")).doubleValue();
					}else{
						int start = aSub.indexOf("-");
						int end = aSub.indexOf("-",start+1);
						delay -= Double.parseDouble(aSub.substring(start,end));
					}
					super.sendEventToListeners("GotResponse", new Double(delay),
							host);
				} 
			}
		}
		return msg;
	}
	
	private String hasActiveSubscription(String web_queryId){
		for(String query :this.sentQueries.keySet()){
			if(web_queryId.substring(web_queryId.indexOf(':'),web_queryId.indexOf('-')).equalsIgnoreCase(query.substring(query.indexOf(':'),query.indexOf('-')))){
				return query;	
			}
		}
		return null;
	}

	
	@Override
	public Application replicate() {
		return new WebApplication(this);
	}

	@Override
	public void update(DTNHost host) {
		// Handle delayed responses
		double now = SimClock.getTime();
		// XXX: could possibly optimize by keeping the list sorted
		ArrayList<DelayedResponse> removeList =
			new ArrayList<DelayedResponse>();
		for (DelayedResponse r : this.delayedQueue) {
			if (r.sendTime<=now) {
				String id = "response:" + r.resource.key + "-" + now +"-" +
					host.getAddress();
				Message m = new Message(host, r.destination, id,
						r.resource.size);
				m.addProperty("web_type", "resource");
				m.addProperty("web_URL", r.resource.key);
				m.addProperty("web_queryId", r.queryId);
				m.setAppID(APP_ID);
				host.createNewMessage(m);
				removeList.add(r);
				super.sendEventToListeners("SentResponseFromStaticCache", null,
						host);
				//this.delayedQueue.remove(r);
			}
		}
		// XXX: Got concurrent access exceptions by doing remove() in the
		// above loop.
		for (DelayedResponse r : removeList) {
			this.delayedQueue.remove(r);
		}
		
		// Handle queries
		if (this.isActive() && this.getQueryResources() != null) {
			if (now>=this.getNextQuery()) {
				// pick resource to query
				int i = this.rng.nextInt(this.query_resources.size());
				String URL = this.query_resources.get(i);
				String id = "request:" + URL + "-" + now + "-"
					+ host.getAddress();
				World w = SimScenario.getInstance().getWorld();
				DTNHost destination = w.getNodeByAddress(
						this.query_destination);
				Message m = new Message(host, destination, id,
						this.query_size);
				m.addProperty("web_type", "request");
				m.addProperty("web_URL", URL);
				m.setAppID(APP_ID);
				host.createNewMessage(m);
				
				this.next_query = now + this.query_interval[0]
				    		        + rng.nextDouble() * this.query_interval[1];
				addToSentList(id);
				super.sendEventToListeners("CreatedQuery", null, host);
			}
		}
	}

	private void addToSentList(String id) {
		this.sentQueries.put(id, new Double(SimClock.getTime()));
	}
	
	
	
	
	
	// Getters/Setters
	public boolean isHotspot() {
		return hotspot;
	}

	public void setHotspot(boolean hotspot) {
		this.hotspot = hotspot;
	}

	public CacheModule getCache() {
		return cache;
	}

	public void setCache(CacheModule cache) {
		this.cache = cache;
	}

	public CacheModule getResources() {
		return resources;
	}

	public void setResources(CacheModule resources) {
		this.resources = resources;
	}

	public boolean isDropOnResponse() {
		return drop_on_response;
	}

	public void setDropOnResponse(boolean drop_on_response) {
		this.drop_on_response = drop_on_response;
	}

	/**
	 * @return the active
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * @param active the active to set
	 */
	public void setActive(boolean active) {
		this.active = active;
	}

	/**
	 * @return the query_destination
	 */
	public int getQueryDestination() {
		return query_destination;
	}

	/**
	 * @param query_destination the query_destination to set
	 */
	public void setQueryDestination(int query_destination) {
		this.query_destination = query_destination;
	}

	/**
	 * @return the query_interval
	 */
	public double[] getQueryInterval() {
		return query_interval;
	}

	/**
	 * @param query_interval the query_interval to set
	 */
	public void setQueryInterval(double[] query_interval) {
		this.query_interval = query_interval;
	}

	/**
	 * @return the query_resources
	 */
	public List<String> getQueryResources() {
		return query_resources;
	}

	/**
	 * @param query_resources the query_resources to set
	 */
	public void setQueryResources(List<String> query_resources) {
		this.query_resources = query_resources;
	}

	/**
	 * @return the rng
	 */
	public Random getRng() {
		return rng;
	}

	/**
	 * @param rng the rng to set
	 */
	public void setRng(Random rng) {
		this.rng = rng;
	}

	/**
	 * @return the last_query
	 */
	public double getNextQuery() {
		return next_query;
	}

	/**
	 * @param last_query the last_query to set
	 */
	public void setNextQuery(double next_query) {
		this.next_query = next_query;
	}

	/**
	 * @return the query_size
	 */
	public int getQuerySize() {
		return query_size;
	}

	/**
	 * @param query_size the query_size to set
	 */
	public void setQuerySize(int query_size) {
		this.query_size = query_size;
	}
}
