/* 
 * Copyright 2010 Aalto University, DCS Lab
 * Released under GPLv3. See LICENSE.txt for details. 
 * @author: RUI and Hasan (hasan02.buet@gmail.com)
 */

package applications;

import java.util.Random;

import report.PingAppReporter;
import core.Application;
import core.DTNHost;
import core.DTNSim;
import core.Message;
import core.Settings;
import core.SimClock;
import core.SimScenario;
import core.World;

//
import core.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author RUI and Hasan
 */

public class CCN_application extends Application {
	static {
		  DTNSim.registerForReset(CCN_application.class.getCanonicalName());
		  reset();
		  
	}
	
	/** Run in producer mode - don't generate content but respond */
	public static final String CCN_PASSIVE = "passive";
	/** interest generation interval */
	public static final String CCN_INTERVAL = "interval";
	/** interest interval offset - avoids synchronization of ping sending */
	public static final String CCN_OFFSET = "offset";
	/** Destination address range - inclusive lower, exclusive upper */
	public static final String CCN_DEST_RANGE = "destinationRange";
	/** Seed for the app's random number generator */
	public static final String CCN_SEED = "seed";
	/** Size of the interest message */
	public static final String CCN_INTEREST_SIZE = "interestSize";
	/** Size of the response message */
	public static final String CCN_CONTENT_SIZE = "contentSize";	
	/** Application ID */
	public static final String APP_ID = "fi.dcslab.CCN_application";
	
	/** Capacity of Cache */
	public static final String CACHE_CAPACITY = "cacheCapacity";
	/** Opportunity cache value range*/
	public static final String OPPO_CACHE_RANGE = "oppoCacheRange";
	/** Seed to generate query*/
	public static final String SEED_QUERY = "seedQuery";
	/** Query range*/
	public static final String QUERY_RANGE = "queryRange";

	// Private vars
	private double		lastPing = 0;
	private double		interval = 500;
	private boolean		passive = false;
	private int			seed = 0;
	private int			destMin=0;
	private int			destMax=1;
	private int			interestSize=1;
	private int			contentSize=1;
	private int			queryResponseSize = 10;
	private Random		rng;

	/** if MODE = 1, node is consumer; if MODE = 2, node is producer; if MODE = 3, node is intermedia node*/
	/** All nodes have both static and oppo cache. Consumer produces query. Producers provides content. Intermedia node is random host */
	private int			mode = 1;
	public static final String MODE = "mode"; 
	
	/** opportunity cache */
	private int			capacity_of_cache = 5;
	private LRUCache 	oppo_cache = null;
	private int			seed_for_create_cache = 8;
	private int			cache_value_range_Min = 1;
	private int			cache_value_range_Max = 100;
	
	private int			seed_for_query = 6;
	private Random 		rng_query;
	private int 		query_range_Min = 1;
	private int 		query_range_Max = 100;
	private int 		max_carried_content = 5;
	public static final String MAX_CARRIED_CONTENT = "maxCarriedContent";
	
	/** query record: first integer is the query_key, the second is to monitor does this query_key get response or not (1 for yes, 0 for no) */
	private HashMap<Integer, Integer> query_record;
	
	/** static cache */
	private HashMap<Integer, String> static_cache;
	public static final		String STATIC_CACHE_RANGE = "staticCacheRange";
	public static final		String CAPACITY_OF_STATIC_CACHE = "staticCacheCapacity";
	public static final		String SEED_FOR_CREATE_STATIC_CACHE = "seedStaticCache";
	private int				static_cache_value_Min = 1;
	private int				static_cache_value_Max = 100;
	private int				capacity_of_static_cache = 25;	//this value is for whole static cache content, not for single host
	private int				seed_for_create_static_cache = 16;
	public static final 	String queryResultsSelectionSeed = "seedQueryResult";
	private					Random rng_query_selection;
	private int 			seed_query_result = 10; //random selected as 10
	
	/** availability of opportunity and static cache */
	public static final 	String AVA_CACHE = "avaCache";
	private int				true_static_cache = 1;
	private int				true_oppo_cache = 1;
	private boolean			ini_cache_again = true;
	
	/** delay queue */
	private int				max_hold_query = 10;
	private List<Delay_Msg> delay_queue = new ArrayList<Delay_Msg>();
	private static final 	String TTL_FOR_DELAY_QUEUE = "ttlForDelayQueue";
	private double			ttl_for_delay_queue = 500.0;	
	
	/** control num of msg to generate */
	public static final String NUM_OF_MSG_TO_GENERATE = "numOfMsgToGenerate";
	private int			num_of_msg_to_generate = 50;
	private int			curGenerated = 0;
	
	/** Pending Interest Table */
	private HashMap<Integer, HostToSend> PIT = new HashMap<Integer, HostToSend>();
	private static final String Enable_PIT = "enablePIT";
	private boolean enablePIT = true;

	/** capacity of the Pending Interest Table (per host); <=0 means unlimited (default, preserves existing behavior) */
	private static final String PIT_CAPACITY = "pitCapacity";
	private int			pitCapacity = -1;
	/** lifetime (seconds) of a PIT entry before it expires; <=0 means no expiry (default, preserves existing behavior) */
	private static final String PIT_ENTRY_LIFETIME = "interestLifetime";
	private double		pitEntryLifetime = -1.0;
	/** insertion time (SimClock seconds) for each currently pending PIT entry, keyed the same way as PIT itself */
	private HashMap<Integer, Double> pitInsertTime = new HashMap<Integer, Double>();

	/** Ground-truth only (CachePoisoningReporter instrumentation): tracks
	 * whether the content currently resident at a given id, in THIS host's
	 * own oppo_cache / static_cache, was written by forged ("isPoison") or
	 * legitimate Data. Never read by any caching/forwarding/acceptance
	 * logic - only by the new events fired alongside the pre-existing
	 * cache read/write code paths. */
	private HashMap<Integer, Boolean> oppoPoisonMarker = new HashMap<Integer, Boolean>();
	private HashMap<Integer, Boolean> staticPoisonMarker = new HashMap<Integer, Boolean>();

	/** Optional cache-poisoning defense (Experiment E): when true, Data is only
	 * admitted into oppo_cache/static_cache if this host actually had a pending
	 * PIT entry for it (host_to_send != null) - i.e. this host or a downstream
	 * neighbor had genuinely asked for that content. Default false preserves
	 * the fork's original unconditional-caching behavior exactly. */
	private static final String CACHE_ADMISSION_REQUIRES_PIT = "cacheAdmissionRequiresPIT";
	private boolean cacheAdmissionRequiresPIT = false;

	/** BPSec-inspired integrity protection (see applications/CCNSecurity.java).
	 * Read from a dedicated "CCNSecurity" namespace, independent of which
	 * Group/app-type this instance belongs to - same precedent already used
	 * by CCNRouter, which reads its own "CCNRouter" namespace regardless of
	 * group. This guarantees every Producer/Consumer/Intermedia instance
	 * derives the identical key with zero duplication risk. */
	private static final int SECURITY_POLICY_ACCEPT = 1;
	private static final int SECURITY_POLICY_REJECT = 2;
	private static final int SECURITY_POLICY_QUARANTINE = 3;

	private static final int SECURITY_OUTCOME_NA = 0;      // security disabled
	private static final int SECURITY_OUTCOME_VALID = 1;
	private static final int SECURITY_OUTCOME_MISSING = 2;
	private static final int SECURITY_OUTCOME_FAILED = 3;
	private static final int SECURITY_OUTCOME_REPLAY = 4;

	private boolean securityEnabled = false;
	private int securityPolicy = SECURITY_POLICY_ACCEPT;
	private int securityKeySeed = 0;
	private double securityFreshnessWindow = -1.0;
	/** When false, skip the per-(source,id) replay/freshness check entirely -
	 *  ANY validly-tagged packet is treated as VALID, even an exact replay.
	 *  Default true (replay checking active whenever security is enabled)
	 *  so this must be explicitly disabled to run an "integrity-only, no
	 *  anti-replay" experiment that isolates what integrity alone catches. */
	private boolean securityReplayCheckEnabled = true;
	private byte[] securityKey = null;

	/** Per-host replay state: latest ACCEPTED creationTime seen for a given
	 * (securitySource, queryMsg) pair. A new Data for the same pair with a
	 * creationTime <= the stored value is classified as a replay; one with
	 * a strictly greater creationTime is not, and updates the stored value.
	 * This means repeated legitimate requests for the same content id are
	 * never flagged - a fresh legitimate response always carries a newer
	 * creationTime than any earlier one, since SimClock is monotonic. */
	private HashMap<String, Double> replayState = new HashMap<String, Double>();
	
	/** Processed Packages List */
	private int		cap_of_list = 500;
	private List<String> procedMsgList = new ArrayList<String>();
	
	
	/** Zipf destribution parameters */
	private static final    String queryDistributionString = "queryDistribution"; //1 for normal random query, 2 for ZipF distribution
	private int				queryDistribution = 1;	// 1 for default
	private int				zipfDistRang = 500; // the range of zipf distribution
	private int				numOfPopuCont = -1; // number of popular content
	private double			topRatio = 0.2; //most popular content hit ratio (smaller than 1)
	private int[] 			contentLadder = new int[zipfDistRang]; //rank 1 ( 1 <--> n1 ), rank 2 ( n1+1 <--> n2 ) ....
	private int				paraS = 1; // 1 for default
	private Random 			zipfDistRandGeneSeed;
	private int[]			popularContent = null;
	private int				seedForCreatePopuContent = 2;
	private int				try_time = 50;
	
	private List<Integer> generatedQueryKeysList = new ArrayList<Integer>();
	
	/** reset application */
	public static void reset()
	{
	}
	/** 
	 * @param s	Settings to use for initializing the application.
	 */
	public CCN_application(Settings s) {
		//test code
		System.out.println("Creating application from setting only once");
		
		if (s.contains(CCN_PASSIVE)){
			this.passive = s.getBoolean(CCN_PASSIVE);
		}
		if (s.contains(CCN_INTERVAL)){
			this.interval = s.getDouble(CCN_INTERVAL);
		}
		if (s.contains(CCN_OFFSET)){
			this.lastPing = s.getDouble(CCN_OFFSET);
		}
		if (s.contains(CCN_SEED)){
			this.seed = s.getInt(CCN_SEED);
		}
		if (s.contains(CCN_INTEREST_SIZE)) {
			this.interestSize = s.getInt(CCN_INTEREST_SIZE);
		}
		if (s.contains(CCN_CONTENT_SIZE)) {
			this.contentSize = s.getInt(CCN_CONTENT_SIZE);
		}
		if (s.contains(CCN_DEST_RANGE)){
			int[] destination = s.getCsvInts(CCN_DEST_RANGE,2);
			this.destMin = destination[0];
			this.destMax = destination[1];
		}
		/** control the num of msg to generate */
		if(s.contains(NUM_OF_MSG_TO_GENERATE)){
			this.num_of_msg_to_generate = s.getInt(NUM_OF_MSG_TO_GENERATE);
		}
		/** mode */
		if(s.contains(MODE)){
			mode = s.getInt(MODE);
			if(mode != 1 && mode != 2 && mode != 3){
				mode = 1;
			}
		}
		/** availability of cache */
		if (s.contains(AVA_CACHE)){
			int[] temp = s.getCsvInts(AVA_CACHE, 2);
			true_static_cache = temp[0];
			true_oppo_cache = temp[1];
		}
		/** initialize the cache */
		if (s.contains(OPPO_CACHE_RANGE)){
			int[] temp = s.getCsvInts(OPPO_CACHE_RANGE, 2);
			cache_value_range_Min = temp[0];
			cache_value_range_Max = temp[1];
		}
		if (s.contains(CACHE_CAPACITY)){
			this.capacity_of_cache = s.getInt(CACHE_CAPACITY);	
		}
		if (s.contains(STATIC_CACHE_RANGE)){
			int[] temp = s.getCsvInts(STATIC_CACHE_RANGE, 2);
			static_cache_value_Min = temp[0];
			static_cache_value_Max = temp[1];
		}
		if (s.contains(CAPACITY_OF_STATIC_CACHE)){
			this.capacity_of_static_cache = s.getInt(CAPACITY_OF_STATIC_CACHE);
		}
		if (s.contains(SEED_FOR_CREATE_STATIC_CACHE)){
			this.seed_for_create_static_cache = s.getInt(SEED_FOR_CREATE_STATIC_CACHE);
		}

		/** selection of results once two hits while there is only one position left in msg */
		if (s.contains(queryResultsSelectionSeed)){
			this.seed_query_result = s.getInt(queryResultsSelectionSeed);
		}
		rng_query_selection = new Random(this.seed_query_result);

		/** initialize the query msg sequence*/
		if (s.contains(SEED_QUERY)){
			this.seed_for_query = s.getInt(SEED_QUERY);
		}
		
		/** get query range*/
		if (s.contains(QUERY_RANGE)){
			int[] query_range = s.getCsvInts(QUERY_RANGE,2);
			this.query_range_Min = query_range[0];
			this.query_range_Max = query_range[1];
		}
		if (s.contains(MAX_CARRIED_CONTENT)){
			this.max_carried_content = s.getInt(MAX_CARRIED_CONTENT);
		}
		if (s.contains(TTL_FOR_DELAY_QUEUE)){
			this.ttl_for_delay_queue = s.getInt(TTL_FOR_DELAY_QUEUE);
		}
		
		if(this.query_record == null){
			query_record = new HashMap<Integer, Integer>();			
			this.query_record.clear();  /** clear query_record list */
		}
		
		
		if(s.contains(Enable_PIT)){
			this.enablePIT = s.getBoolean(Enable_PIT);
		}

		if(s.contains(PIT_CAPACITY)){
			this.pitCapacity = s.getInt(PIT_CAPACITY);
		}
		if(s.contains(PIT_ENTRY_LIFETIME)){
			this.pitEntryLifetime = s.getDouble(PIT_ENTRY_LIFETIME);
		}
		if(s.contains(CACHE_ADMISSION_REQUIRES_PIT)){
			this.cacheAdmissionRequiresPIT = s.getBoolean(CACHE_ADMISSION_REQUIRES_PIT);
		}

		/** BPSec-inspired integrity settings - dedicated namespace, not tied
		 * to this instance's own Group/app-type, so every Producer/Consumer/
		 * Intermedia derives the identical key. Defaults (enabled=false)
		 * preserve existing behavior exactly unless explicitly turned on. */
		Settings ccnSecSettings = new Settings("CCNSecurity");
		if(ccnSecSettings.contains("enabled")){
			this.securityEnabled = ccnSecSettings.getBoolean("enabled");
		}
		if(ccnSecSettings.contains("policy")){
			this.securityPolicy = ccnSecSettings.getInt("policy");
		}
		if(ccnSecSettings.contains("keySeed")){
			this.securityKeySeed = ccnSecSettings.getInt("keySeed");
		}
		if(ccnSecSettings.contains("freshnessWindow")){
			this.securityFreshnessWindow = ccnSecSettings.getDouble("freshnessWindow");
		}
		if(ccnSecSettings.contains("replayCheckEnabled")){
			this.securityReplayCheckEnabled = ccnSecSettings.getBoolean("replayCheckEnabled");
		}
		this.securityKey = CCNSecurity.deriveKeyFromSeed(this.securityKeySeed);
				
		this.PIT.clear();  /** clear PIT table */
		
		/** get distribution type */
		if (s.contains(queryDistributionString))
		{
			this.queryDistribution = s.getInt(queryDistributionString);
			if(this.queryDistribution < 1 || this.queryDistribution > 2)
			{
				//right now, we only have two kinds of distribution. If more added, the value range should change also
				this.queryDistribution = 1;
				
				print("Invalid queryDistribution, this value will be set to default");
			}
		}
				
		if(this.mode == 1 && this.queryDistribution == 2)
		{
			//(Only consumer has) zipf distribution, initializing the contentLadder and the contentLadder will be copied to every nodes in one group in class constructor
			//createContentLadder();
			createContentLadder();
			createPopularContent();
		}
		
		super.setAppID(APP_ID);
	}

	/**
	* initialize the cache content
	*/	
	public void Ini_oppo_cache(){
		if(true_oppo_cache >= 1){
			/** initialize oppo_cache */
			this.oppo_cache = new LRUCache(this.capacity_of_cache);
		}
	}
		
	/** test code */
	public void print_oppo_cache(DTNHost host){
		LRUCache print_cache = new LRUCache(this.oppo_cache);
	}
	
	
	/** initialize the static cache */
	public void Ini_static_cache(DTNHost h){
		if(static_cache == null){
			this.static_cache 
			= new HashMap<Integer, String>();
		}
		
		System.out.println("Init static cache for host "+ h +  "if value is " + true_static_cache);
		if(true_static_cache >= 1){
			/** initialize static_cache */
			//get the num of the host since the host num starting from 0
			int seed_from_host = Integer.parseInt(h.toString().substring(1)) + 1;
			//get the num of hosts in the simulation scenario
			int num_of_hosts = SimScenario.getInstance().getWorld().getHosts().size();
			//calculate the range of values for every host
			int range = (static_cache_value_Max - static_cache_value_Min)/num_of_hosts + 1;
			System.out.println("Init static cache: "+ num_of_hosts + " why range: "+ range);
			//distribution setting: all 1-20
			int max_range = static_cache_value_Max;
			int min_range = static_cache_value_Min;
			num_of_hosts = 1;
			
			//test code
			System.out.println("Host:  range max: + mim " + max_range + " " + min_range);
			
			Random rng_static_cache = new Random(this.seed_for_create_static_cache + seed_from_host);
			
			for(int i = 0; i < capacity_of_static_cache/num_of_hosts ; i++){
				int temp = min_range + rng_static_cache.nextInt(max_range - min_range);
				if(static_cache.containsKey(temp)){
					i--;
					continue;
				}
				static_cache.put(temp, new Integer(i+1).toString());
			}
			//test code
			System.out.println();			
		}
	}
	
	/** 
	 * Copy-constructor
	 * 
	 * @param a
	 */
	public CCN_application(CCN_application a) {
		super(a);
		
		this.lastPing = a.getLastPing();
		this.interval = a.getInterval();
		this.passive = a.isPassive();
		this.destMax = a.getDestMax();
		this.destMin = a.getDestMin();
		this.seed = a.getSeed();
		this.contentSize = a.getContentSize();
		this.interestSize = a.getInterestSize();
		this.queryResponseSize = a.getQueryResponseSize();
		//this.rng = new Random(this.seed);

		//this.oppo_cache = a.oppo_cache;
		this.capacity_of_cache = a.getCapacityOfCache();
		this.seed_for_create_cache = a.getSeedForCreateCache();
		this.cache_value_range_Min = a.getCacheValueRangeMin();
		this.cache_value_range_Max = a.getCacheValueRangeMax();
		this.max_carried_content = a.getMaxCarriedContent();
		
		this.seed_for_query = a.getSeedForQuery();
		//this.rng_query = a.rng_query;
		this.query_range_Min = a.getQueryRangeMin();
		this.query_range_Max = a.getQueryRangeMax();
		this.mode = a.getMode();
		//this.query_record = a.query_record;
		this.query_record = new HashMap<Integer, Integer>();
		
		//this.static_cache = a.static_cache;
		this.static_cache_value_Min = a.static_cache_value_Min;
		this.static_cache_value_Max = a.static_cache_value_Max;
		this.capacity_of_static_cache = a.capacity_of_static_cache;
		this.seed_for_create_static_cache = a.seed_for_create_static_cache;
		this.rng_query_selection = a.getRngQuerySelection();
		this.seed_query_result = a.getSeedQueryResult();
		
		this.true_static_cache = a.getTrueStaticCache();
		this.true_oppo_cache = a.getTrueOppoCache();
		this.enablePIT = a.getEnablePIT();
		this.pitCapacity = a.pitCapacity;
		this.pitEntryLifetime = a.pitEntryLifetime;
		this.pitInsertTime = new HashMap<Integer, Double>();
		this.oppoPoisonMarker = new HashMap<Integer, Boolean>();
		this.staticPoisonMarker = new HashMap<Integer, Boolean>();
		this.cacheAdmissionRequiresPIT = a.cacheAdmissionRequiresPIT;
		this.securityEnabled = a.securityEnabled;
		this.securityPolicy = a.securityPolicy;
		this.securityKeySeed = a.securityKeySeed;
		this.securityFreshnessWindow = a.securityFreshnessWindow;
		this.securityReplayCheckEnabled = a.securityReplayCheckEnabled;
		this.securityKey = a.securityKey;
		this.replayState = new HashMap<String, Double>();
		
		this.max_hold_query = a.getMaxHoldQuery();
		this.num_of_msg_to_generate = a.getNumOfMsgToGenerate();
		
		
		//contentLadder should be the same for the consumer in order to create Zipf distribution of query key
		this.contentLadder = a.getContentLadder();
		this.popularContent = a.getPopularContent();
		this.queryDistribution = a.getQueryDistribution();
		this.numOfPopuCont = a.getNumOfPopuCont();
	}
	/** 
	 * @param host	host for which PIT will be printed
	 */

	void print_PIT(DTNHost Host){
		System.out.print(Host+ "# ");
		for(Integer query_key : this.PIT.keySet()){
			System.out.print(query_key + ":" + this.PIT.get(query_key).getHostToSendList() + ", ");
		}
		System.out.println();
	}
	
	
	/** 
	 * Handles an incoming message. If the message is a ping message replies
	 * with a pong message. Generates events for ping and pong messages.
	 * 
	 * @param msg	message received by the router
	 * @param host	host to which the application instance is attached
	 */
	@Override
	public Message handle(Message msg, DTNHost host) {
		String type = (String)msg.getProperty("type");

		System.out.println("in message handle : " + host);

		if(type == null) return msg; //Not a valid msg
		
		if(this.ini_cache_again == true || static_cache == null){
			this.ini_cache_again = false;
			Ini_static_cache(host);
		}
		
		if(this.oppo_cache == null)
			Ini_oppo_cache();
		
		if(msg.getFrom() == host)
			return msg;
	
		//if(this.checkProcedMsgList(msg) && true){//disable dropping duplicated packages
		if(this.checkProcessedMsgList(msg)){
			//drop msg: according to routing/MessageRouter.java in line no.359
			print(host + ": drop duplicated [" + type + "] msg =[" + msg.getProperty("queryMsg") +"] id =[" + msg.toString() + "] from " + msg.getFrom() + " to " + msg.getTo() + " visited hosts:" + " total:" + msg.getHopCount() + msg.getHops());
			super.sendEventToListeners("forwardingStopNonce", null, host);
			return null;
		}
		
		
	
		
		//test code
	//	System.out.println(host + ": got a [" + type + "] msg = [" + msg.getProperty("queryMsg") + "] id = [" + msg.toString() + "] from " + msg.getFrom() + " to " + msg.getTo() + " visited hosts:" + " total:" + msg.getHopCount() + msg.getHops() + " hostsTo= " + getHostsTo(msg));
		

		print_oppo_cache(host);

		/** Ground-truth only, for CachePoisoningReporter instrumentation below.
		 * "isPoison" is a property CachePoisoningApplication adds to its own
		 * forged Data messages; no code anywhere reads it to change caching,
		 * forwarding, or acceptance behavior - only which counters/events fire. */
		boolean isForgedData = (type.equalsIgnoreCase("queryResponse") && msg.getProperty("isPoison") != null);

		/** BPSec-inspired integrity verification, computed once per received
		 * Data message so both the consumer-acceptance block and the
		 * cache-admission block below can use the same outcome. Only runs
		 * at all when securityEnabled - when disabled this stays NA and
		 * every downstream check that consults it is a no-op, preserving
		 * existing behavior exactly. */
		int securityOutcome = SECURITY_OUTCOME_NA;
		if(this.securityEnabled && type.equalsIgnoreCase("queryResponse")){
			String securitySource = (String)msg.getProperty("securitySource");
			String creationTimeStr = (String)msg.getProperty("creationTime");
			String integrityTag = (String)msg.getProperty("integrityTag");
			String secQueryMsg = (String)msg.getProperty("queryMsg");
			String secContent = (String)msg.getProperty("content");

			if(integrityTag == null || securitySource == null || creationTimeStr == null){
				securityOutcome = SECURITY_OUTCOME_MISSING;
				super.sendEventToListeners("IntegrityMissing", secQueryMsg, host);
			}
			else{
				String canonical = CCNSecurity.canonicalString(type, secQueryMsg, secContent, securitySource, creationTimeStr);
				boolean tagOk = CCNSecurity.verifyHex(this.securityKey, canonical, integrityTag);
				if(!tagOk){
					securityOutcome = SECURITY_OUTCOME_FAILED;
					super.sendEventToListeners("IntegrityFailed", secQueryMsg, host);
				}
				else{
					double msgCreationTime = Double.parseDouble(creationTimeStr);
					double nowTime = SimClock.getTime();
					String replayKey = securitySource + ":" + secQueryMsg;
					Double lastSeen = this.replayState.get(replayKey);

					boolean isReplay = false;
					if(this.securityReplayCheckEnabled){
						boolean tooStale = (this.securityFreshnessWindow > 0) &&
								((nowTime - msgCreationTime) > this.securityFreshnessWindow);
						isReplay = tooStale || (lastSeen != null && msgCreationTime <= lastSeen);
					}
					/* When securityReplayCheckEnabled is false, isReplay stays
					 * false unconditionally - this is "integrity only": a
					 * validly-tagged packet is always accepted regardless of
					 * timestamp or repetition, isolating exactly what HMAC
					 * verification alone does and does not catch. */

					if(isReplay){
						securityOutcome = SECURITY_OUTCOME_REPLAY;
						super.sendEventToListeners("ReplayDetected", secQueryMsg, host);
						if(this.securityPolicy == SECURITY_POLICY_ACCEPT){
							super.sendEventToListeners("ReplayAccepted", secQueryMsg, host);
						}
						else{
							super.sendEventToListeners("ReplayRejected", secQueryMsg, host);
						}
					}
					else{
						securityOutcome = SECURITY_OUTCOME_VALID;
						this.replayState.put(replayKey, msgCreationTime);
						super.sendEventToListeners("IntegrityValid", secQueryMsg, host);
					}
				}
			}
		}

		/** Single gate used below: does current policy allow trusting this
		 * Data (as a consumer's answer, and for caching)? Compatibility
		 * mode (policy==accept, the default whenever security is enabled)
		 * always allows it regardless of outcome, so turning security on
		 * to collect metrics does not, by itself, start rejecting traffic. */
		boolean securityAllowsTrust = (!this.securityEnabled)
				|| (this.securityPolicy == SECURITY_POLICY_ACCEPT)
				|| (securityOutcome == SECURITY_OUTCOME_VALID);

		/** parse response whose destination host is current host */
		if (type.equalsIgnoreCase("queryResponse") ){
			String query_key_of_response = (String)msg.getProperty("queryMsg");

			if( msg.getProperty("content") == null){
				//System.out.println(host + ": got an invalid response msg");
			}
			else if(this.mode == 1){
				int response_key_in_int = Integer.parseInt(query_key_of_response);
				if(this.query_record == null){
					query_record = new HashMap<Integer, Integer>();
				}
				if(this.query_record.get( response_key_in_int) != null && this.query_record.get( response_key_in_int ) != 1){
					if(securityAllowsTrust){
						super.sendEventToListeners("OriginalGotResponse", query_key_of_response, host);
//						this.query_record.put(response_key_in_int, 1);
						this.query_record.remove(response_key_in_int);
						/** Ground truth only: the accept/remove logic above ran
						 * identically either way - this only measures how often
						 * a real consumer's own pending query gets satisfied by
						 * forged content instead of the legitimate producer. */
						if(isForgedData){
							super.sendEventToListeners("OriginalGotPoisonedResponse", query_key_of_response, host);
						}
					}
					else{
						/** Policy rejected/quarantined this Data: the pending
						 * query is left intact (not removed), so a later
						 * genuinely valid response can still satisfy it. */
						super.sendEventToListeners("ConsumerAcceptanceRejected", query_key_of_response, host);
					}
				}			
			}
			//update hostsTo property
			ArrayList<String> hostsTo = new ArrayList<String>(getHostsTo(msg));
			boolean flag_for_update = false;
			
			if(hostsTo.contains(host.toString())){				
				hostsTo.remove(host.toString());  ///I am one of the receiver
				flag_for_update = true;
			}
			
			
			if(msg.getTo() == host){
				if(hostsTo.size() > 1){
					////there are more hosts to send(just pick the first one)
					msg.setTo(getHostByName(hostsTo.get(0)));
					hostsTo.remove(0);
					String hostsToString = "";
					for(String hostName: hostsTo){
						hostsToString += hostName + "+";  //in a form of host1+host2+host3+.....
					}
					
					if(hostsTo.size() > 0){
						hostsToString = hostsToString.substring(0, hostsToString.length() - 1);
					}
					
					msg.updateProperty("hostsTo", hostsToString);
					host.createNewMessage(msg);
				}
				else{
					//test code
					print(host + ":I am the last one");
				}
			}
			else{
				//no, renew this property (PIT table check later);
				if(flag_for_update == true){
					String hostsToString = "";
					for(String hostName: hostsTo){
						hostsToString += hostName + "+";  //in the form of host1+host2+host3+.....
					}
					
					if(hostsTo.size() > 0){
						hostsToString = hostsToString.substring(0, hostsToString.length() - 1);
					}
					
					msg.updateProperty("hostsTo", hostsToString);
					super.sendEventToListeners("forwardingStopListPIT", null, host);
				}
			}
		}
		/** After get a response, add to oppo cache */
		if(type.equalsIgnoreCase("queryResponse") && ((String)msg.getProperty("content")).isEmpty() == false && msg.getFrom() != host){
			//It is time to check the PIT table
			int request_in_int = Integer.parseInt( (String)msg.getProperty("queryMsg") );

			if(isForgedData){
				super.sendEventToListeners("ForgedDataReceived", (String)msg.getProperty("queryMsg"), host);
			}

			HostToSend host_to_send = this.PIT.get(request_in_int);
			
			if(host_to_send != null && this.enablePIT){
				///there is a record for this reponse
				List<DTNHost> hostsToFromPIT = host_to_send.getHostToSendList();
				
				
				/*only send once*/
				msg.setTo(hostsToFromPIT.get(0));
				String hostsToString = "";
				hostsToFromPIT.remove(0);
				for(DTNHost h:hostsToFromPIT){
					hostsToString += h.toString() + "+";
				}
				
				//combine the hostsTo from PIT table and hosts list in hostsTo property in msg
				ArrayList<String> hostsToFromMsg = new ArrayList<String>(getHostsTo(msg));
				for(String h: hostsToFromMsg){
					hostsToString += h + "+";
				}
				
				if(hostsToFromPIT.size() > 0 || hostsToFromMsg.size() > 0){
					hostsToString = hostsToString.substring(0, hostsToString.length() - 1);
				}
				
				msg.updateProperty("hostsTo", hostsToString);			
				///remove the record in the PIT table
				this.PIT.remove(request_in_int);
				this.pitInsertTime.remove(request_in_int);
				super.sendEventToListeners("PITOccupancy", Integer.toString(this.PIT.size()), host);
			}
			
			/** Experiment E defense: if enabled, only admit Data into the
			 * cache when this host actually has a pending PIT entry for it
			 * (reusing host_to_send, already looked up above - no extra
			 * lookup). Default false means this block always runs, i.e.
			 * the fork's original unconditional-caching behavior. */
			boolean admitToCache = ((!this.cacheAdmissionRequiresPIT) || (host_to_send != null))
					&& securityAllowsTrust;

			Integer evictedOppoKey = null;
			if(admitToCache){
				//add the response to the oppo cache
				evictedOppoKey = oppo_cache.set( request_in_int, (String)msg.getProperty("content") );
			}
			else{
				super.sendEventToListeners("CacheAdmissionRejected", (String)msg.getProperty("queryMsg"), host);
			}

			/** Everything below is ground-truth bookkeeping for
			 * CachePoisoningReporter. The set() call above already ran,
			 * identically, whether or not this was forged content - nothing
			 * here changes what got cached or who it displaced. */
			if(admitToCache){
			this.oppoPoisonMarker.put(request_in_int, isForgedData);
			if(isForgedData){
				super.sendEventToListeners("ForgedDataCached", (String)msg.getProperty("queryMsg"), host);
			}
			if(evictedOppoKey != null){
				Boolean evictedWasPoison = this.oppoPoisonMarker.remove(evictedOppoKey);
				if(evictedWasPoison != null && evictedWasPoison){
					super.sendEventToListeners("PoisonedEntryEvicted", evictedOppoKey.toString(), host);
				}
				else{
					super.sendEventToListeners("ValidEntryEvicted", evictedOppoKey.toString(), host);
				}
			}
			int currentPoisonedCount = 0;
			for(Boolean v : this.oppoPoisonMarker.values()){
				if(v != null && v) currentPoisonedCount++;
			}
			super.sendEventToListeners("CacheOccupancy",
					oppo_cache.get_len() + "," + this.getCacheCapacity() + "," + currentPoisonedCount, host);
			
			//if I am an intermedia node, i should add the response to my static cache also
			if(this.mode == 3 && true_static_cache == 1){
				if( static_cache.get(request_in_int) == null){
					///I don't have it in my static cache. Adding it to my static cache
					static_cache.put( request_in_int, (String)msg.getProperty("content") );
					this.staticPoisonMarker.put(request_in_int, isForgedData);
					if(isForgedData){
						super.sendEventToListeners("ForgedDataCachedStatic", (String)msg.getProperty("queryMsg"), host);
					}
				}
			}
			} // end if(admitToCache)
		}
		
		/** current host is the destination host, check both the static and oppo cache */
		if(type.equalsIgnoreCase("query_ad") && msg.getFrom() != host){
	
			String query_key = (String)msg.getProperty("queryMsg");
			int request_in_int = Integer.parseInt( query_key );
			
			String retrieved_value_from_oppo = "";
			if(true_oppo_cache >= 1){
				retrieved_value_from_oppo = oppo_cache.get(request_in_int);
				if(retrieved_value_from_oppo.isEmpty()){
					super.sendEventToListeners("oppoCacheMiss", null, host);
				}
				else{
					super.sendEventToListeners("oppoCacheHit", null, host);
					Boolean oppoHitIsPoison = this.oppoPoisonMarker.get(request_in_int);
					if(oppoHitIsPoison != null && oppoHitIsPoison){
						super.sendEventToListeners("PoisonedCacheHit", query_key, host);
					}
					else{
						super.sendEventToListeners("LegitCacheHit", query_key, host);
					}
					
					//test code
				//	System.out.println(host + ":[oppo hit] for [" + query_key +  "] visited hosts: " + msg.getHops());
				}
			}
			
			String retrieved_value_from_static = "";
			if(true_static_cache >= 1){
				if(static_cache.get(request_in_int) == null){
					super.sendEventToListeners("staticCacheMiss", null, host);
				}else{
					retrieved_value_from_static = static_cache.get(request_in_int);
					super.sendEventToListeners("staticCacheHit", null, host);
					Boolean staticHitIsPoison = this.staticPoisonMarker.get(request_in_int);
					if(staticHitIsPoison != null && staticHitIsPoison){
						super.sendEventToListeners("PoisonedCacheHit", query_key, host);
					}
					else{
						super.sendEventToListeners("LegitCacheHit", query_key, host);
					}
					
					//test code
					System.out.println(host + ":[static hit] for [" + query_key +  "] visited hosts: " + msg.getHops());
				}
			}
			
			if(msg.getTo() == host){
				boolean flag_send = true;
				Message response_msg = new Message(host, msg.getFrom(), "queryResponse" + SimClock.getIntTime() + "-" + host.getAddress(), getInterestSize()*3); //the size of response msg is 3 times of the query msg
				if(retrieved_value_from_static.isEmpty() == false ){
					response_msg.addProperty("content", retrieved_value_from_static);
				}
				else if(retrieved_value_from_oppo.isEmpty() == false){
					response_msg.addProperty("content", retrieved_value_from_oppo);
				}
				else{
					//msg.addProperty("content", "");
					flag_send = false;
					DTNHost host_to_send; //I don't have the content. So i will sen it to another random host
					do{
						host_to_send = randomHost(host);
					}while(host_to_send == host || host_to_send == msg.getFrom());
					
					msg.setTo(host_to_send);
					host.createNewMessage(msg);
					
					//test code
				//	System.out.println(host + ": I don't have the content of [" + query_key + "]. I will retransmit it to " + host_to_send + ". It visits " + msg.getHops());
				}
				if(flag_send){
					response_msg.updateProperty("type", "queryResponse");
					response_msg.addProperty("queryMsg", (String)msg.getProperty("queryMsg"));

					/** BPSec-inspired integrity: only adds properties/computes
					 * anything when securityEnabled - when disabled this is a
					 * complete no-op, preserving existing behavior exactly. */
					if(this.securityEnabled){
						String secSource = host.toString();
						String secCreationTime = Double.toString(SimClock.getTime());
						String secCanonical = CCNSecurity.canonicalString("queryResponse",
								(String)msg.getProperty("queryMsg"), (String)response_msg.getProperty("content"),
								secSource, secCreationTime);
						String secTag = CCNSecurity.hmacHex(this.securityKey, secCanonical);
						response_msg.addProperty("securitySource", secSource);
						response_msg.addProperty("creationTime", secCreationTime);
						response_msg.addProperty("integrityTag", secTag);
					}

					response_msg.setAppID(APP_ID);
					host.createNewMessage(response_msg);
					super.sendEventToListeners("SentData", (String)msg.getProperty("queryMsg"), host);
				
				//	print(host + ": drop [" + type + "] msg =[" + msg.getProperty("queryMsg") +"] id =[" + msg.toString() + "] from " + msg.getFrom() + " to " + msg.getTo() + " visited hosts:" + " total:" + msg.getHopCount() + msg.getHops());
					//drop current msg
					return null;
				}
			}else{
				//intermedia node
				boolean flag_to_send = false;
				Message response_msg = new Message(host, msg.getFrom(), "queryResponse" + SimClock.getIntTime() + "-" + host.getAddress(), getInterestSize()*3);
				
				//test code
				print(host + ":lol:" + this.enablePIT);
				
				if(retrieved_value_from_static.isEmpty() && retrieved_value_from_oppo.isEmpty() && this.enablePIT){
					HostToSend temp_hostToSend = PIT.get(request_in_int);
					if(temp_hostToSend == null){
						if(this.pitCapacity > 0 && this.PIT.size() >= this.pitCapacity){
							/** PIT is full: reject the new Interest using the same
							 * drop convention used for a duplicate/looped Interest. */
							super.sendEventToListeners("PITFull", null, host);
							return null;
						}
						///this msg has no record in PIT yet, let's create one for it
						temp_hostToSend = new HostToSend();
						temp_hostToSend.addHost(msg.getFrom());
						this.PIT.put(request_in_int, temp_hostToSend);
						this.pitInsertTime.put(request_in_int, SimClock.getTime());
						super.sendEventToListeners("PITOccupancy", Integer.toString(this.PIT.size()), host);
						
						//test code
						System.out.println(host + ": add new entry, PIT size:" + PIT.size());
						print_PIT(host);
					}
					else{
						///currently there is a record for it, we just add to it
						temp_hostToSend.addHost(msg.getFrom());						
						this.PIT.remove(request_in_int);  //update PIT table
						this.PIT.put(request_in_int, temp_hostToSend);
						
						//test code
						System.out.println(host + ": update entry, PIT size:" + PIT.size());
				//		print_PIT(host);
						super.sendEventToListeners("forwardingStopListPIT", null, host);
						return null;
					}
				}
				else if(retrieved_value_from_static.isEmpty() == false){
					response_msg.addProperty("content", retrieved_value_from_static);
					flag_to_send = true;
				}
				else if(retrieved_value_from_oppo.isEmpty() == false){
					response_msg.addProperty("content", retrieved_value_from_oppo);
					flag_to_send = true;
				}
				
				if(flag_to_send == true){
					response_msg.updateProperty("type", "queryResponse");
					response_msg.addProperty("queryMsg", (String)msg.getProperty("queryMsg"));

					if(this.securityEnabled){
						String secSource = host.toString();
						String secCreationTime = Double.toString(SimClock.getTime());
						String secCanonical = CCNSecurity.canonicalString("queryResponse",
								(String)msg.getProperty("queryMsg"), (String)response_msg.getProperty("content"),
								secSource, secCreationTime);
						String secTag = CCNSecurity.hmacHex(this.securityKey, secCanonical);
						response_msg.addProperty("securitySource", secSource);
						response_msg.addProperty("creationTime", secCreationTime);
						response_msg.addProperty("integrityTag", secTag);
					}

					response_msg.setAppID(APP_ID);
					host.createNewMessage(response_msg);
					super.sendEventToListeners("SentData", (String)msg.getProperty("queryMsg"), host);
					
				//	print(host + ": drop [" + type + "] msg =[" + msg.getProperty("queryMsg") +"] id =[" + msg.toString() + "] from " + msg.getFrom() + " to " + msg.getTo() + " visited hosts:" + " total:" + msg.getHopCount() + msg.getHops());
					//drop current msg
					return null;
				}				
			}			
		}
	  
	  return msg;
	}

	/** 
	 * Draws a random host from the destination range
	 * 
	 * @return host
	 */
	private DTNHost randomHost(DTNHost host) {
		if(this.rng == null){
			this.rng = new Random( this.seed + 2 * Integer.parseInt(host.toString().substring(1)) );
			//test code;
			//System.out.println(host + ": rng empty ");
		}
		
		World w = SimScenario.getInstance().getWorld();
		int destaddr =  -1;
		do{
			destaddr = this.rng.nextInt(w.getHosts().size());			
			//test code;
			//System.out.println(host + ": generating destaddr = " + destaddr);
		}while(w.getNodeByAddress(destaddr) == host);
		
		return w.getNodeByAddress(destaddr);
	}
	
	@Override
	public Application replicate() {
		return new CCN_application(this);
	}

	
	public DTNHost getHostByName(String hostName){
		World w = SimScenario.getInstance().getWorld();
		return w.getNodeByAddress(Integer.parseInt(hostName.substring(1)));
	}
	
	public List<String> getHostsTo(Message msg){
		List<String> hostsTo = null;
		if(msg.getProperty("hostsTo") != null && !((String)msg.getProperty("hostsTo")).isEmpty()){
			String hostsToString = (String)msg.getProperty("hostsTo");
		//	print("hostsToString:" + hostsToString);
			
			if(hostsToString.indexOf("+") != -1){
				hostsTo = new ArrayList<String>(Arrays.asList(((String)msg.getProperty("hostsTo")).split("\\+")));
			}
			else{
				hostsTo = new ArrayList<String>();
				hostsTo.add(hostsToString);
			}
		}
		if(hostsTo == null){
			hostsTo = new ArrayList<String>();
		}
		if(!hostsTo.contains(msg.getTo().toString()))
			hostsTo.add(msg.getTo().toString());
		
		//test code
		//print("getHostsTo:" + hostsTo);
		
		return hostsTo;
	}
	
	public boolean checkToes(DTNHost host, Message msg){
		if(host == msg.getTo())
			return true;
		if(getHostsTo(msg).contains(host.toString()))
			return true;
		return false;
	}
	
	/** 
	 * @param host to which the application instance is attached
	 */
	@Override
	public void update(DTNHost host) {
		/* PIT expiry sweep: runs for every mode (Producer/Consumer/Intermedia
		 * all hold their own PIT), independent of the mode==1 (consumer)
		 * generation logic below. No-ops entirely (zero behavior change)
		 * unless CCN_application.interestLifetime is explicitly configured. */
		if(this.enablePIT && this.pitEntryLifetime > 0 && !this.PIT.isEmpty()){
			double now = SimClock.getTime();
			List<Integer> expiredKeys = new ArrayList<Integer>();
			for(Integer key : this.pitInsertTime.keySet()){
				Double insertedAt = this.pitInsertTime.get(key);
				if(insertedAt != null && (now - insertedAt) >= this.pitEntryLifetime){
					expiredKeys.add(key);
				}
			}
			for(Integer key : expiredKeys){
				this.PIT.remove(key);
				this.pitInsertTime.remove(key);
				super.sendEventToListeners("PITTimeout", null, host);
				super.sendEventToListeners("PITOccupancy", Integer.toString(this.PIT.size()), host);
			}
		}

		if(this.mode == 1){
			/** only consumers generate msg */
			double curTime = SimClock.getTime();
			if (curTime - this.lastPing >= this.interval) {
				if( curGenerated < num_of_msg_to_generate){
					curGenerated ++;
					
					//test code: print contentLadder
				/*	print(host + ": Start of contentladder");
					for(int i = 0; i<zipfDistRang; i++)
					{
						print(contentLadder[i] + " ");
					}
					print(host + ": End of contentladder");*/
					
					if(this.static_cache == null){
						Ini_static_cache(host);
					}
					
					if(this.oppo_cache == null)
						Ini_oppo_cache();
					
					if(rng_query == null && queryDistribution == 1){
						//accept the num in the name of host to act as an parameter to generate query
						int seed_from_host = 3*Integer.parseInt(host.toString().substring(1)) + 1;
						
						//rng_query = new Random(seed_for_query + seed_from_host);
						rng_query = new Random(seed_for_query); /// generating the same query for all nodes
						
						//Sleep for 0.3 seconds
						try {
						    Thread.sleep(300);
						} catch (InterruptedException e) {
						    e.printStackTrace();
						}
					}
						
					//check does current host itself already has the resource or not
//					int try_time = 0;
					int query_key = -1;
						
						if(this.queryDistribution == 2){
								query_key = getQueryKeyWithZipfDistFeature(host);
						}
						else{
							query_key = query_range_Min + rng_query.nextInt(query_range_Max - query_range_Min);
						}
						
						
						//test code
					//	print(host + ": query type: " + this.queryDistribution + " temp_query_key=" + query_key + " try_time=" + try_time);
						
						
						boolean flag_for_reg = false;
						if(static_cache != null){
							if( static_cache.get(query_key) != null && !static_cache.get(query_key).isEmpty() ){
								flag_for_reg = true;
								//System.out.println("static true:" + static_cache.get(query_key));
							}
						}
						
						if(oppo_cache != null){
							if( oppo_cache.get(query_key) != null && !oppo_cache.get(query_key).isEmpty() ){
								flag_for_reg = true;
								//System.out.println("oppo true:" + oppo_cache.get(query_key));
							}
						}
						
						if( this.query_record != null ){
							if( this.query_record.get(query_key) != null )
								flag_for_reg = true;
						}
					
					if(!flag_for_reg){
						/** send msg to a random host */
						DTNHost random_host = randomHost(host);
						Message m = new Message(host, random_host, "query_ad" +
										SimClock.getIntTime() + "-" + host.getAddress(),
										getInterestSize());		
											
						m.addProperty("type", "query_ad");
						m.addProperty("queryMsg", new Integer(query_key).toString());
						m.setAppID(APP_ID);
						host.createNewMessage(m);
								
						super.sendEventToListeners("SentQuery", (String)m.getProperty("queryMsg"), host);
						this.query_record.put(query_key, 0);
					//	System.out.println(host + " query for [" + (String)m.getProperty("queryMsg") + "] to random host:" + random_host  + " query record " + this.query_record);
					}
					else{
						super.sendEventToListeners("SentDuplicatedQuery", null, host);
					}

					this.lastPing = curTime;
				}
			}
		}
	}
	

	/**
	 * @return the lastPing
	 */
	public double getLastPing() {
		return lastPing;
	}

	/**
	 * @param lastPing the lastPing to set
	 */
	public void setLastPing(double lastPing) {
		this.lastPing = lastPing;
	}

	/**
	 * @return the interval
	 */
	public double getInterval() {
		return interval;
	}

	/**
	 * @param interval the interval to set
	 */
	public void setInterval(double interval) {
		this.interval = interval;
	}

	/**
	 * @return the passive
	 */
	public boolean isPassive() {
		return passive;
	}

	/**
	 * @param passive the passive to set
	 */
	public void setPassive(boolean passive) {
		this.passive = passive;
	}

	/**
	 * @return the destMin
	 */
	public int getDestMin() {
		return destMin;
	}

	/**
	 * @param destMin the destMin to set
	 */
	public void setDestMin(int destMin) {
		this.destMin = destMin;
	}

	/**
	 * @return the destMax
	 */
	public int getDestMax() {
		return destMax;
	}

	/**
	 * @param destMax the destMax to set
	 */
	public void setDestMax(int destMax) {
		this.destMax = destMax;
	}

	/**
	 * @return the seed
	 */
	public int getSeed() {
		return seed;
	}

	/**
	 * @param seed the seed to set
	 */
	public void setSeed(int seed) {
		this.seed = seed;
	}

	/**
	 * @return the contentSize
	 */
	public int getContentSize() {
		return contentSize;
	}

	/**
	 * @param contentSize the contentSize to set
	 */
	public void setContentSize(int contentSize) {
		this.contentSize = contentSize;
	}

	/**
	 * @return the interestSize
	 */
	public int getInterestSize() {
		return interestSize;
	}

	/**
	 * @param interestSize the interestSize to set
	 */
	public void setInterestSize(int interestSize) {
		this.interestSize = interestSize;
	}

	/**
	* @return the cache capacity
	*/
	public int getCacheCapacity(){
		return this.capacity_of_cache;
	}
	
	public int getQueryResponseSize(){
		return this.queryResponseSize;
	}
	
	public HashMap<Integer, HostToSend> getPIT(){
		return this.PIT;
	}
	
	public int getCapacityOfCache(){
		return this.capacity_of_cache;
	}
	
	public int getSeedForCreateCache(){
		return this.seed_for_create_cache;
	}
	
	public int getCacheValueRangeMin(){
		return this.cache_value_range_Min;
	}
	
	public int getCacheValueRangeMax(){
		return this.cache_value_range_Max;
	}
	
	public int getMaxCarriedContent(){
		return this.max_carried_content;
	}
	
	public int getSeedForQuery(){
		return this.seed_for_query;
	}
	
	public int getQueryRangeMin(){
		return this.query_range_Min;
	}
	
	public int getQueryRangeMax(){
		return this.query_range_Max;
	}
	
	public int getMode(){
		return this.mode;
	}
	
	public Random getRngQuerySelection(){
		return this.rng_query_selection;
	}
	
	public int getSeedQueryResult(){
		return this.seed_query_result;
	}
	
	public int getTrueStaticCache(){
		return this.true_static_cache;
	}
	
	public int getTrueOppoCache(){
		return this.true_oppo_cache;
	}
	
	public int getMaxHoldQuery(){
		return this.max_hold_query;
	}
	
	public int getNumOfMsgToGenerate(){
		return this.num_of_msg_to_generate;
	}
	
	public void print(Object content){
		System.out.println(content);
	}
	
	public boolean checkProcessedMsgList(Message msg){
	
		if(!procedMsgList.contains(msg.toString())){
			if(procedMsgList.size() >= cap_of_list){
				//remove the first one
				procedMsgList.remove(0);
			}
			procedMsgList.add(msg.toString());
			return false;
		}
		else{
			return true;
		}
	}
	
	
	public int[] getContentLadder(){
		return this.contentLadder;
	}
	
	public int[] getPopularContent(){
		return this.popularContent;
	}
	
	public int getQueryDistribution(){
		return this.queryDistribution;
	}
	
	public int getNumOfPopuCont(){
		return this.numOfPopuCont;
	}
	
	public boolean getEnablePIT(){
		return this.enablePIT;
	}
	
	public void printIntArray(int[] array, int size){
		for(int i=0; i<size ; i++){
			print(array[i] + " ");
		}
		print("\n");
	}
	
	public void createPopularContent(){
		if(numOfPopuCont <= 0){
		//	System.out.println("numOfPopuCont not set yet.");
			return;
		}
		
	//	System.out.println("Generating popular content list");
		
		popularContent = new int[numOfPopuCont];
		Random content_generator = new Random(seedForCreatePopuContent);
		
		for(int i=0; i < numOfPopuCont; i++){
			popularContent[i] = content_generator.nextInt(query_range_Max - query_range_Min) + query_range_Min;			
		//	System.out.print(popularContent[i] + " ");
		}
		System.out.println("\n Done Generating popular content list");
	}
	
	
	
	//content ladder
	public void createContentLadder(){
		int total = 0;
		double topValue = zipfDistRang * topRatio;
		for(int i = 0; i < zipfDistRang; i++){
			if(total < zipfDistRang){
				total += Math.ceil(topValue/Math.pow(i+1, paraS));
				contentLadder[i] = total;
			}
			else{
				this.numOfPopuCont = i;
				
				//set the rest of contentLadder array to a value bigger than zipfDistRang
				for(int j=i+1; j < zipfDistRang; j++){
					contentLadder[j] = total + 1;
				}
				
				break;
			}
		}
		
	}
	
	public int getRankInContentLadder(int value){
		for(int i=0; i < numOfPopuCont + 1; i++){
			if(value < this.contentLadder[i])
				return i;
		}
		
		// $value is not popular content
		return -1;
	}
	
	
	public int getQueryKeyWithZipfDistFeature(DTNHost host){
	
		int contentRank;
		
		if(zipfDistRandGeneSeed == null){
			this.zipfDistRandGeneSeed = new Random( Integer.parseInt(host.toString().substring(1)) ); // using the num character in the name of host as a random seed
			//print(host + ":zipfDistRandGeneSeed");
		}
		
		do{
			int random_num =  this.zipfDistRandGeneSeed.nextInt(zipfDistRang);
			contentRank = getRankInContentLadder(random_num);
		//	print(host + ":" + random_num + " rank:" + contentRank + " content:" + popularContent[contentRank]  + " num of popular content:" + numOfPopuCont);
		}while(contentRank < 0);
		
		if(contentRank > numOfPopuCont){
		//	System.out.println("Looking for unpopular content");
		}
		return popularContent[contentRank];
	}
}

