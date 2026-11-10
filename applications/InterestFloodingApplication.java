/*
 * Copyright 2010 Aalto University, DCS Lab
 * Released under GPLv3. See LICENSE.txt for details.
 * Interest Flooding Attack (IFA) application for the CCN fork of The ONE Simulator.
 */

package applications;

import java.util.Random;

import core.Application;
import core.DTNHost;
import core.Message;
import core.Settings;
import core.SimClock;
import core.SimScenario;
import core.World;

/**
 * Interest Flooding Attack application.
 *
 * Generates a configurable, rate-limited stream of CCN Interests
 * ("query_ad" messages, exactly as produced by CCN_application mode 1)
 * for content identifiers that are guaranteed to fall outside every
 * configured Producer's static cache range. Because no Producer can ever
 * satisfy them, every Interest this application injects either times out
 * or occupies PIT state on every intermediate CCNRouter/CCN_application
 * node it opportunistically reaches, for as long as that state is held.
 *
 * This class does NOT extend CCN_application and does NOT touch PIT,
 * static cache, or opportunistic cache state directly: it only
 * constructs and injects Interest-shaped Messages using the same
 * type / queryMsg / appID contract that CCN_application.handle() already
 * expects, so the resulting traffic is processed by CCN_application and
 * CCNRouter exactly like a Consumer's normal Interest. No "isAttacker"
 * flag exists anywhere in the forwarding/routing/PIT path; this is a
 * pure traffic-generation exercise on top of the existing CCN logic.
 */
public class InterestFloodingApplication extends Application {

	/** Application ID for this class' own installed-app identity */
	public static final String APP_ID = "fi.dcslab.InterestFloodingApplication";

	/** Start time (simulated seconds) of the attack window */
	public static final String ATTACK_START = "attackStart";
	/** End time (simulated seconds) of the attack window */
	public static final String ATTACK_END = "attackEnd";
	/** Interest generation interval - same setting name as CCN_application's "interval" */
	public static final String INTERVAL = "interval";
	/** Max number of malicious Interests this app instance will generate */
	public static final String NUM_OF_MSG_TO_GENERATE = "numOfMsgToGenerate";
	/** Size (bytes) of each malicious Interest - same setting name as CCN_application's "interestSize" */
	public static final String INTEREST_SIZE = "interestSize";
	/** Numeric range (min,max) used for attack content identifiers */
	public static final String ATTACK_QUERY_RANGE = "attackQueryRange";
	/** Per-attacker/per-run numeric separator, folded into content-ID generation */
	public static final String ATTACK_PREFIX = "attackPrefix";
	/** Starting sequence number for this app instance's generated IDs */
	public static final String START_SEQUENCE = "startSequence";
	/** Seed for this app's random destination-selection generator */
	public static final String SEED = "seed";

	/** numeric space reserved per attacker host inside the attack query range */
	private static final long HOST_SPACING = 100000L;
	/** numeric space reserved per attackPrefix "run" inside the attack query range */
	private static final long PREFIX_SPACING = 10000000L;

	private double		lastPing = 0;
	private double		interval = 500;
	private double		attackStart = 0;
	private double		attackEnd = Double.MAX_VALUE;
	private int			numOfMsgToGenerate = 50;
	private int			interestSize = 1;
	private int			attackQueryMin = 1000000;
	private int			attackQueryMax = 9999999;
	private int			attackPrefix = 0;
	private int			startSequence = 0;
	private int			seed = 0;
	private int			curGenerated = 0;

	private Random		rng = null;

	/**
	 * @param s	Settings to use for initializing the application.
	 */
	public InterestFloodingApplication(Settings s) {
		if (s.contains(ATTACK_START)) {
			this.attackStart = s.getDouble(ATTACK_START);
		}
		if (s.contains(ATTACK_END)) {
			this.attackEnd = s.getDouble(ATTACK_END);
		}
		if (s.contains(INTERVAL)) {
			this.interval = s.getDouble(INTERVAL);
		}
		if (s.contains(NUM_OF_MSG_TO_GENERATE)) {
			this.numOfMsgToGenerate = s.getInt(NUM_OF_MSG_TO_GENERATE);
		}
		if (s.contains(INTEREST_SIZE)) {
			this.interestSize = s.getInt(INTEREST_SIZE);
		}
		if (s.contains(ATTACK_QUERY_RANGE)) {
			int[] range = s.getCsvInts(ATTACK_QUERY_RANGE, 2);
			this.attackQueryMin = range[0];
			this.attackQueryMax = range[1];
		}
		if (s.contains(ATTACK_PREFIX)) {
			this.attackPrefix = s.getInt(ATTACK_PREFIX);
		}
		if (s.contains(START_SEQUENCE)) {
			this.startSequence = s.getInt(START_SEQUENCE);
		}
		if (s.contains(SEED)) {
			this.seed = s.getInt(SEED);
		}

		super.setAppID(APP_ID);
	}

	/**
	 * Copy constructor.
	 * @param a The application prototype where setting values are copied from
	 */
	public InterestFloodingApplication(InterestFloodingApplication a) {
		super(a);
		this.lastPing = a.lastPing;
		this.interval = a.interval;
		this.attackStart = a.attackStart;
		this.attackEnd = a.attackEnd;
		this.numOfMsgToGenerate = a.numOfMsgToGenerate;
		this.interestSize = a.interestSize;
		this.attackQueryMin = a.attackQueryMin;
		this.attackQueryMax = a.attackQueryMax;
		this.attackPrefix = a.attackPrefix;
		this.startSequence = a.startSequence;
		this.seed = a.seed;

		/* curGenerated and rng are per-instance runtime state, not
		 * configuration: each replicated host must start its own count
		 * and its own random generator, the same way CCN_application
		 * leaves its own rng/curGenerated fields to be lazily
		 * (re)initialized per host rather than copied. */
		this.curGenerated = 0;
		this.rng = null;
	}

	/**
	 * The attacker never needs to react to received messages: it only
	 * injects traffic. Returning the message unchanged means it is not
	 * consumed here and normal router-level forwarding is unaffected by
	 * this application, with no attacker-specific branching of any kind.
	 *
	 * @param msg	message received by the router
	 * @param host	host to which the application instance is attached
	 */
	@Override
	public Message handle(Message msg, DTNHost host) {
		return msg;
	}

	/**
	 * @param host host for which the application instance is attached
	 */
	@Override
	public void update(DTNHost host) {
		double curTime = SimClock.getTime();

		if (curTime < this.attackStart || curTime > this.attackEnd) {
			return; /* outside the configured attack window */
		}

		if (curTime - this.lastPing < this.interval) {
			return; /* not yet time to send the next Interest */
		}

		if (this.curGenerated >= this.numOfMsgToGenerate) {
			return; /* malicious-Interest budget for this host is exhausted */
		}

		if (this.rng == null) {
			this.rng = new Random(this.seed + 2 * Integer.parseInt(host.toString().substring(1)));
		}

		DTNHost destination = randomHost(host);
		int attackQueryKey = nextAttackQueryKey(host);

		Message m = new Message(host, destination,
				"flood_ad" + SimClock.getIntTime() + "-" + host.getAddress() + "-" + this.curGenerated,
				this.interestSize);

		/* Same two properties CCN_application.handle() checks for a real
		 * Interest: "type" = "query_ad" and "queryMsg" = the numeric
		 * content key as a String. Nothing else marks this as an attack
		 * packet, and nothing here calls into CCN_application, PIT, or
		 * cache internals directly. */
		m.addProperty("type", "query_ad");
		m.addProperty("queryMsg", Integer.toString(attackQueryKey));

		/* Stamped with CCN_application's own APP_ID (not this app's ID)
		 * so every receiving host's CCN_application instance is the one
		 * that handles it, exactly as it would a Consumer's Interest. */
		m.setAppID(CCN_application.APP_ID);

		host.createNewMessage(m);

		super.sendEventToListeners("SentFloodInterest", Integer.toString(attackQueryKey), host);

		this.curGenerated++;
		this.lastPing = curTime;
	}

	/**
	 * Builds a numeric content identifier that is unique for this host and
	 * this sequence number, and stays inside [attackQueryMin, attackQueryMax).
	 * Uniqueness across multiple attacker hosts is provided by folding in
	 * each host's own address (DTNHost addresses are unique per node in the
	 * simulation); attackPrefix is provided purely as an additional
	 * configurable separator between independent attack configurations.
	 *
	 * @param host host generating the Interest
	 * @return a numeric content key outside the valid content range
	 */
	private int nextAttackQueryKey(DTNHost host) {
		long rangeSize = (long) this.attackQueryMax - (long) this.attackQueryMin;
		if (rangeSize <= 0) {
			rangeSize = 1;
		}

		long rawOffset = (long) this.attackPrefix * PREFIX_SPACING
				+ (long) host.getAddress() * HOST_SPACING
				+ (long) (this.startSequence + this.curGenerated);

		return this.attackQueryMin + (int) (rawOffset % rangeSize);
	}

	/**
	 * Draws a random destination host, same pattern as
	 * CCN_application's own randomHost(DTNHost): the Interest's initial
	 * "to" field is just a random walk starting point, not a routing
	 * decision, consistent with how this fork's opportunistic content
	 * forwarding already works.
	 *
	 * @param host host generating the Interest
	 * @return a random host other than itself
	 */
	private DTNHost randomHost(DTNHost host) {
		World w = SimScenario.getInstance().getWorld();
		int destAddr = -1;
		do {
			destAddr = this.rng.nextInt(w.getHosts().size());
		} while (w.getNodeByAddress(destAddr) == host);

		return w.getNodeByAddress(destAddr);
	}

	@Override
	public Application replicate() {
		return new InterestFloodingApplication(this);
	}
}
