/*
 * Copyright 2010 Aalto University, DCS Lab
 * Released under GPLv3. See LICENSE.txt for details.
 * Cache (Content) Poisoning Attack application for the CCN fork of The ONE Simulator.
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
 * Cache Poisoning (Content Poisoning) Attack application.
 *
 * Injects forged Data ("queryResponse") messages for VALID, EXISTING
 * numeric content IDs inside the legitimate producer range (default
 * 1-200), carrying a forged payload instead of real content.
 *
 * This is NOT Interest Flooding: every message this application creates
 * already has type="queryResponse" (a Data packet), never type="query_ad".
 * It does not generate any Interests at all.
 *
 * Why no Interest/PIT "race" step is needed in this fork: inspection of
 * CCN_application.handle() shows the unconditional caching line
 *     oppo_cache.set(request_in_int, (String)msg.getProperty("content"));
 * runs for ANY received queryResponse with a non-empty "content" property
 * and msg.getFrom() != host, regardless of whether a matching PIT entry
 * (i.e. a prior real Interest) exists for that content ID. The PIT lookup
 * immediately above it only decides who the Data gets forwarded to next -
 * it is not a gate on whether the Data gets accepted/cached. So a forged
 * Data message injected directly, with no preceding Interest at all, is
 * cached by this fork exactly as readily as a legitimate response is.
 *
 * This class does not extend CCN_application and does not touch
 * oppo_cache/static_cache/PIT internals directly - it only constructs and
 * injects Data-shaped Messages using the same type/queryMsg/content/appID
 * contract CCN_application.handle() already expects, so the resulting
 * traffic is processed by every receiving host's own CCN_application and
 * CCNRouter exactly like a real producer's response. The one extra
 * property it adds, "isPoison", is read only by the ground-truth
 * instrumentation added to CCN_application.java for CachePoisoningReporter
 * - nothing in the caching/forwarding/acceptance logic itself reads it, so
 * it changes no behavior, only what gets counted.
 */
public class CachePoisoningApplication extends Application {

	/** Application ID for this class' own installed-app identity */
	public static final String APP_ID = "fi.dcslab.CachePoisoningApplication";

	/** Start time (simulated seconds) of the attack window */
	public static final String ATTACK_START = "attackStart";
	/** End time (simulated seconds) of the attack window */
	public static final String ATTACK_END = "attackEnd";
	/** Forged-Data generation interval - same setting name as CCN_application's "interval" */
	public static final String INTERVAL = "interval";
	/** Max number of forged Data packets this app instance will generate */
	public static final String NUM_OF_MSG_TO_GENERATE = "numOfMsgToGenerate";
	/** Numeric range (min,max) of EXISTING, valid content IDs to poison */
	public static final String CONTENT_RANGE = "contentRange";
	/** Size (bytes) of the forged Message itself - router/buffer accounting */
	public static final String INTEREST_OR_DATA_SIZE = "interestOrDataSize";
	/** Length (characters) of the forged "content" payload string carried inside the Message */
	public static final String POISON_PAYLOAD_SIZE = "poisonPayloadSize";
	/** Seed for this app's RNG (target-id selection and destination selection) */
	public static final String SEED = "seed";
	/** 0 (default) = omit integrity properties entirely - this attacker has
	 *  no key, so it cannot produce a tag that would verify. Models the
	 *  "missing protection" case (IntegrityMissing on the receiving side).
	 *  1 = include securitySource/creationTime/integrityTag anyway, with a
	 *  tag computed under a DIFFERENT key this attacker made up (it still
	 *  does not have the real CCNSecurity.keySeed). Models an attacker that
	 *  tries to look protected without holding the key (IntegrityFailed on
	 *  the receiving side), distinct from the missing-protection case. */
	public static final String FORGE_INTEGRITY_TAG = "forgeIntegrityTag";
	private static final int FORGE_TAG_OMIT = 0;
	private static final int FORGE_TAG_WRONG = 1;
	/** Seed for the WRONG key this attacker fabricates when forgeIntegrityTag=1 -
	 *  deliberately NOT the real CCNSecurity.keySeed, since the attacker does
	 *  not have the real key. */
	public static final String ATTACKER_KEY_SEED = "attackerKeySeed";
	/** When true, this attacker opportunistically captures any GENUINE
	 *  (isPoison absent) signed queryResponse it relays, and can later
	 *  resend it verbatim via replayInterval - a byte-for-byte replay of
	 *  real, validly-signed content, not forged content. Default false:
	 *  this attacker behaves exactly as before unless explicitly enabled. */
	public static final String REPLAY_CAPTURE = "replayCapture";
	/** Seconds between verbatim resends of the most recently captured
	 *  genuine Data. <=0 (default) disables replaying entirely. */
	public static final String REPLAY_INTERVAL = "replayInterval";
	/** Optional: if set to a valid id inside contentRange, always poison that one id instead of rotating */
	public static final String TARGET_ID = "targetId";
	/** 1 = random (default), 2 = sequential/round-robin - how successive target ids are
	 *  chosen when targetId is not fixed. Int-coded (not a string setting) to match this
	 *  fork's own convention (e.g. CCN_application.mode), since no plain string-valued
	 *  Settings getter is used anywhere in the supplied source. */
	public static final String SELECTION_MODE = "selectionMode";
	private static final int SELECTION_MODE_RANDOM = 1;
	private static final int SELECTION_MODE_SEQUENTIAL = 2;

	private double		lastPing = 0;
	private double		interval = 1.0;
	private double		attackStart = 0;
	private double		attackEnd = Double.MAX_VALUE;
	private int			numOfMsgToGenerate = 1000;
	private int			contentRangeMin = 1;
	private int			contentRangeMax = 200;
	private int			interestOrDataSize = 50;
	private int			poisonPayloadSize = 20;
	private int			seed = 0;
	private int			targetId = -1;			// -1 = rotate instead of a single fixed id
	private int			selectionMode = SELECTION_MODE_RANDOM;
	private int			curGenerated = 0;
	private int			sequentialCursor = -1;	// runtime cursor for selectionMode=="sequential"
	private int			forgeIntegrityTag = FORGE_TAG_OMIT;
	private int			attackerKeySeed = 999999;	// deliberately not a real CCNSecurity.keySeed value
	private boolean		replayCapture = false;
	private double		replayInterval = -1.0;
	private double		lastReplay = 0;
	private int			curReplayed = 0;

	/* Most recently captured GENUINE signed Data, verbatim - only
	 * populated when replayCapture is true. All null until something is
	 * actually captured. */
	private String		capturedQueryMsg = null;
	private String		capturedContent = null;
	private String		capturedSecuritySource = null;
	private String		capturedCreationTime = null;
	private String		capturedIntegrityTag = null;

	private Random		rng = null;

	/**
	 * @param s Settings to use for initializing the application.
	 */
	public CachePoisoningApplication(Settings s) {
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
		if (s.contains(CONTENT_RANGE)) {
			int[] range = s.getCsvInts(CONTENT_RANGE, 2);
			this.contentRangeMin = range[0];
			this.contentRangeMax = range[1];
		}
		if (s.contains(INTEREST_OR_DATA_SIZE)) {
			this.interestOrDataSize = s.getInt(INTEREST_OR_DATA_SIZE);
		}
		if (s.contains(POISON_PAYLOAD_SIZE)) {
			this.poisonPayloadSize = s.getInt(POISON_PAYLOAD_SIZE);
		}
		if (s.contains(SEED)) {
			this.seed = s.getInt(SEED);
		}
		if (s.contains(TARGET_ID)) {
			this.targetId = s.getInt(TARGET_ID);
		}
		if (s.contains(SELECTION_MODE)) {
			this.selectionMode = s.getInt(SELECTION_MODE);
		}
		if (s.contains(FORGE_INTEGRITY_TAG)) {
			this.forgeIntegrityTag = s.getInt(FORGE_INTEGRITY_TAG);
		}
		if (s.contains(ATTACKER_KEY_SEED)) {
			this.attackerKeySeed = s.getInt(ATTACKER_KEY_SEED);
		}
		if (s.contains(REPLAY_CAPTURE)) {
			this.replayCapture = s.getBoolean(REPLAY_CAPTURE);
		}
		if (s.contains(REPLAY_INTERVAL)) {
			this.replayInterval = s.getDouble(REPLAY_INTERVAL);
		}

		super.setAppID(APP_ID);
	}

	/**
	 * Copy constructor.
	 * @param a The application prototype where setting values are copied from
	 */
	public CachePoisoningApplication(CachePoisoningApplication a) {
		super(a);
		this.lastPing = a.lastPing;
		this.interval = a.interval;
		this.attackStart = a.attackStart;
		this.attackEnd = a.attackEnd;
		this.numOfMsgToGenerate = a.numOfMsgToGenerate;
		this.contentRangeMin = a.contentRangeMin;
		this.contentRangeMax = a.contentRangeMax;
		this.interestOrDataSize = a.interestOrDataSize;
		this.poisonPayloadSize = a.poisonPayloadSize;
		this.seed = a.seed;
		this.targetId = a.targetId;
		this.selectionMode = a.selectionMode;
		this.forgeIntegrityTag = a.forgeIntegrityTag;
		this.attackerKeySeed = a.attackerKeySeed;
		this.replayCapture = a.replayCapture;
		this.replayInterval = a.replayInterval;
		/* Captured-packet state is per-instance runtime state, not
		 * configuration - each replicated attacker host starts with
		 * nothing captured, same pattern as curGenerated/rng above. */
		this.lastReplay = 0;
		this.curReplayed = 0;
		this.capturedQueryMsg = null;
		this.capturedContent = null;
		this.capturedSecuritySource = null;
		this.capturedCreationTime = null;
		this.capturedIntegrityTag = null;

		/* curGenerated, sequentialCursor, and rng are per-instance runtime
		 * state, not configuration - each replicated attacker host starts
		 * its own count/cursor/generator, same pattern used throughout
		 * CCN_application and InterestFloodingApplication. */
		this.curGenerated = 0;
		this.sequentialCursor = -1;
		this.rng = null;
	}

	/**
	 * The attacker never needs to react to received messages - it only
	 * injects forged Data. Returning the message unchanged means it is not
	 * consumed here and normal router-level forwarding is unaffected.
	 *
	 * @param msg	message received by the router
	 * @param host	host to which the application instance is attached
	 */
	@Override
	public Message handle(Message msg, DTNHost host) {
		if(this.replayCapture){
			String type = (String) msg.getProperty("type");
			/* Only capture GENUINE signed Data: isPoison absent rules out
			 * this attack's own forged traffic (and any other attacker's),
			 * integrityTag present means it is actually worth replaying -
			 * an unsigned genuine packet would just look like any other
			 * missing-protection case on the receiving end, nothing new. */
			if(type != null && type.equalsIgnoreCase("queryResponse")
					&& msg.getProperty("isPoison") == null
					&& msg.getProperty("integrityTag") != null){
				this.capturedQueryMsg = (String) msg.getProperty("queryMsg");
				this.capturedContent = (String) msg.getProperty("content");
				this.capturedSecuritySource = (String) msg.getProperty("securitySource");
				this.capturedCreationTime = (String) msg.getProperty("creationTime");
				this.capturedIntegrityTag = (String) msg.getProperty("integrityTag");
			}
		}
		return msg;
	}

	/**
	 * @param host host for which the application instance is attached
	 */
	@Override
	public void update(DTNHost host) {
		/* Independent of the forging logic below - has its own guards
		 * (replayInterval<=0, nothing captured yet, attack window, own
		 * rate limit) and must run even on ticks where forging's own
		 * early-return conditions below would otherwise skip the rest
		 * of this method. */
		maybeReplayCaptured(host);

		double curTime = SimClock.getTime();

		if (curTime < this.attackStart || curTime > this.attackEnd) {
			return; /* outside the configured attack window */
		}

		if (curTime - this.lastPing < this.interval) {
			return; /* not yet time to inject the next forged Data packet */
		}

		if (this.curGenerated >= this.numOfMsgToGenerate) {
			return; /* forged-Data budget for this host is exhausted */
		}

		if (this.rng == null) {
			this.rng = new Random(this.seed + 2 * Integer.parseInt(host.toString().substring(1)));
		}

		int poisonedContentId = nextTargetId();
		DTNHost destination = randomHost(host);
		String forgedContent = buildForgedPayload(poisonedContentId);

		Message m = new Message(host, destination,
				"queryResponse" + SimClock.getIntTime() + "-" + host.getAddress() + "-" + this.curGenerated,
				this.interestOrDataSize);

		/* Same three properties CCN_application.handle() reads for any
		 * Data packet: "type"="queryResponse", "queryMsg"=the EXISTING
		 * valid content id as a String, "content"=the payload. Nothing
		 * else about this Message's handling differs from a real
		 * producer's response. */
		m.addProperty("type", "queryResponse");
		m.addProperty("queryMsg", Integer.toString(poisonedContentId));
		m.addProperty("content", forgedContent);

		/* Ground-truth marker only - see the class-level note above and
		 * the CCN_application.java patch: read only for instrumentation,
		 * never for caching/forwarding/acceptance decisions. */
		m.addProperty("isPoison", Boolean.TRUE);

		/* Optional: forgeIntegrityTag=1 makes this attacker attach
		 * protection-shaped properties anyway, computed under a key it
		 * fabricated itself (attackerKeySeed) - NOT CCN_application's real
		 * CCNSecurity.keySeed, which this attacker never has access to.
		 * This will reliably fail verification on the receiving side
		 * (IntegrityFailed), distinct from the default omit-entirely case
		 * (IntegrityMissing). Default (0) leaves these properties unset. */
		if(this.forgeIntegrityTag == FORGE_TAG_WRONG){
			String fakeSource = host.toString();
			String fakeCreationTime = Double.toString(SimClock.getTime());
			byte[] wrongKey = CCNSecurity.deriveKeyFromSeed(this.attackerKeySeed);
			String canonical = CCNSecurity.canonicalString("queryResponse",
					Integer.toString(poisonedContentId), forgedContent, fakeSource, fakeCreationTime);
			String wrongTag = CCNSecurity.hmacHex(wrongKey, canonical);
			m.addProperty("securitySource", fakeSource);
			m.addProperty("creationTime", fakeCreationTime);
			m.addProperty("integrityTag", wrongTag);
		}

		/* Stamped with CCN_application's own APP_ID (not this app's ID) so
		 * every receiving host's real CCN_application instance is the one
		 * that handles it, exactly as it would a real producer's Data. */
		m.setAppID(CCN_application.APP_ID);

		host.createNewMessage(m);

		super.sendEventToListeners("ForgedDataGenerated", Integer.toString(poisonedContentId), host);

		this.curGenerated++;
		this.lastPing = curTime;
	}

	/**
	 * Resends the most recently captured GENUINE signed Data verbatim -
	 * byte-for-byte the same content/queryMsg/securitySource/creationTime/
	 * integrityTag as when it was captured. This is a real replay: the
	 * tag IS genuinely valid (it was never altered), so integrity
	 * verification alone cannot catch it - only the receiving side's
	 * freshness/replay-state tracking can, exactly as described in the
	 * security design. Runs independently of the normal forging schedule
	 * in update() above; call this once per update() tick as well.
	 *
	 * @param host host for which the application instance is attached
	 */
	private void maybeReplayCaptured(DTNHost host) {
		if(this.replayInterval <= 0 || this.capturedIntegrityTag == null){
			return; // replaying disabled, or nothing captured yet
		}

		double curTime = SimClock.getTime();
		if(curTime < this.attackStart || curTime > this.attackEnd){
			return;
		}
		if(curTime - this.lastReplay < this.replayInterval){
			return;
		}

		Message r = new Message(host, randomHost(host),
				"replay" + SimClock.getIntTime() + "-" + host.getAddress() + "-" + this.curReplayed,
				this.interestOrDataSize);

		r.addProperty("type", "queryResponse");
		r.addProperty("queryMsg", this.capturedQueryMsg);
		r.addProperty("content", this.capturedContent);
		r.addProperty("securitySource", this.capturedSecuritySource);
		r.addProperty("creationTime", this.capturedCreationTime);
		r.addProperty("integrityTag", this.capturedIntegrityTag);

		/* Ground truth only, distinct from isPoison: this is a REPLAY of
		 * real content, not forged content - the payload is genuine, only
		 * its timing/reuse is malicious. Never read by caching/forwarding/
		 * acceptance logic. */
		r.addProperty("isReplayedByAttacker", Boolean.TRUE);

		r.setAppID(CCN_application.APP_ID);
		host.createNewMessage(r);

		super.sendEventToListeners("ReplayInjected", this.capturedQueryMsg, host);

		this.lastReplay = curTime;
		this.curReplayed++;
	}

	/**
	 * Chooses which valid content id to poison next.
	 * - If targetId is configured (>=0), always returns that one id, so a
	 *   single id can be poisoned continuously if that is what the
	 *   experiment calls for.
	 * - Otherwise rotates across [contentRangeMin, contentRangeMax): either
	 *   "sequential" (round-robin) or "random" (default), so the attack
	 *   does not have to continuously poison exactly one id.
	 *
	 * @return a content id inside [contentRangeMin, contentRangeMax)
	 */
	private int nextTargetId() {
		if (this.targetId >= 0) {
			return this.targetId;
		}

		int rangeSize = this.contentRangeMax - this.contentRangeMin;
		if (rangeSize <= 0) {
			rangeSize = 1;
		}

		if (this.selectionMode == SELECTION_MODE_SEQUENTIAL) {
			this.sequentialCursor = (this.sequentialCursor + 1) % rangeSize;
			return this.contentRangeMin + this.sequentialCursor;
		}

		return this.contentRangeMin + this.rng.nextInt(rangeSize);
	}

	/**
	 * Builds a forged payload string of the configured length, carrying no
	 * resemblance to whatever the real producer's content for this id
	 * would be - this IS the forged value CCN_application.handle() will
	 * cache under the real id.
	 *
	 * @param id the content id this payload is forged for (included only
	 *           to make forged values distinguishable from each other in
	 *           debug output; has no effect on caching behavior)
	 * @return a non-empty forged content string
	 */
	private String buildForgedPayload(int id) {
		StringBuilder sb = new StringBuilder();
		sb.append("POISON-").append(id).append("-");
		while (sb.length() < this.poisonPayloadSize) {
			sb.append('X');
		}
		return sb.toString();
	}

	/**
	 * Draws a random destination host, same pattern as
	 * CCN_application's own randomHost(DTNHost) and
	 * InterestFloodingApplication's randomHost(DTNHost): the forged
	 * Data's initial "to" field is just a starting point for opportunistic
	 * forwarding, not a routing decision.
	 *
	 * @param host host generating the forged Data
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
		return new CachePoisoningApplication(this);
	}
}
