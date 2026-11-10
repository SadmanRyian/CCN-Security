/*
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details.
 */
package report;

import java.util.HashMap;

import core.Application;
import core.ApplicationListener;
import core.DTNHost;
import core.Settings;
import core.SimClock;

/**
 * Reports Cache (Content) Poisoning Attack specific metrics: forged Data
 * generation/propagation/caching, poisoned vs legitimate cache hits,
 * eviction of valid entries, cache occupancy/capacity, and legitimate
 * consumer impact bucketed into before/during/after the attack window.
 *
 * Deliberately kept separate from CCN_application_reporter, which keeps
 * owning the baseline legitimate-consumer QoS metrics unchanged. This
 * reporter reads CachePoisoner.attackStart/attackEnd directly from
 * Settings (same technique CCN_application itself uses) purely to bucket
 * events by simulated time - it does not change attack behavior.
 *
 * Depends on these events, fired from applications/CachePoisoningApplication.java
 * and from the minimal ground-truth instrumentation patch applied to
 * applications/CCN_application.java:
 *   ForgedDataGenerated, ForgedDataReceived, ForgedDataCached,
 *   ForgedDataCachedStatic, PoisonedCacheHit, LegitCacheHit,
 *   OriginalGotPoisonedResponse, ValidEntryEvicted, PoisonedEntryEvicted,
 *   CacheOccupancy ("len,capacity,poisonedCount")
 * plus the pre-existing SentQuery / OriginalGotResponse events (already
 * fired unconditionally by CCN_application, unrelated to this attack) used
 * here only to bucket legitimate delivery/delay by phase.
 */
public class CachePoisoningReporter extends Report implements ApplicationListener {

	private static final String ATTACK_START = "attackStart";
	private static final String ATTACK_END = "attackEnd";

	private double attackStart = 0;
	private double attackEnd = Double.MAX_VALUE;

	private int forgedDataGenerated = 0;
	private int forgedDataReceived = 0;
	private int forgedDataCachedOppo = 0;
	private int forgedDataCachedStatic = 0;
	private int poisonedCacheHits = 0;
	private int legitCacheHits = 0;
	private int originalGotPoisonedResponse = 0;
	private int originalGotAnyResponse = 0;
	private int validEntriesEvicted = 0;
	private int poisonedEntriesEvicted = 0;

	/* CacheOccupancy carries "len,capacity,poisonedCount" per host per
	 * write - these aggregate across every host that reported, same
	 * network-wide-signal caveat as InterestFloodingReporter's PIT
	 * occupancy (not one host's trace, an aggregate over all of them). */
	private int occSamples = 0;
	private long occLenSum = 0;
	private int occLenMax = 0;
	private long occPoisonedSum = 0;
	private int occPoisonedMax = 0;
	private int lastOccLen = 0;
	private int lastOccCapacity = 0;
	private int lastOccPoisonedCount = 0;

	/* Per-phase legitimate delivery/delay tracking, bucketed by when the
	 * query was SENT. Keyed by host+":"+queryKey, since "SentQuery" and
	 * the eventual response event both fire on the SAME originating host. */
	private HashMap<String, Double> pendingSentTime = new HashMap<String, Double>();
	private HashMap<String, String> pendingPhase = new HashMap<String, String>();

	private int[] phaseQueryCount = new int[3];      // 0=before,1=during,2=after
	private int[] phaseResponseCount = new int[3];
	private int[] phasePoisonedResponseCount = new int[3];
	private double[] phaseDelaySum = new double[3];
	private int[] phaseDelayCount = new int[3];

	public CachePoisoningReporter() {
		super();
		Settings s = new Settings("CachePoisoner");
		if (s.contains(ATTACK_START)) {
			this.attackStart = s.getDouble(ATTACK_START);
		}
		if (s.contains(ATTACK_END)) {
			this.attackEnd = s.getDouble(ATTACK_END);
		}
	}

	private int phaseAt(double time) {
		if (time < this.attackStart) return 0;
		if (time > this.attackEnd) return 2;
		return 1;
	}

	private String phaseLabel(int p) {
		switch (p) {
			case 0: return "before";
			case 2: return "after";
			default: return "during";
		}
	}

	public void gotEvent(String event, Object params, Application app, DTNHost host) {
		double now = SimClock.getTime();

		if (event.equals("ForgedDataGenerated")) {
			this.forgedDataGenerated++;
		}
		if (event.equals("ForgedDataReceived")) {
			this.forgedDataReceived++;
		}
		if (event.equals("ForgedDataCached")) {
			this.forgedDataCachedOppo++;
		}
		if (event.equals("ForgedDataCachedStatic")) {
			this.forgedDataCachedStatic++;
		}
		if (event.equals("PoisonedCacheHit")) {
			this.poisonedCacheHits++;
		}
		if (event.equals("LegitCacheHit")) {
			this.legitCacheHits++;
		}
		if (event.equals("ValidEntryEvicted")) {
			this.validEntriesEvicted++;
		}
		if (event.equals("PoisonedEntryEvicted")) {
			this.poisonedEntriesEvicted++;
		}
		if (event.equals("CacheOccupancy")) {
			String[] parts = ((String) params).split(",");
			int len = Integer.parseInt(parts[0]);
			int capacity = Integer.parseInt(parts[1]);
			int poisonedCount = Integer.parseInt(parts[2]);

			this.occSamples++;
			this.occLenSum += len;
			if (len > this.occLenMax) this.occLenMax = len;
			this.occPoisonedSum += poisonedCount;
			if (poisonedCount > this.occPoisonedMax) this.occPoisonedMax = poisonedCount;

			this.lastOccLen = len;
			this.lastOccCapacity = capacity;
			this.lastOccPoisonedCount = poisonedCount;
		}

		/* Legitimate-impact bucketing, independent of CCN_application_reporter. */
		if (event.equals("SentQuery")) {
			String key = host.toString() + ":" + params;
			int phase = phaseAt(now);
			this.pendingSentTime.put(key, now);
			this.pendingPhase.put(key, phaseLabel(phase));
			this.phaseQueryCount[phase]++;
		}
		if (event.equals("OriginalGotResponse") || event.equals("OriginalGotPoisonedResponse")) {
			String key = host.toString() + ":" + params;
			Double sentAt = this.pendingSentTime.get(key);
			String phaseStr = this.pendingPhase.get(key);
			int phase = "before".equals(phaseStr) ? 0 : ("after".equals(phaseStr) ? 2 : 1);

			if (event.equals("OriginalGotResponse")) {
				this.originalGotAnyResponse++;
				this.phaseResponseCount[phase]++;
			}
			if (event.equals("OriginalGotPoisonedResponse")) {
				this.originalGotPoisonedResponse++;
				this.phasePoisonedResponseCount[phase]++;
			}
			if (sentAt != null) {
				this.phaseDelaySum[phase] += (now - sentAt);
				this.phaseDelayCount[phase]++;
				/* Only clear bookkeeping on the plain response event, since
				 * a poisoned response and a later legitimate one could in
				 * principle both reference the same key before cleanup;
				 * CCN_application's own query_record removal (which this
				 * mirrors) already prevents a second OriginalGotResponse
				 * for the same key in practice. */
				if (event.equals("OriginalGotResponse")) {
					this.pendingSentTime.remove(key);
					this.pendingPhase.remove(key);
				}
			}
		}
	}

	private double avgDelay(int phase) {
		return this.phaseDelayCount[phase] == 0 ? 0.0
				: this.phaseDelaySum[phase] / this.phaseDelayCount[phase];
	}

	private double deliveryRatio(int phase) {
		return this.phaseQueryCount[phase] == 0 ? 0.0
				: (double) this.phaseResponseCount[phase] / this.phaseQueryCount[phase];
	}

	private double cacheHitRatioOverall() {
		int total = this.poisonedCacheHits + this.legitCacheHits;
		return total == 0 ? 0.0 : (double) this.legitCacheHits / total;
	}

	@Override
	public void done() {
		write("Cache Poisoning Attack stats for scenario " + getScenarioName() +
				"\nsim_time: " + format(getSimTime()));

		double avgOccLen = this.occSamples == 0 ? 0.0 : (double) this.occLenSum / this.occSamples;
		double avgOccPoisoned = this.occSamples == 0 ? 0.0 : (double) this.occPoisonedSum / this.occSamples;
		int totalInterestOrData = this.forgedDataGenerated; // forged side only, by design of this reporter
		double poisonedToLegitHitRatio = this.legitCacheHits == 0 ? Double.NaN
				: (double) this.poisonedCacheHits / this.legitCacheHits;

		String statsText =
			/* #1-#3: forged data lifecycle */
			"\nforged_data_generated: " + this.forgedDataGenerated +
			"\nforged_data_received: " + this.forgedDataReceived +
			"\nforged_data_cached_oppo: " + this.forgedDataCachedOppo +
			"\nforged_data_cached_static: " + this.forgedDataCachedStatic +
			/* #4, #9: occupancy/capacity/currently-poisoned - EXACT, via LRUCache.set()
			 * now returning the evicted key and CCN_application's own getCacheCapacity() */
			"\ncurrent_poisoned_entries_last: " + this.lastOccPoisonedCount +
			"\npoisoned_entries_avg: " + avgOccPoisoned +
			"\npoisoned_entries_max: " + this.occPoisonedMax +
			"\ncache_occupancy_last: " + this.lastOccLen +
			"\ncache_occupancy_avg: " + avgOccLen +
			"\ncache_occupancy_max: " + this.occLenMax +
			"\ncache_capacity_last_seen: " + this.lastOccCapacity +
			/* #5, #7: hit classification */
			"\npoisoned_cache_hits: " + this.poisonedCacheHits +
			"\nlegit_cache_hits: " + this.legitCacheHits +
			"\npoisoned_to_legit_hit_ratio: " + poisonedToLegitHitRatio +
			"\ncache_hit_legit_fraction_overall: " + cacheHitRatioOverall() +
			/* #6: consumer deception */
			"\noriginal_got_any_response: " + this.originalGotAnyResponse +
			"\noriginal_got_poisoned_response: " + this.originalGotPoisonedResponse +
			/* #8: eviction of valid entries - EXACT, via LRUCache.set()'s returned evicted key */
			"\nvalid_entries_evicted: " + this.validEntriesEvicted +
			"\npoisoned_entries_evicted: " + this.poisonedEntriesEvicted +
			/* #10, #11: legitimate impact, before/during/after */
			"\nquery_count_before: " + this.phaseQueryCount[0] +
			"\nquery_count_during: " + this.phaseQueryCount[1] +
			"\nquery_count_after: " + this.phaseQueryCount[2] +
			"\ndelivery_ratio_before: " + deliveryRatio(0) +
			"\ndelivery_ratio_during: " + deliveryRatio(1) +
			"\ndelivery_ratio_after: " + deliveryRatio(2) +
			"\npoisoned_response_count_before: " + this.phasePoisonedResponseCount[0] +
			"\npoisoned_response_count_during: " + this.phasePoisonedResponseCount[1] +
			"\npoisoned_response_count_after: " + this.phasePoisonedResponseCount[2] +
			"\navg_delay_before: " + avgDelay(0) +
			"\navg_delay_during: " + avgDelay(1) +
			"\navg_delay_after: " + avgDelay(2)
			;

		write(statsText);
		super.done();
	}
}
