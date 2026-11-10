/*
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details.
 */
package report;

import core.Application;
import core.ApplicationListener;
import core.DTNHost;

/**
 * Reports Interest Flooding Attack specific metrics: malicious Interest
 * volume, PIT-capacity drops, PIT timeouts, aggregate PIT occupancy, and
 * the overall Interest/Data ratio for the scenario.
 *
 * Deliberately kept separate from CCN_application_reporter, which
 * continues to own the legitimate-consumer QoS metrics (delivery ratio,
 * delay, cache hit ratio) unchanged.
 *
 * Depends on these events, fired from applications/InterestFloodingApplication.java
 * and from the minimal patch applied to applications/CCN_application.java:
 *   SentFloodInterest, SentQuery, SentData, PITFull, PITTimeout, PITOccupancy
 */
public class InterestFloodingReporter extends Report implements ApplicationListener {

	private int sentFloodInterests = 0;
	private int sentLegitimateInterests = 0;
	private int sentData = 0;
	private int pitFullDrops = 0;
	private int pitTimeouts = 0;

	/* PITOccupancy events are fired by every host's CCN_application
	 * instance whenever its own PIT changes size, so these aggregate
	 * across all forwarding-capable hosts in the scenario, not a single
	 * host. Treat pit_occupancy_avg/pit_occupancy_max as a scenario-wide
	 * signal rather than any one node's peak. */
	private int pitOccupancySamples = 0;
	private long pitOccupancySum = 0;
	private int pitOccupancyMax = 0;

	public void gotEvent(String event, Object params, Application app, DTNHost host) {
		if (event.equals("SentFloodInterest")) {
			this.sentFloodInterests++;
		}
		if (event.equals("SentQuery")) {
			this.sentLegitimateInterests++;
		}
		if (event.equals("SentData")) {
			this.sentData++;
		}
		if (event.equals("PITFull")) {
			this.pitFullDrops++;
		}
		if (event.equals("PITTimeout")) {
			this.pitTimeouts++;
		}
		if (event.equals("PITOccupancy")) {
			int occupancy = Integer.parseInt((String) params);
			this.pitOccupancySamples++;
			this.pitOccupancySum += occupancy;
			if (occupancy > this.pitOccupancyMax) {
				this.pitOccupancyMax = occupancy;
			}
		}
	}

	@Override
	public void done() {
		int totalInterests = this.sentFloodInterests + this.sentLegitimateInterests;
		double interestToDataRatio = (this.sentData == 0) ? Double.NaN
				: ((double) totalInterests / (double) this.sentData);
		double avgPitOccupancy = (this.pitOccupancySamples == 0) ? 0.0
				: ((double) this.pitOccupancySum / (double) this.pitOccupancySamples);

		write("Interest Flooding Attack stats for scenario " + getScenarioName() +
				"\nsim_time: " + format(getSimTime()));

		String statsText =
			"\nsent_flood_interests: " + this.sentFloodInterests +
			"\nsent_legitimate_interests: " + this.sentLegitimateInterests +
			"\nsent_data: " + this.sentData +
			"\ninterest_to_data_ratio: " + interestToDataRatio +
			"\npit_full_drops: " + this.pitFullDrops +
			"\npit_timeouts: " + this.pitTimeouts +
			"\npit_occupancy_avg: " + avgPitOccupancy +
			"\npit_occupancy_max: " + this.pitOccupancyMax
			;

		write(statsText);
		super.done();
	}
}
