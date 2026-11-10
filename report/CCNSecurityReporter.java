/*
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details.
 */
package report;

import core.Application;
import core.DTNHost;
import core.ApplicationListener;

/**
 * Reports BPSec-inspired integrity-protection metrics: verification
 * outcomes (valid / missing / failed), replay detection and policy
 * disposition, consumer-acceptance rejections, and - reusing the same
 * ground-truth poisoning events from CachePoisoningReporter's dependencies
 * - how integrity enforcement interacts with forged-content acceptance.
 *
 * Deliberately separate from CCN_application_reporter and
 * CachePoisoningReporter. Run all three together (Report.report1/2/3/4)
 * to compare: legitimate QoS (CCN_application_reporter), raw poisoning
 * ground truth (CachePoisoningReporter), and integrity-policy effect
 * (this class) on the same run.
 *
 * Depends on these events, fired from the CCNSecurity-related patch to
 * applications/CCN_application.java:
 *   IntegrityValid, IntegrityMissing, IntegrityFailed, ReplayDetected,
 *   ReplayAccepted, ReplayRejected, ConsumerAcceptanceRejected
 * plus the pre-existing OriginalGotPoisonedResponse (from the cache-
 * poisoning work) and SentData (fired whether or not security is
 * enabled) events, used here to measure overhead and forged-acceptance
 * interaction without modifying either earlier reporter.
 */
public class CCNSecurityReporter extends Report implements ApplicationListener {

	private int integrityValid = 0;
	private int integrityMissing = 0;
	private int integrityFailed = 0;
	private int replayDetected = 0;
	private int replayAccepted = 0;
	private int replayRejected = 0;
	private int consumerAcceptanceRejected = 0;
	private int poisonedResponsesDespiteSecurity = 0;
	private int totalDataSent = 0;

	public void gotEvent(String event, Object params, Application app, DTNHost host) {
		if (event.equals("IntegrityValid")) {
			this.integrityValid++;
		}
		if (event.equals("IntegrityMissing")) {
			this.integrityMissing++;
		}
		if (event.equals("IntegrityFailed")) {
			this.integrityFailed++;
		}
		if (event.equals("ReplayDetected")) {
			this.replayDetected++;
		}
		if (event.equals("ReplayAccepted")) {
			this.replayAccepted++;
		}
		if (event.equals("ReplayRejected")) {
			this.replayRejected++;
		}
		if (event.equals("ConsumerAcceptanceRejected")) {
			this.consumerAcceptanceRejected++;
		}
		/* A consumer still accepting forged content WHILE security
		 * verification events are also firing is the single most
		 * important cross-check this reporter provides: with
		 * CCNSecurity.policy=reject and the feature actually wired
		 * correctly, this should drop to (near) zero compared to the
		 * CachePoisoningReporter baseline run without security enabled. */
		if (event.equals("OriginalGotPoisonedResponse")) {
			this.poisonedResponsesDespiteSecurity++;
		}
		if (event.equals("SentData")) {
			this.totalDataSent++;
		}
	}

	@Override
	public void done() {
		int totalVerifications = this.integrityValid + this.integrityMissing + this.integrityFailed;
		double validFraction = totalVerifications == 0 ? 0.0
				: (double) this.integrityValid / totalVerifications;

		write("BPSec-inspired CCN Integrity stats for scenario " + getScenarioName() +
				"\nsim_time: " + format(getSimTime()) +
				"\nNOTE: this is a BPSec-INSPIRED HMAC-SHA256 adaptation for CCN Messages " +
				"in this fork, not an RFC 9172/9173-compliant BPSec implementation " +
				"(no BPv7 bundles, no security blocks, no CBOR encoding).");

		String statsText =
			"\nintegrity_valid: " + this.integrityValid +
			"\nintegrity_missing: " + this.integrityMissing +
			"\nintegrity_failed: " + this.integrityFailed +
			"\nintegrity_valid_fraction: " + validFraction +
			"\nreplay_detected: " + this.replayDetected +
			"\nreplay_accepted: " + this.replayAccepted +
			"\nreplay_rejected: " + this.replayRejected +
			"\nconsumer_acceptance_rejected: " + this.consumerAcceptanceRejected +
			"\npoisoned_responses_despite_security: " + this.poisonedResponsesDespiteSecurity +
			"\ntotal_data_sent: " + this.totalDataSent
			;

		write(statsText);
		super.done();
	}
}
