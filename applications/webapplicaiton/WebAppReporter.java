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

public class WebAppReporter extends Report implements ApplicationListener {

	private int opportunistic_cache_hit=0;
	private int opportunistic_cache_miss=0;
	private int queries_sent=0;
	private int static_cache_hit=0;
	private int static_cache_sent=0;
	private int response_count=0;
	
	private List<Double>	delays = new ArrayList<Double>();
	
	public void gotEvent(String event, Object params, Application app,
			DTNHost host) {
		if (event.equals("SentResponseFromOpportunisticCache")) {
			this.opportunistic_cache_hit++;
		}
		if (event.equals("OpportunisticCacheMiss")) {
			this.opportunistic_cache_miss++;
		}
		if (event.equals("SentResponseFromStaticCache")) {
			this.static_cache_sent++;
		}
		if (event.equals("CreatedQuery")) {
			this.queries_sent++;
		}
		if (event.equals("GotResponse")) {
			this.delays.add((Double)params);
			this.response_count++;
		}
		if (event.equals("StaticCacheHit")) {
			this.static_cache_hit++;
		}
	}
	
	@Override
	public void done() {
		write("WebApp stats for scenario " + getScenarioName() + 
				"\nsim_time: " + format(getSimTime()));
		
		double responseProb = 0.0;
		double cacheProb = 0.0;
		
		if (this.queries_sent > 0) {
			responseProb = (1.0* this.response_count) / 
				this.queries_sent;
		}
		if (this.queries_sent > 0) {
			cacheProb = (1.0* this.opportunistic_cache_hit) / 
				this.queries_sent;
		}
		
		String statsText = 
			"\nqueries_created: " + this.queries_sent + 
			"\nstatic_cache_hits: " + this.static_cache_hit +
			"\nstatic_cache_sent: " + this.static_cache_sent +
			"\nopportunistic_cache_matches: " + this.opportunistic_cache_hit +
			"\nopportunistic_cache_prob: " + format(cacheProb)+
			"\nopportunistic_cache_misses: " + this.opportunistic_cache_miss +
			"\nresponses_through: " + this.response_count +
			"\nresponse_latency_avrg: " + getAverage(this.delays) +
			"\nresponse_latency_var: " + getVariance(this.delays) +
			"\nresponse_prob: " + format(responseProb)
			;
		
		write(statsText);
		super.done();
	}

}
