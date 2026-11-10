/*
 * Copyright 2010 Aalto University, DCS Lab
 * Released under GPLv3. See LICENSE.txt for details.
 *
 * BPSec-inspired integrity-protection helper for CCN Data in this fork.
 */

package applications;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * BPSec-inspired integrity-protection helper for CCN Data.
 *
 * SCOPE - READ BEFORE USING ELSEWHERE: this is NOT an RFC 9172 (BPSec) or
 * RFC 9173 (default security contexts) implementation. There is no BPv7
 * bundle, no CBOR encoding, no BPSec security block (target/parameters/
 * results) structure, and no block-level canonicalization as those RFCs
 * define - this fork has no bundle layer at all. This class computes a
 * single HMAC-SHA256 tag over a custom, simulator-specific canonical
 * string built from existing CCN Message properties. It borrows BPSec's
 * BIB-HMAC-SHA2 CONCEPT (integrity via a shared-secret HMAC) without any
 * of its wire format or RFC-defined processing. Always call this
 * "BPSec-inspired CCN integrity" in reports/write-ups - never "BPSec" or
 * "BIB" unqualified, and never claim RFC 9172/9173 compliance.
 *
 * Key handling: deriveKeyFromSeed() uses java.util.Random, which is NOT
 * cryptographically secure. This is intentional and acceptable only
 * because this produces a reproducible SIMULATION test key, not a real
 * secret - the seed is an ordinary configuration value
 * (CCNSecurity.keySeed in settings), not something meant to resist a real
 * adversary. Do not reuse this key-derivation approach outside a
 * simulator, and never put a real production key in settings or source.
 *
 * Security properties actually achieved (and NOT achieved) by this class:
 * - A valid tag proves the (type, queryMsg, content, securitySource,
 *   creationTime) tuple was produced by someone holding the shared key,
 *   and was not altered afterward. Nothing more.
 * - Because the key is shared by every legitimate producer/relay (there
 *   is no per-node PKI here), this CANNOT distinguish one legitimate
 *   producer from another - "valid tag" means "someone with the key",
 *   not "this specific producer".
 * - This alone does NOT prevent replay of a previously-valid, captured
 *   packet - see CCN_application's freshness/replay-state tracking for
 *   that; this class only does the integrity math.
 * - This does nothing against Interest Flooding - that remains the
 *   separate PIT-capacity/timeout defense already in CCN_application.
 */
public class CCNSecurity {

	private static final String HMAC_ALGO = "HmacSHA256";
	private static final int KEY_LENGTH_BYTES = 32;

	private CCNSecurity() {
		// static utility class, not instantiated
	}

	/**
	 * Deterministically derives a 32-byte HMAC-SHA256 key from an integer
	 * seed, using java.util.Random (NOT cryptographically secure - see
	 * class-level note). The same seed always yields the same key, which
	 * is exactly what a reproducible simulation needs: every legitimate
	 * node constructed with the same CCNSecurity.keySeed setting derives
	 * the identical key independently, with nothing transmitted over the
	 * network to provision it.
	 *
	 * @param seed configuration-provided seed (CCNSecurity.keySeed)
	 * @return a 32-byte key suitable for HmacSHA256
	 */
	public static byte[] deriveKeyFromSeed(int seed) {
		Random r = new Random(seed);
		byte[] key = new byte[KEY_LENGTH_BYTES];
		r.nextBytes(key);
		return key;
	}

	/**
	 * Parses an explicit hex-encoded test key, for callers that want a
	 * literal fixed key instead of seed derivation (e.g. hard-coded in a
	 * specific unit test). Not wired to any Settings key in this project,
	 * since no plain string-valued Settings getter is confirmed to exist
	 * anywhere in the supplied source - CCNSecurity.keySeed (int) is the
	 * only settings-driven key-provisioning path. Still a plaintext
	 * simulation test key, never a production secret.
	 *
	 * @param hex even-length hex string, e.g. "0a1b2c..."
	 * @return the decoded key bytes
	 */
	public static byte[] deriveKeyFromHex(String hex) {
		int len = hex.length();
		byte[] out = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			out[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
		}
		return out;
	}

	/**
	 * Builds the exact canonical string that gets authenticated. Used
	 * identically whether signing (producer side, at response-creation
	 * time) or verifying (any receiving node), so both sides compute the
	 * same bytes from the same Message properties.
	 *
	 * Binds together: packet type, the numeric queryMsg/content id, the
	 * content value itself, the claimed security source (producer
	 * identity), and the creation time (freshness field) - exactly the
	 * fields the research goal requires bound together, and nothing an
	 * attacker without the key can alter without invalidating the tag.
	 *
	 * @param type            e.g. "queryResponse"
	 * @param queryMsg        numeric content id, as a String
	 * @param content         the content value
	 * @param securitySource  the DTNHost.toString() of whoever generated this Data
	 * @param creationTime    SimClock time at generation, as a String
	 * @return the canonical string to authenticate
	 */
	public static String canonicalString(String type, String queryMsg, String content,
			String securitySource, String creationTime) {
		return type + "|" + queryMsg + "|" + content + "|" + securitySource + "|" + creationTime;
	}

	/**
	 * Computes the HMAC-SHA256 tag over the canonical string, returned as
	 * a lowercase hex string so it can be carried as a plain String
	 * Message property, consistent with every other property in this
	 * fork's CCN messages (type, queryMsg, content are all Strings too).
	 *
	 * @param key       HMAC key bytes
	 * @param canonical the canonical string from canonicalString(...)
	 * @return lowercase hex-encoded HMAC-SHA256 tag
	 */
	public static String hmacHex(byte[] key, String canonical) {
		try {
			Mac mac = Mac.getInstance(HMAC_ALGO);
			mac.init(new SecretKeySpec(key, HMAC_ALGO));
			byte[] raw = mac.doFinal(canonical.getBytes(StandardCharsets.UTF_8));
			return toHex(raw);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("HmacSHA256 not available on this JVM", e);
		} catch (InvalidKeyException e) {
			throw new RuntimeException("Invalid HMAC key", e);
		}
	}

	/**
	 * Recomputes the expected tag and compares it to the provided one
	 * using a constant-time comparison (MessageDigest.isEqual), so
	 * verification time does not itself leak how many leading bytes of
	 * a forged tag happened to match.
	 *
	 * @param key            HMAC key bytes
	 * @param canonical      the canonical string from canonicalString(...)
	 * @param providedTagHex the tag to verify, as received (lowercase hex), or null if absent
	 * @return true only if a tag was provided and matches exactly
	 */
	public static boolean verifyHex(byte[] key, String canonical, String providedTagHex) {
		if (providedTagHex == null) {
			return false;
		}
		String expected = hmacHex(key, canonical);
		return MessageDigest.isEqual(
				expected.getBytes(StandardCharsets.UTF_8),
				providedTagHex.getBytes(StandardCharsets.UTF_8));
	}

	private static String toHex(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		for (byte b : bytes) {
			sb.append(Character.forDigit((b >> 4) & 0xF, 16));
			sb.append(Character.forDigit(b & 0xF, 16));
		}
		return sb.toString();
	}
}
