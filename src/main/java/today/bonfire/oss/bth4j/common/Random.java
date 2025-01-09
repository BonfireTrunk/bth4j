package today.bonfire.oss.bth4j.common;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;

public class Random {
  private static final SecureRandom   secureRandom  = new SecureRandom();
  private static final Base64.Encoder base64Encoder = Base64.getUrlEncoder()
                                                            .withoutPadding();

  public static byte[] generateByteArray(int byteLength) {
    byte[] randomBytes = new byte[byteLength];
    secureRandom.nextBytes(randomBytes);
    return randomBytes;
  }

  /**
   * UUID uses 16 byte length for approx 128 bits of security
   * this is for varying length
   *
   * @param byteLength no of random bytes to used to generate the token
   */
  public static String generateNewToken(int byteLength) {
    return base64Encoder.encodeToString(generateByteArray(byteLength));
  }

  /**
   * uuid but with base64 encoding
   * 18 does not need padding since base64 is 4 char for 3 bytes
   * and provides sufficient collision resistance for general purposes
   * (greater that normal uuid)
   * we use url encode to ensure maximum compatibility to pass data to anywhere.
   *
   * @return base64 encoded string of 144 bits of assumed random data
   */
  public static String UIDBASE64() {
    return generateNewToken(18);
  }

  /**
   * Timestamp UID
   * Instant + uniqueValue
   * 9 chars + 16 chars
   * time + 128bits of random stuff
   * should be better than uuid for all practical use cases
   * the timestamp would be in a sortable manner,
   * but not withing the same millisecond
   *
   * @return 24chars of random data
   */
  public static String tuid() {
    var time = Long.toString(Instant.now()
                                    .toEpochMilli(), 32);
    var rand = generateNewToken(12);
    return time + rand;
  }
}
