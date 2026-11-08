package today.bonfire.oss.bth4j.admin.http;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/** A salted PBKDF2-HMAC-SHA256 hash of a password; the password itself is never kept. */
record PasswordHash(byte[] salt, byte[] hash) {

    /** The OWASP Password Storage Cheat Sheet figure for PBKDF2-HMAC-SHA256. */
    static final int ITERATIONS = 600_000;

    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 256;
    private static final SecureRandom RANDOM = new SecureRandom();

    static PasswordHash create(String password) {
        var salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        return new PasswordHash(salt, derive(password, salt));
    }

    /** Compares in constant time; the work is the same whichever hash is given. */
    boolean matches(String password) {
        return MessageDigest.isEqual(derive(password, salt), hash);
    }

    private static byte[] derive(String password, byte[] salt) {
        var spec = new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, HASH_BITS);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(spec)
                    .getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 is required of every JDK", e);
        } finally {
            spec.clearPassword();
        }
    }
}
