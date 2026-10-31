package today.bonfire.oss.bth4j.redis;

import org.testcontainers.utility.DockerImageName;

/**
 * The server image the integration tests run against: {@code bth4j.redis.image}, {@code redis:8.8-alpine} by
 * default; the Maven profile {@code valkey} selects {@code valkey/valkey:9.1.3-alpine}.
 */
final class RedisImage {
    static final DockerImageName NAME =
            DockerImageName.parse(System.getProperty("bth4j.redis.image", "redis:8.8-alpine"));

    private RedisImage() {}
}
