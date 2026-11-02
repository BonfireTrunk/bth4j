package today.bonfire.oss.bth4j.spi;

/**
 * Identifies a store.
 *
 * @param engine        for example {@code redis}, {@code postgresql}, {@code memory}
 * @param schemaVersion the layout version the store writes; {@link TaskStore#init} refuses another one. Each
 *                      store has its own: the Redis layout changed in 3.1, the PostgreSQL one did not
 */
public record StoreInfo(String engine, int schemaVersion) {

    /** The layout version of the PostgreSQL and in-memory stores. */
    public static final int SCHEMA_VERSION = 1;
}
