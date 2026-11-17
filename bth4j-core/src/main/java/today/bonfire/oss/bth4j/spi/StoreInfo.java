package today.bonfire.oss.bth4j.spi;

/**
 * Identifies a store.
 *
 * @param engine        for example {@code redis}, {@code postgresql}, {@code memory}
 * @param schemaVersion the version of the tables or keys the store writes; {@link TaskStore#init} refuses another one. Each
 *                      store numbers its own versions
 */
public record StoreInfo(String engine, int schemaVersion) {}
