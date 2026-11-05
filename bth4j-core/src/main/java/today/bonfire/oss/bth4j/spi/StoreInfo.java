package today.bonfire.oss.bth4j.spi;

/**
 * Identifies a store.
 *
 * @param engine        for example {@code redis}, {@code postgresql}, {@code memory}
 * @param schemaVersion the layout version the store writes; {@link TaskStore#init} refuses another one. Each
 *                      store numbers its own layouts
 */
public record StoreInfo(String engine, int schemaVersion) {}
