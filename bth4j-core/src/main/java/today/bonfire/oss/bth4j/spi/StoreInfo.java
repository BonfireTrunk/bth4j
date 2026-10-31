package today.bonfire.oss.bth4j.spi;

/**
 * Identifies a store.
 *
 * @param engine        for example {@code redis}, {@code postgresql}, {@code memory}
 * @param schemaVersion the layout version this library writes; {@link TaskStore#init} refuses another one
 */
public record StoreInfo(String engine, int schemaVersion) {

    /** The layout version of this library. */
    public static final int SCHEMA_VERSION = 1;
}
