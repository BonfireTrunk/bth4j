package today.bonfire.oss.bth4j.common;

public record Pair<T, U>(T first, U second) {

  public static <T, U> Pair<T, U> of(T firstItem, U secondItem) {
    return new Pair<>(firstItem, secondItem);
  }

  public static <T, U> Pair<T, U> of(T firstItem) {
    return new Pair<>(firstItem, null);
  }
}
