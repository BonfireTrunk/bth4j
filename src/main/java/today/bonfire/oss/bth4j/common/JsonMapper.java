package today.bonfire.oss.bth4j.common;

/**
 * The JsonMapper interface provides methods for converting between JSON strings and Java objects.
 * Implementations of this interface should handle the serialization and deserialization process
 * while adhering to the specified contract.
 * <p>
 * Implementations may use various JSON processing libraries such as Jackson, Gson, or FastJson2,
 * or custom solutions to perform the JSON conversions.
 */
public interface JsonMapper {

  /**
   * Converts the given JSON string to an object of the specified class.
   *
   * @param <T>   the type of the object to which the JSON will be converted
   * @param json  the JSON string to convert
   * @param clazz the Class object representing the type to which the JSON should be converted
   * @return an instance of the specified class representing the JSON data
   *
   * @throws Exception if any error occurs during the conversion process, such as:
   *                   - Invalid JSON syntax
   *                   - Incompatible types between JSON and the specified class
   *                   - Inability to instantiate the target class
   */
  <T> T fromJson(String json, Class<T> clazz) throws Exception;

  /**
   * Converts the given JSON byte array to an object of the specified class.
   *
   * @param <T>   the type of the object to which the JSON will be converted
   * @param json  the JSON byte array to convert
   * @param clazz the Class object representing the type to which the JSON should be converted
   * @return an instance of the specified class representing the JSON data
   *
   * @throws Exception if any error occurs during the conversion process, such as:
   *                   - Invalid JSON syntax
   *                   - Incompatible types between JSON and the specified class
   *                   - Inability to instantiate the target class
   */
  <T> T fromJson(byte[] json, Class<T> clazz) throws Exception;

  /**
   * Converts the given object to a JSON string representation.
   *
   * @param o the object to convert to JSON
   * @return a JSON string representation of the given object
   *
   * @throws Exception if any error occurs during the conversion process, such as:
   *                   - Circular references in the object graph
   *                   - Inability to serialize certain object types
   *                   - Custom serialization errors defined by the implementation
   */
  String toJson(Object o) throws Exception;

  /**
   * Converts the given object to a JSON byte array representation.
   *
   * @param o the object to convert to JSON bytes
   * @return a JSON byte array representation of the given object
   *
   * @throws Exception if any error occurs during the conversion process, such as:
   *                   - Circular references in the object graph
   *                   - Inability to serialize certain object types
   *                   - Custom serialization errors defined by the implementation
   */
  byte[] toJsonAsBytes(Object o) throws Exception;
}
