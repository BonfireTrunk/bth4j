package today.bonfire.oss.bth4j;

import com.alibaba.fastjson2.JSON;
import today.bonfire.oss.bth4j.common.JsonMapper;

public class JsonMapperTest implements JsonMapper {
  @Override public <T> T fromJson(String json, Class<T> clazz) throws Exception {
    return JSON.parseObject(json, clazz);
  }

  @Override public <T> T fromJson(byte[] json, Class<T> clazz) throws Exception {
    return JSON.parseObject(json, clazz);
  }

  @Override public String toJson(Object o) throws Exception {
    return JSON.toJSONString(o);
  }

  @Override public byte[] toJsonAsBytes(Object o) throws Exception {
    return JSON.toJSONBytes(o);
  }
}
