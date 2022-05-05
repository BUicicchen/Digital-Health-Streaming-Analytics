package query.customtype;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

final class TypeUtils {
    private TypeUtils() {}

    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();

    static <T> Type<T> createJsonType(String typeNamespace, Class<T> jsonClass) {
        return SimpleType.simpleImmutableTypeFrom(
                TypeName.typeNameOf(typeNamespace, jsonClass.getName()),
                JSON_OBJ_MAPPER::writeValueAsBytes,
                bytes -> JSON_OBJ_MAPPER.readValue(bytes, jsonClass));
    }

}
