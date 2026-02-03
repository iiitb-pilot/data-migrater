package io.mosip.packet.data.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONTokener;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ImportIdentityUtil {

    @Autowired
    ObjectMapper mapper;

    public JSONObject loadDemographicIdentity(Map<String, String> fieldMap) throws IOException, JSONException {
        JSONObject demographicIdentity = new JSONObject();
        for (Map.Entry e : fieldMap.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }

            String value = e.getValue().toString();
            if (value == null) {
                demographicIdentity.putIfAbsent(e.getKey(), value);
                continue;
            }

            Object json = new JSONTokener(value).nextValue();
            if (json instanceof org.json.JSONObject) {
                demographicIdentity.putIfAbsent(e.getKey(), mapper.readValue(value, Object.class));
                continue;
            }

            if (json instanceof JSONArray) {
                List jsonList = new ArrayList<>();
                JSONArray jsonArray = new JSONArray(value);
                for (int i = 0; i < jsonArray.length(); i++) {
                    Object obj = jsonArray.get(i);
                    if (obj instanceof String) {
                        jsonList.add(obj);
                    } else {
                        jsonList.add(mapper.readValue(obj.toString(), Object.class));
                    }
                }
                demographicIdentity.putIfAbsent(e.getKey(), jsonList);
            }
            else {
                if ("IDSchemaVersion".equalsIgnoreCase((String) e.getKey())) {
                    demographicIdentity.putIfAbsent(e.getKey(), Double.valueOf(value));
                } else {
                    demographicIdentity.putIfAbsent(e.getKey(), value);
                }
            }
        }
        return demographicIdentity;
    }
}
