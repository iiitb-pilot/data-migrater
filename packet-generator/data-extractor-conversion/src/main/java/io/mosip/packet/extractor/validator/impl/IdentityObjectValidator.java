package io.mosip.packet.extractor.validator.impl;

import io.mosip.packet.core.dto.dbimport.DBImportRequest;
import io.mosip.packet.extractor.validator.Validator;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class IdentityObjectValidator implements Validator {

    @Override
    public Boolean validate(DBImportRequest dbImportRequest) throws Exception {

        Path identityFile = Paths.get(System.getProperty("user.dir"), "identity.json");

        if (identityFile.toFile().exists()) {
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(IOUtils.toString(new FileInputStream(identityFile.toFile()), StandardCharsets.UTF_8));
            JSONObject identityJsonObject = (JSONObject) jsonObject.get("identity");
            String dob = (String) ((JSONObject) identityJsonObject.get("dob")).get("value");
            String crdt = (String) ((JSONObject) identityJsonObject.get("crdt")).get("value");

            if (dob == null || crdt == null)
                throw new Exception("Missing Attributes for dob or crdt in identity.json");
        }
        return true;
    }
}
