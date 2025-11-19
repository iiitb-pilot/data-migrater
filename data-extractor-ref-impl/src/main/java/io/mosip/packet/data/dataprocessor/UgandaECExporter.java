package io.mosip.packet.data.dataprocessor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.packet.core.constant.database.DBDriverType;
import io.mosip.packet.core.constant.database.DBTypes;
import io.mosip.packet.core.dto.DataPostProcessorResponseDto;
import io.mosip.packet.core.dto.DataProcessorResponseDto;
import io.mosip.packet.core.dto.TrackerAdditionalColumns;
import io.mosip.packet.core.exception.ExceptionUtils;
import io.mosip.packet.core.logger.DataProcessLogger;
import io.mosip.packet.core.service.thread.ResultSetter;
import io.mosip.packet.core.spi.datapostprocessor.DataPostProcessor;
import io.mosip.packet.core.util.TrackerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.*;

import static io.mosip.packet.core.constant.GlobalConfig.IS_RUNNING_AS_BATCH;
import static io.mosip.packet.core.constant.GlobalConfig.PACKET_TRACKER_ADDITIONAL_FIELDS;

@Component
public class UgandaECExporter implements DataPostProcessor {
    private static final Logger LOGGER = DataProcessLogger.getLogger(TrackerUtil.class);
    private static Connection conn = null;

    @Autowired
    private Environment env;

    @Value("${mosip.extractor.uganda.ec.table.mapping:{}}")
    private String tableMapping;

    @Autowired
    private ObjectMapper objectMapper;

    private List<String> fieldsToStore;

    public final String UG_TABLE_NAME = "UG_DATA_EXPORTER";

    private String preparedQuery;

    @Override
    public DataPostProcessorResponseDto postProcess(DataProcessorResponseDto processObject, ResultSetter setter, Long startTime) throws Exception {
        intialize();
        prepareQuery();
        Map<String, Object> map = processObject.getResponses();

        DataPostProcessorResponseDto responseDto = new DataPostProcessorResponseDto();
        responseDto.setProcess(processObject.getProcess());
        responseDto.setRefId(processObject.getRefId());
        responseDto.setTrackerRefId(processObject.getTrackerRefId());

        PreparedStatement preparedStatement = conn.prepareStatement(preparedQuery);
        for(int i=0; i < fieldsToStore.size(); i++) {
            preparedStatement.setObject(i+1, map.get(fieldsToStore.get(i)));
        }
        preparedStatement.executeUpdate();
        return responseDto;
    }

    private void prepareQuery() throws Exception {
        if(preparedQuery == null) {
            JsonNode jsonNode = objectMapper.readTree(tableMapping);

            fieldsToStore = new ArrayList<>();
            parseJsonNode(jsonNode);
            StringBuilder query = new StringBuilder("INSERT INTO " + UG_TABLE_NAME + "( " + String.join(",", fieldsToStore) + ") VALUES(");

            for(int i=0; i < fieldsToStore.size(); i++) {
                query.append("?");

                if(i < fieldsToStore.size()-1)
                    query.append(",");
            }
            query.append(")");
            preparedQuery = query.toString();
        }
    }

    private void intialize() throws Exception {
        if(conn == null) {
            DBTypes dbType = Enum.valueOf(DBTypes.class, env.getProperty("spring.datasource.uganda.ec.dbtype"));

            Class driverClass = Class.forName(dbType.getDriver());
            DriverManager.registerDriver((Driver) driverClass.newInstance());
            String driverFormat = env.getProperty("spring.datasource.uganda.ec.driver.format");
            DBDriverType dbDriverType = DBDriverType.valueOf(driverFormat == null ? DBDriverType.DEFAULT.toString() : driverFormat);
            String connectionHost = String.format(dbType.getDriverUrl(dbDriverType), env.getProperty("spring.datasource.uganda.ec.host"), env.getProperty("spring.datasource.uganda.ec.port"), env.getProperty("spring.datasource.uganda.ec.database"));
            conn = DriverManager.getConnection(connectionHost, env.getProperty("spring.datasource.uganda.ec.username"), env.getProperty("spring.datasource.uganda.ec.password"));
            conn.setAutoCommit(true);

            Statement statement = null;
            try {
                statement = conn.createStatement();
                statement.execute("SELECT 1 FROM " + UG_TABLE_NAME + " LIMIT 1");
            } catch (Exception e) {
                System.out.println("Table " + UG_TABLE_NAME +  " not Present in DB " + env.getProperty("spring.datasource.uganda.ec.host") + ExceptionUtils.getStackTrace(e));
                throw new Exception("Table " + UG_TABLE_NAME +  " not Present in DB " + env.getProperty("spring.datasource.uganda.ec.host") + ExceptionUtils.getStackTrace(e));
            } finally {
                if(statement != null)
                    statement.close();
            }
        }
    }

    private void parseJsonNode(JsonNode node) throws Exception {
        String fieldName = null;
        try {
            // If object → loop fields
            if (node.isObject()) {
                ObjectNode obj = (ObjectNode) node;

                Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();

                    fieldName = entry.getKey();

                    // recursive call for nested elements
                    fieldsToStore.add(fieldName);
                }
            }

            if (node.isArray()) {
                ArrayNode arr = (ArrayNode) node;
                for (int i = 0; i < arr.size(); i++) {
                    parseJsonNode(arr.get(i));
                }
            }
        } catch (Exception e) {
            throw new Exception("Error Occured while extract Data for field " + fieldName + ExceptionUtils.getStackTrace(e));
        }
    }
}
