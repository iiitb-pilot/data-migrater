package io.mosip.packet.data.dataprocessor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.packet.core.config.activity.Activity;
import io.mosip.packet.core.constant.GlobalConfig;
import io.mosip.packet.core.constant.activity.ActivityName;
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
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static io.mosip.packet.core.constant.GlobalConfig.IS_RUNNING_AS_BATCH;
import static io.mosip.packet.core.constant.GlobalConfig.PACKET_TRACKER_ADDITIONAL_FIELDS;
import static io.mosip.packet.core.constant.RegistrationConstants.APPLICATION_ID;
import static io.mosip.packet.core.constant.RegistrationConstants.APPLICATION_NAME;

@Component
public class UgandaECExporter implements DataPostProcessor {
    private static final Logger LOGGER = DataProcessLogger.getLogger(TrackerUtil.class);
    private DataSource dataSource;

    @Autowired
    private Environment env;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mosip.extractor.uganda.ec.table.mapping:{}}")
    private String tableMapping;

    private List<String> fieldsToStore = new ArrayList<>();

    public final String UG_TABLE_NAME = "UG_DATA_EXPORTER";

    private String preparedQuery;

    private final Object lock = new Object();

    private static final int BATCH_SIZE = 5;

    private final ConcurrentLinkedQueue<Map<String,Object>> batchBuffer = new ConcurrentLinkedQueue<>();


    @Override
    public DataPostProcessorResponseDto postProcess(DataProcessorResponseDto processObject, ResultSetter setter, Long startTime) throws Exception {
        LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + processObject.getRefId() + " Time taken to get Connection From Database " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));
        Map<String, Object> record = processObject.getResponses();

        synchronized (lock) {
            LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + processObject.getRefId() + " Time taken to Enter the Lock Method " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));
            batchBuffer.add(record);
            // If batch full → flush to DB
            if (batchBuffer.size() >= BATCH_SIZE) {
                flushBatch(processObject.getRefId(), startTime);
            }
        }
        LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + processObject.getRefId() + " Time taken to Exist the Lock Method " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

        DataPostProcessorResponseDto responseDto = new DataPostProcessorResponseDto();
        responseDto.setProcess(processObject.getProcess());
        responseDto.setRefId(processObject.getRefId());
        responseDto.setTrackerRefId(processObject.getTrackerRefId());
        return responseDto;
    }

    private void flushBatch(String ref_id, Long startTime) throws Exception {
        List<Map<String, Object>> toFlush;
        LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to Enter the Flush Method " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

        LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to Enter the Flush Lock Method " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));
        if (batchBuffer.isEmpty()) return;
        toFlush = new ArrayList<>(batchBuffer);
        batchBuffer.clear();

        LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to Enter the Flush Lock Exist Method " + toFlush.size() + " - "  + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(preparedQuery)) {
            conn.setAutoCommit(false);
            LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to getting Connection from Database " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

            for (Map<String, Object> rec : toFlush) {
                for (int i = 0; i < fieldsToStore.size(); i++) {
                    ps.setObject(i + 1, rec.get(fieldsToStore.get(i)));
                }
                ps.addBatch();
            }
            LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to Map fields with PreparedStatement " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

            ps.executeBatch();
            LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to After Executing batch " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));
            conn.commit();
            LOGGER.info("SESSION_ID", APPLICATION_NAME, APPLICATION_ID, "Thread - " + ref_id + " Time taken to Exit the Flush Method " + TimeUnit.MILLISECONDS.convert(System.nanoTime()-startTime, TimeUnit.NANOSECONDS));

        } catch (Exception e) {
            // If batch failed, put records back in buffer
            synchronized (lock) {
                batchBuffer.addAll(toFlush);
            }
            throw e;
        }
    }

    @PostConstruct
    public void init() throws Exception {
        try {
            initializeDataSource();
            prepareFields();
            prepareQuery();
            validateTableExists();
        } catch (Exception e) {
            LOGGER.error("INIT_FAILED", APPLICATION_NAME, APPLICATION_ID, "Failed to initialize UgandaECExporter: " + ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    @PreDestroy
    public void onShutdown() {
        try {
            flushBatch(null, System.nanoTime());
        } catch (Exception e) {
            LOGGER.error("Failed to flush batch on shutdown: {}", e.getMessage());
        }
    }


    // -------------------------------------------------------------------------
    // 1. CREATE DATASOURCE INSIDE SAME CLASS
    // -------------------------------------------------------------------------
    private void initializeDataSource() {
        HikariDataSource ds = new HikariDataSource();
        DBTypes dbType = Enum.valueOf(DBTypes.class, env.getProperty("spring.datasource.uganda.ec.dbtype"));
        String driverFormat = env.getProperty("spring.datasource.uganda.ec.driver.format");
        DBDriverType dbDriverType = DBDriverType.valueOf(driverFormat == null ? DBDriverType.DEFAULT.toString() : driverFormat);
        String connectionHost = String.format(dbType.getDriverUrl(dbDriverType), env.getProperty("spring.datasource.uganda.ec.host"), env.getProperty("spring.datasource.uganda.ec.port"), env.getProperty("spring.datasource.uganda.ec.database"));

        ds.setJdbcUrl(connectionHost);
        ds.setUsername(env.getProperty("spring.datasource.uganda.ec.username"));
        ds.setPassword(env.getProperty("spring.datasource.uganda.ec.password"));
        ds.setDriverClassName(dbType.getDriver());

        ds.setMaximumPoolSize(20);
        ds.setMinimumIdle(5);
        ds.setPoolName("UgandaECExporterPool");

        this.dataSource = ds;
    }

    // -------------------------------------------------------------------------
    // 3. Parse fields from JSON mapping
    // -------------------------------------------------------------------------
    private void prepareFields() throws Exception {
        JsonNode node = objectMapper.readTree(tableMapping);

        fieldsToStore.clear();
        Iterator<String> it = node.fieldNames();

        while (it.hasNext()) {
            fieldsToStore.add(it.next());
        }
    }

    // -------------------------------------------------------------------------
    // 4. Build INSERT query
    // -------------------------------------------------------------------------
    private void prepareQuery() {
        StringJoiner cols = new StringJoiner(",");
        StringJoiner vals = new StringJoiner(",");

        fieldsToStore.forEach(f -> {
            cols.add(f);
            vals.add("?");
        });

        preparedQuery = "INSERT INTO " + UG_TABLE_NAME +
                " (" + cols + ") VALUES (" + vals + ")";
    }


    // -------------------------------------------------------------------------
    // 5. Validate table exists
    // -------------------------------------------------------------------------
    private void validateTableExists() throws Exception {

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM " + UG_TABLE_NAME + " LIMIT 1")) {
            ps.executeQuery();
        }
    }
}
