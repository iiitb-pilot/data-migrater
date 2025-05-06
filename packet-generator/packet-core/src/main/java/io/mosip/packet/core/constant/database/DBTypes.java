package io.mosip.packet.core.constant.database;

import static io.mosip.packet.core.constant.database.DBDriverType.*;

import java.util.Map;

public enum DBTypes {
    MSSQL("com.microsoft.sqlserver.jdbc.SQLServerDriver", Map.of(DEFAULT, "jdbc:sqlserver://%s:%s;sslProtocol=TLSv1.2;databaseName=%s;Trusted_Connection=True;")),
    ORACLE("oracle.jdbc.driver.OracleDriver", Map.of(SID, "jdbc:oracle:thin:@%s:%s:%s", SERVICE, "jdbc:oracle:thin:@//%s:%s/%s", TNS, "jdbc:oracle:thin:@%s", DEFAULT, "jdbc:oracle:thin:@%s:%s:%s")),
    MYSQL("com.mysql.cj.jdbc.Driver", Map.of(DEFAULT,"jdbc:mysql://%s:%s/%s")),
    POSTGRESQL("org.postgresql.Driver", Map.of(DEFAULT,"jdbc:postgresql://%s:%s/%s?useSSL=false"));

    private final String driver;
    private final Map<DBDriverType, String> driverUrl;

    DBTypes(String driver, Map<DBDriverType, String> driverUrl) {
        this.driver = driver;
        this.driverUrl = driverUrl;
    }

    public String getDriver() {
        return driver;
    }

    public String getDriverUrl(DBDriverType driverFormat) {
        if(driverFormat == null)
            return driverUrl.get(DEFAULT);

        return driverUrl.get(driverFormat);
    }
}
