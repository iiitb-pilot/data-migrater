package io.mosip.packet.core.constant.tracker;

public enum StringType {
    MSSQL("VARCHAR"),
    ORACLE("VARCHAR2"),
    MYSQL("VARCHAR"),
    POSTGRESQL("VARCHAR");

    private final String type;

    StringType(String type) {
        this.type = type;
    }

    public String getValue(String val) {
        return type + String.format("(%s)", val);
    }
}
