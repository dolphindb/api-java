package com.xxdb.comm;

public enum SqlStdEnum {

    DolphinDB("DolphinDB", 0),
    Oracle("Oracle", 1),
    MySQL("MySQL", 2)
    ;

    private String name;

    private Integer code;

    SqlStdEnum(String name, Integer code) {
        this.name = name;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public Integer getCode() {
        return code;
    }

    public static SqlStdEnum getByName(String name) {
        for (SqlStdEnum sqlStd : SqlStdEnum.values()) {
            if (sqlStd.getName().equals(name)) {
                return sqlStd;
            }
        }
        throw new IllegalArgumentException("No matching SqlStdEnum constant found for name: " + name);
    }

}
