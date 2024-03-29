SNOWFLAKE_TYPE_CONVERSIONS = {
    "NUMBER": {"value": "number"},
    "DECIMAL": {"value": "number"},
    "NUMERIC": {"value": "number"},
    "INT": {"value": "number"},
    "INTEGER": {"value": "number"},
    "BIGINT": {"value": "number"},
    "SMALLINT": {"value": "number"},
    "FLOAT": {"value": "number"},
    "FLOAT4": {"value": "number"},
    "FLOAT8": {"value": "number"},
    "DOUBLE": {"value": "number"},
    "DOUBLE PRECISION": {"value": "number"},
    "REAL": {"value": "number"},
    "VARCHAR": {"value": "string"},
    "CHAR": {"value": "string"},
    "CHARACTER": {"value": "string"},
    "STRING": {"value": "string"},
    "TEXT": {"value": "string"},
    "BINARY": {"value": "string"},
    "VARBINARY": {"value": "string"},
    "BOOLEAN": {"value": "yesno"},
    "DATE": {"value": "time"},
    "DATETIME": {"value": "time"},
    "TIME": {"value": "string"},
    "TIMESTAMP": {"value": "time"},
    "TIMESTAMP_NTZ": {"value": "time"},
    "TIMESTAMP_TZ": {
        "value": "time",
        "sql": "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.\"{name}\") AS TIMESTAMP_NTZ)",
    },
    "TIMESTAMP_LTZ": {
        "value": "time",
        "sql": "CAST(CONVERT_TIMEZONE('UTC', ${{TABLE}}.\"{name}\") AS TIMESTAMP_NTZ)",
    },
    "VARIANT": {"value": "string"},
    "OBJECT": {"value": "string"},
    "ARRAY": {"value": "string"},
    "GEOGRAPHY": {"value": "string"},
}
