package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;
import java.util.UUID;

import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

final class ClickHouseUUIDParser extends ClickHouseValueParser<UUID> {

    private static ClickHouseUUIDParser instance;

    static ClickHouseUUIDParser getInstance() {
        if (instance == null) {
            instance = new ClickHouseUUIDParser();
        }
        return instance;
    }

    private ClickHouseUUIDParser() {
        // prevent regular instantiation
    }

    @Override
    public UUID parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        if (value.isNull()) {
            return null;
        }
        String s = value.asString();
        if (s.isEmpty()) {
            return null;
        }
        try {
            return UUID.fromString(s);
        } catch (Exception e) {
            throw new ClickHouseUnknownException(
                "error parsing UUID '" + value.asString() + "'", e);
        }
    }

}
