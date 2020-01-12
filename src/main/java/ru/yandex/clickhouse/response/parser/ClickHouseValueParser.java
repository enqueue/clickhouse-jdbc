package ru.yandex.clickhouse.response.parser;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

public abstract class ClickHouseValueParser<T> {

    static Map<Class<?>, ClickHouseValueParser<?>> parsers;

    static {
        parsers = new HashMap<>();
        parsers.put(Date.class, ClickHouseSQLDateParser.getInstance());
        parsers.put(Instant.class, ClickHouseInstantParser.getInstance());
        parsers.put(LocalDate.class, ClickHouseLocalDateParser.getInstance());
        parsers.put(LocalDateTime.class, ClickHouseLocalDateTimeParser.getInstance());
        parsers.put(LocalTime.class, ClickHouseLocalTimeParser.getInstance());
        parsers.put(OffsetDateTime.class, ClickHouseOffsetDateTimeParser.getInstance());
        parsers.put(OffsetTime.class, ClickHouseOffsetTimeParser.getInstance());
        parsers.put(String.class, ClickHouseStringParser.getInstance());
        parsers.put(Time.class, ClickHouseSQLTimeParser.getInstance());
        parsers.put(Timestamp.class, ClickHouseSQLTimestampParser.getInstance());
        parsers.put(UUID.class, ClickHouseUUIDParser.getInstance());
        parsers.put(ZonedDateTime.class, ClickHouseZonedDateTimeParser.getInstance());
    }

    @SuppressWarnings("unchecked")
    public static <T> ClickHouseValueParser<T> getParser(Class<?> clazz) throws SQLException {
        ClickHouseValueParser<T> p = (ClickHouseValueParser<T>) parsers.get(clazz);
        if (p == null) {
            throw new ClickHouseUnknownException(
                "No value parser for class '" + clazz.getName() + "'", null);
        }
        return p;
    }

    public abstract T parse(ByteFragment value, ClickHouseColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException;

}
