package ru.yandex.clickhouse.response.parser;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseSQLDateParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;

    @BeforeClass
    public void setUp() {
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
    }

    @Test
    public void testParseSQLDateNull() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        try {
            ClickHouseSQLDateParser.getInstance().parse(
                null, columnInfo, tzBerlin);
            fail();
        } catch (NullPointerException npe) {
            // should be handled before calling the parser
        }
    }

    @Test
    public void testParseSQLDateDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Date", "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzLosAngeles),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzLosAngeles.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        // local stays local
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12"), columnInfo, tzBerlin),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("0000-00-00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseSQLDateDateNullable() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("Nullable(Date)", "col");
        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("0000-00-00"), columnInfo, tzLosAngeles));
    }

    @Test
    public void testParseSQLDateDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime", "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzLosAngeles.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test
    public void testParseSQLDateDateTimeTZColumn() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Europe/Berlin)", "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzLosAngeles.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test
    public void testParseSQLDateDateTimeOtherTZColumn() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse("DateTime(Asia/Vladivostok)", "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 01:02:03"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 11)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzLosAngeles),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzLosAngeles.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-12 22:23:24"), columnInfo, tzBerlin),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("0000-00-00 00:00:00"), columnInfo, null));
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseSQLDateNumber(ClickHouseDataType dataType) throws Exception {

        Instant instant = LocalDateTime.of(2020, 1, 12, 22, 23, 24)
            .atZone(tzLosAngeles.toZoneId())
            .toInstant();
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(dataType.name(), "col");

        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString(
                    String.valueOf(instant.getEpochSecond())),
                columnInfo,
                tzLosAngeles),
            new Date(
                LocalDate.of(2020, 1, 12)
                    .atStartOfDay(tzLosAngeles.toZoneId())
                    .toInstant()
                    .toEpochMilli()));

        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString(
                    String.valueOf(instant.getEpochSecond())),
                    columnInfo,
                    tzBerlin),
            new Date(
                LocalDate.of(2020, 1, 13)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));

        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("ABC"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }

        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("3.14159265359"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }

        assertNull(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString(String.valueOf(0)), columnInfo, tzBerlin));
    }

    @Test
    public void testParseSQLDateNumberNegative() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Int64.name(), "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString(
                    String.valueOf(-386384400)),
                columnInfo,
                tzBerlin),
            new Date(
                LocalDate.of(1957, 10, 4)
                    .atStartOfDay(tzBerlin.toZoneId())
                    .toInstant()
                    .toEpochMilli()));
    }

    @Test
    public void testParseSQLDateOtherLikeDate() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-13"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 13)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-1-13"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // bad format
        }
        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-42"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // bad format
        }
    }

    @Test
    public void testParseSQLDateOtherLikeDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo =
            ClickHouseColumnInfo.parse(
                ClickHouseDataType.Unknown.name(),
                "col");
        assertEquals(
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-13 22:23:24"), columnInfo, null),
            new Date(
                LocalDate.of(2020, 1, 13)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli()));
        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-1-13 22:23:24"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // bad format
        }
        try {
            ClickHouseSQLDateParser.getInstance().parse(
                ByteFragment.fromString("2020-01-42 22:23:24"), columnInfo, null);
            fail();
        } catch (ClickHouseException che) {
            // bad format
        }
    }

}
