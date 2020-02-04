package ru.yandex.clickhouse.response.parser;

import java.time.Instant;
import java.util.TimeZone;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;

public class ClickHouseInstantParserTest {

    private TimeZone tzLosAngeles;
    private TimeZone tzBerlin;
    private ClickHouseInstantParser parser;

    @BeforeClass
    public void setUp() {
        parser = ClickHouseInstantParser.getInstance();
        tzLosAngeles = TimeZone.getTimeZone("America/Los_Angeles");
        tzBerlin = TimeZone.getTimeZone("Europe/Berlin");
    }

    @Test
    public void testParseInstantDateTime() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime", "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579587804);
    }

    @Test
    public void testParseInstantDateTimeColumnOverride() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "DateTime(Europe/Berlin)", "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20 22:23:24"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
    }

    @Test
    public void testParseInstantDate() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "Date", "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void testParseInstantTimestampSeconds(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
    }

    @Test(
        dataProvider = ClickHouseTimeParserTestDataProvider.OTHER_DATA_TYPES,
        dataProviderClass = ClickHouseTimeParserTestDataProvider.class)
    public void parseInstantTimestampMillis(ClickHouseDataType dataType) throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            dataType.name(), "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("1579507200000"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
        inst = parser.parse(
            ByteFragment.fromString("1579507200"), columnInfo, tzBerlin);
        assertEquals(
            inst.getEpochSecond(),
            1579507200);
    }

    @Test
    public void testParseInstantString() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse(
            "String", "col");
        Instant inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579587804);
        inst = parser.parse(
            ByteFragment.fromString("2020-01-20T22:23:24.123+01:00"), columnInfo, tzLosAngeles);
        assertEquals(
            inst.getEpochSecond(),
            1579555404);
    }

}
