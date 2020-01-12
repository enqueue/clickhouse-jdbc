package ru.yandex.clickhouse.response.parser;

import java.util.UUID;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.response.ByteFragment;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseUUIDParserTest {

    @Test
    public void testNullValue() throws Exception {
        assertNull(ClickHouseUUIDParser.getInstance().parse(
            ByteFragment.fromString("\\N"), null, null));
    }

    @Test
    public void testEmptyValue() throws Exception {
        assertNull(ClickHouseUUIDParser.getInstance().parse(
            ByteFragment.fromString(""), null, null));
    }

    @Test
    public void testSimpleUUID() throws Exception {
        UUID uuid = UUID.randomUUID();
        assertEquals(
            ClickHouseUUIDParser.getInstance().parse(
                ByteFragment.fromString(uuid.toString()), null, null),
            uuid);
        assertNotEquals(
            ClickHouseUUIDParser.getInstance().parse(
                ByteFragment.fromString(uuid.toString()), null, null),
            UUID.randomUUID());
    }

    @Test
    public void testBrokenUUID() throws Exception {
        try {
            ClickHouseUUIDParser.getInstance().parse(
                ByteFragment.fromString("BROKEN"), null, null);
            fail();
        } catch (ClickHouseException che) {
            // expected
        }
    }

}
