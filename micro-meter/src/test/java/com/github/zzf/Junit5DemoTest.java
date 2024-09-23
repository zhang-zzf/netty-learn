package com.github.zzf;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @author zhanfeng.zhang
 * @date 2022/04/10
 */
public class Junit5DemoTest {

    /**
     * 参数化测试，可以快速测试边界值。
     * <p>编程写死各种入参</p>
     */
    @DisplayName("@ParameterizedTest with ValueSource")
    @ParameterizedTest
    @NullSource
    @EmptySource
    @NullAndEmptySource
    @ValueSource(strings = {"  ", " ", "    "})
    void givenParameterizedTest_whenWithValueSource_thenSuccess(String str) {
        boolean b = str == null || str.trim().isEmpty();
        then(b).isTrue();
    }

    /**
     * 有多个入参的参数化测试
     *
     * <p>使用 CSV 格式，code 层写死条件</p>
     */
    @DisplayName("@ParameterizedTest with CsvSource")
    @ParameterizedTest(name = "concat({0}, {1}) is {2}")
    @CsvSource({
            "4,5,45",
            "1,2,12",
    })
    void givenParameterizedTest_whenWithCsvSource_thenSuccess(int i, long l, String str) {
        String r = "" + i + l;
        then(r).isEqualTo(str);
    }

    /**
     * 有多个入参的参数化测试
     * <p>使用 csv 文件</p>
     */
    @DisplayName("@ParameterizedTest with CsvFileSource")
    @ParameterizedTest(name = "concat({0}, {1}) is {2}")
    @CsvSource({
            "4,5,45",
            "1,2,12",
    })
    @CsvFileSource(resources = {"/csv_file.txt"})
    void givenParameterizedTest_whenWithCsvFileSource_thenSuccess(int i, long l, String str) {
        String r = "" + i + l;
        then(r).isEqualTo(str);
    }

    @DisplayName("@ParameterizedTest with EnumSource")
    @ParameterizedTest()
    @EnumSource(value = TimeUnit.class, names = {"MINUTES", "DAYS"})
    void givenParameterizedTest_whenWithEnumParams_thenSuccess(TimeUnit unit) {
        then(EnumSet.allOf(TimeUnit.class).contains(unit)).isTrue();
    }

    @DisplayName("@ParameterizedTest with MethodSource")
    @ParameterizedTest
    @MethodSource("stringIntAndListProvider")
    void testWithMultiArgMethodSource(String str, int num, List<String> list) {
        assertEquals(5, str.length());
        assertTrue(num >= 1 && num <= 2);
        assertEquals(2, list.size());
    }

    static Stream<Arguments> stringIntAndListProvider() {
        return Stream.of(
                arguments("apple", 1, Arrays.asList("a", "b")),
                arguments("lemon", 2, Arrays.asList("x", "y"))
        );
    }

    /**
     * 跳过测试
     */
    @Test
    @Disabled
    void givenDisable_whenAssertEquals_then() {

    }

    /**
     * 限时测试
     */
    @Test
    @Disabled
    @Timeout(1)
    void givenTimeout_when_then() throws InterruptedException {
        Thread.sleep(1200);
    }


}
