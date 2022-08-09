package com.github.zzf.assertj;

import lombok.Getter;
import lombok.Setter;
import org.assertj.core.api.BDDAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * @author zhanfeng.zhang
 * @date 2022/04/10
 */
public class AssertJTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private final String STR = "Hello, World";

    @Mock
    Function<String, Integer> function;

    /**
     * 结果 assert，过程 verify
     */
    @Test
    public void givenCommonUsage_when_then() {
        // given: mock stub
        given(function.apply(any())).willReturn(1);
        // when
        final Integer ans = function.apply("");
        // then
        // 对接口返回值断言
        then(ans).isEqualTo(1);
        // verify: method invoke
        BDDMockito.then(function).should(times(1)).apply(any());
    }

    /**
     * 一个结果，多个断言
     */
    @Test
    public void assertThatUsage() {
        BDDAssertions.then(STR)
                .isEqualTo(STR).isNotEqualTo("").isEqualToIgnoringCase(STR)
                .startsWith("He").endsWith("ld").contains(", ")
                .isNotNull().isNotBlank().isNotEmpty()
                .hasSizeBetween(1, 20).hasSize(12).hasLineCount(1)
                .isOfAnyClassIn(String.class).isInstanceOfAny(CharSequence.class);

    }

    /**
     * 捕捉程序异常
     */
    @Test
    public void givenException_whenCaughtException_thenSuccess() {
        // when
        final Throwable throwable = BDDAssertions.catchThrowable(() -> {
            // code that throw exception when executing it.
            throw new IllegalArgumentException();
        });
        // then
        BDDAssertions.then(throwable)
                .isInstanceOfAny(IllegalArgumentException.class)
                .hasMessage(null);
    }

    /**
     * 一个对象多个属性的多个断言
     */
    @org.junit.jupiter.api.Test
    void given_whenCheckMultiProperties_then() {
        Person person = Person.valueOf("zhanfeng.zhang", 22);
        // method one
        then(person)
                .returns("zhanfeng.zhang", from(Person::getName))
                .returns(22, from(Person::getAge));
    }

    /**
     * stub void method
     */
    @Test
    public void givenVoidMethod_thenStubThrowException_then() {
        final Person mock = mock(Person.class);
        // given
        // given(mock.setAge(any())).willThrow(new IllegalArgumentException());
        doThrow(new IllegalArgumentException()).when(mock).setName(anyString());
        // when
        final Throwable throwable = catchThrowable(() -> mock.setName(anyString()));
        // then
        then(throwable).isInstanceOfAny(IllegalArgumentException.class);
    }

}

@Getter
@Setter
class Person {

    private String name;
    private int age;

    public static Person valueOf(String name, int age) {
        final Person p = new Person();
        p.setName(name);
        p.setAge(age);
        return p;
    }

}
