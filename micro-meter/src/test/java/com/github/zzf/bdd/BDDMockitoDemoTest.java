package com.github.zzf.bdd;

import org.assertj.core.api.BDDAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;

/**
 * @author zhanfeng.zhang
 * @date 2022/04/10
 */
public class BDDMockitoDemoTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @InjectMocks
    PhoneBookService phoneBookService;

    @Mock
    PhoneBookRepository phoneBookRepository;

    String momContactName = "Mom";
    String momPhoneNumber = "01234";

    @Test
    public void givenValidContactName_whenSearchInPhoneBook_thenReturnPhoneNum() {
        // given or stub
        given(phoneBookRepository.contains(momContactName)).willReturn(true);
        given(phoneBookRepository.getPhoneNumberByContactName(anyString()))
                .will(invocation -> momContactName.equals(invocation.getArgument(0)) ? momPhoneNumber : null);
        // when
        String number = phoneBookService.search(momContactName);
        // verify
        BDDMockito.then(phoneBookRepository).should(atLeastOnce()).contains(momContactName);
        BDDMockito.then(phoneBookRepository).should(atMost(1)).getPhoneNumberByContactName(momContactName);
        // then
        BDDAssertions.then(number).isEqualTo(momPhoneNumber);
    }

}