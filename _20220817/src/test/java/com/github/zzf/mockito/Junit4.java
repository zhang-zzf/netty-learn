package com.github.zzf.mockito;


import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.BDDMockito.given;

public class Junit4 {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    List list;

    @Test
    public void testJunit4WithMockito() {
        given(list.size()).willReturn(5);
        then(list.size()).isEqualTo(5);
    }


}
