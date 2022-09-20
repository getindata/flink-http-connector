package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.LookupArg;

public class GenericGetQueryCreatorTest {
    static Stream<Arguments> queryArguments() {
        return Stream.of(
            Arguments.of(List.of(), ""),
            Arguments.of(List.of(new LookupArg("key1", "val1")), "key1=val1"),
            Arguments.of(List.of(
                new LookupArg("key1", "val1"),
                new LookupArg("key2", "val2"),
                new LookupArg("key3", "3")
            ), "key1=val1&key2=val2&key3=3")
        );
    }

    @ParameterizedTest
    @MethodSource("queryArguments")
    public void testGenericGetQueryCreation(List<LookupArg> args, String expectedQuery) {
        var queryCreator = new GenericGetQueryCreator();
        var createdQuery = queryCreator.createLookupQuery(args);
        assertThat(createdQuery).isEqualTo(expectedQuery);
    }
}
