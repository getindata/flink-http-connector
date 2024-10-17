package com.getindata.connectors.http.internal.status;

import java.util.function.Predicate;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * Predicate that validates status code against constant value.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
class SingleValueHttpStatusCodeCheckerPredicate implements Predicate<Integer> {

    /**
     * A reference http status code to compare with.
     */
    private final int errorCode;

    /**
     * Validates given statusCode against constant value.
     *
     * @param statusCode http status code to assess.
     * @return <code>true</code> if status code is equal to expected value.
     */
    @Override
    public boolean test(Integer statusCode) {
        return errorCode == statusCode;
    }
}
