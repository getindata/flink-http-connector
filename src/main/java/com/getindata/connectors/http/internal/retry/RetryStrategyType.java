package com.getindata.connectors.http.internal.retry;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum RetryStrategyType {
    FIXED_DELAY("fixed-delay"),
    EXPONENTIAL_DELAY("exponential-delay"),
    ;

    private final String code;

    public static RetryStrategyType fromCode(String code) {
        if (code == null) {
            throw new NullPointerException("Code is null");
        }
        for (var strategy : RetryStrategyType.values()) {
            if (strategy.getCode().equalsIgnoreCase(code)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("No enum constant for " + code);
    }
}
