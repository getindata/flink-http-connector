package com.getindata.connectors.http.internal.config;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import static org.apache.flink.configuration.description.TextElement.text;

public enum RetryStrategyType implements DescribedEnum {

    NONE("none", text("None")),
    FIXED_DELAY("fixed-delay", text("Fixed delay strategy")),
    EXPONENTIAL_DELAY("exponential-delay", text("Exponential delay strategy"));

    private final String value;
    private final InlineElement inlineElement;

    RetryStrategyType(String value, InlineElement inlineElement) {
        this.value = value;
        this.inlineElement = inlineElement;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return inlineElement;
    }

}
