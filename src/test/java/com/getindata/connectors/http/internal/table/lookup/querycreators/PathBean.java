/*
 * © Copyright IBM Corp. 2025
 */
package com.getindata.connectors.http.internal.table.lookup.querycreators;


import java.util.Objects;

public class PathBean {
    private String key1;
    public String getKey1() {
        return key1;
    }

    public PathBean(String key1) {
        this.key1 = key1;
    }
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PathBean pathBean = (PathBean) o;
        return Objects.equals(key1, pathBean.key1);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key1);
    }
}
