/*
 * © Copyright IBM Corp. 2025
 */
package com.getindata.connectors.http.internal.table.lookup.querycreators;

<<<<<<< HEAD
import lombok.Data;

@Data
public class PersonBean {
    private final String firstName;
    private final String lastName;
=======
import java.util.Objects;

public class PersonBean {
    private String firstName;
    private String lastName;

    public PersonBean() {
    }

    public PersonBean(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PersonBean that = (PersonBean) o;
        return Objects.equals(firstName, that.firstName) && Objects.equals(lastName, that.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstName, lastName);
    }
>>>>>>> 6c68722 (HTTP-99 Generic Json url query creator)
}
