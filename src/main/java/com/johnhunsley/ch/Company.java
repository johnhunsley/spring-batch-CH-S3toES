package com.johnhunsley.ch;

import java.util.Objects;

public class Company {
    private String companyName;
    private String companyNumber;

    public Company() {}

    public Company(String companyName, String companyNumber) {
        this.companyName = companyName;
        this.companyNumber = companyNumber;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getCompanyNumber() {
        return companyNumber;
    }

    public void setCompanyNumber(String companyNumber) {
        this.companyNumber = companyNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Company company = (Company) o;
        return Objects.equals(companyName, company.companyName) &&
                Objects.equals(companyNumber, company.companyNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(companyName, companyNumber);
    }
}
