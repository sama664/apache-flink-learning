package org.myorg.quickstart.model;

public class Feature {
    public String featureName;
    public Boolean isEnabled;
    public int age;

    public Feature() {}
    public Feature(String featureName, Boolean isEnabled, int age) {
        this.featureName = featureName;
        this.isEnabled = isEnabled;
        this.age = age;
    }
    public String toString() {
        return this.featureName.toString() + ": isEnabled " + this.isEnabled.toString() + ", age " + this.age;
    }
}
