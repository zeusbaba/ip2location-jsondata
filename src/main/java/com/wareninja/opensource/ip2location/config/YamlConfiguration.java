package com.wareninja.opensource.ip2location.config;

public class YamlConfiguration {

    private Importer importer;
    private Elastic elastic;

    public Elastic getElastic() {
        return elastic;
    }

    public void setElastic(Elastic elastic) {
        this.elastic = elastic;
    }

    public Importer getImporter() {
        return importer;
    }

    public void setImporter(Importer importer) {
        this.importer = importer;
    }

    @Override
    public String toString() {
        return "{" +
                "elastic=" + elastic 
                + ", importer=" + importer
                + '}';
    }
}