package com.kg.config;

import java.util.List;

/**
 * Created by kevingracie on 22/09/2016.
 */
public class Configuration {

    private List<String> elasticNodes;
    private String clusterName;
    private String sourceIndex;
    private String destinationIndex;
    private String destinationType;
    private int scrollSize;
    private long searchContextTimeValue;

    public List<String> getElasticNodes() {
        return elasticNodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }

    public String getDestinationIndex() {
        return destinationIndex;
    }

    public String getDestinationType() {
        return destinationType;
    }

    public int getScrollSize() {
        return scrollSize;
    }

    public long getSearchContextTimeValue() {
        return searchContextTimeValue;
    }

    public void setElasticNodes(List<String> elasticNodes) {
        this.elasticNodes = elasticNodes;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setSourceIndex(String sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    public void setDestinationIndex(String destinationIndex) {
        this.destinationIndex = destinationIndex;
    }

    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }

    public void setScrollSize(int scrollSize) {
        this.scrollSize = scrollSize;
    }

    public void setSearchContextTimeValue(long searchContextTimeValue) {
        this.searchContextTimeValue = searchContextTimeValue;
    }
}
