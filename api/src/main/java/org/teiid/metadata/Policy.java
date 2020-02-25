package org.teiid.metadata;

import java.util.LinkedHashSet;
import java.util.Set;

public class Policy extends AbstractMetadataRecord {

    public static enum Operation {
        ALL,
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }

    private static final long serialVersionUID = 6571225750196709855L;

    private Database.ResourceType resourceType;
    private String resourceName;
    private String condition;
    private Set<Operation> operations = new LinkedHashSet<>(1);

    public Database.ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(Database.ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String resource) {
        this.resourceName = resource;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public Set<Operation> getOperations() {
        return operations;
    }

    public boolean appliesTo(Operation operation) {
        return operations.isEmpty() || operations.contains(operation) || operations.contains(Operation.ALL);
    }

    //TODO: should this set the role (or resource) as parent

}
