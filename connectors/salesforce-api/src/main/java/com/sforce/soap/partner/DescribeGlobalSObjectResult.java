/**
 * DescribeGlobalSObjectResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeGlobalSObjectResult  implements java.io.Serializable {
    private boolean activateable;

    private boolean createable;

    private boolean custom;

    private boolean customSetting;

    private boolean deletable;

    private boolean deprecatedAndHidden;

    private java.lang.String keyPrefix;

    private java.lang.String label;

    private java.lang.String labelPlural;

    private boolean layoutable;

    private boolean mergeable;

    private java.lang.String name;

    private boolean queryable;

    private boolean replicateable;

    private boolean retrieveable;

    private boolean searchable;

    private boolean triggerable;

    private boolean undeletable;

    private boolean updateable;

    public DescribeGlobalSObjectResult() {
    }

    public DescribeGlobalSObjectResult(
           boolean activateable,
           boolean createable,
           boolean custom,
           boolean customSetting,
           boolean deletable,
           boolean deprecatedAndHidden,
           java.lang.String keyPrefix,
           java.lang.String label,
           java.lang.String labelPlural,
           boolean layoutable,
           boolean mergeable,
           java.lang.String name,
           boolean queryable,
           boolean replicateable,
           boolean retrieveable,
           boolean searchable,
           boolean triggerable,
           boolean undeletable,
           boolean updateable) {
           this.activateable = activateable;
           this.createable = createable;
           this.custom = custom;
           this.customSetting = customSetting;
           this.deletable = deletable;
           this.deprecatedAndHidden = deprecatedAndHidden;
           this.keyPrefix = keyPrefix;
           this.label = label;
           this.labelPlural = labelPlural;
           this.layoutable = layoutable;
           this.mergeable = mergeable;
           this.name = name;
           this.queryable = queryable;
           this.replicateable = replicateable;
           this.retrieveable = retrieveable;
           this.searchable = searchable;
           this.triggerable = triggerable;
           this.undeletable = undeletable;
           this.updateable = updateable;
    }


    /**
     * Gets the activateable value for this DescribeGlobalSObjectResult.
     * 
     * @return activateable
     */
    public boolean isActivateable() {
        return activateable;
    }


    /**
     * Sets the activateable value for this DescribeGlobalSObjectResult.
     * 
     * @param activateable
     */
    public void setActivateable(boolean activateable) {
        this.activateable = activateable;
    }


    /**
     * Gets the createable value for this DescribeGlobalSObjectResult.
     * 
     * @return createable
     */
    public boolean isCreateable() {
        return createable;
    }


    /**
     * Sets the createable value for this DescribeGlobalSObjectResult.
     * 
     * @param createable
     */
    public void setCreateable(boolean createable) {
        this.createable = createable;
    }


    /**
     * Gets the custom value for this DescribeGlobalSObjectResult.
     * 
     * @return custom
     */
    public boolean isCustom() {
        return custom;
    }


    /**
     * Sets the custom value for this DescribeGlobalSObjectResult.
     * 
     * @param custom
     */
    public void setCustom(boolean custom) {
        this.custom = custom;
    }


    /**
     * Gets the customSetting value for this DescribeGlobalSObjectResult.
     * 
     * @return customSetting
     */
    public boolean isCustomSetting() {
        return customSetting;
    }


    /**
     * Sets the customSetting value for this DescribeGlobalSObjectResult.
     * 
     * @param customSetting
     */
    public void setCustomSetting(boolean customSetting) {
        this.customSetting = customSetting;
    }


    /**
     * Gets the deletable value for this DescribeGlobalSObjectResult.
     * 
     * @return deletable
     */
    public boolean isDeletable() {
        return deletable;
    }


    /**
     * Sets the deletable value for this DescribeGlobalSObjectResult.
     * 
     * @param deletable
     */
    public void setDeletable(boolean deletable) {
        this.deletable = deletable;
    }


    /**
     * Gets the deprecatedAndHidden value for this DescribeGlobalSObjectResult.
     * 
     * @return deprecatedAndHidden
     */
    public boolean isDeprecatedAndHidden() {
        return deprecatedAndHidden;
    }


    /**
     * Sets the deprecatedAndHidden value for this DescribeGlobalSObjectResult.
     * 
     * @param deprecatedAndHidden
     */
    public void setDeprecatedAndHidden(boolean deprecatedAndHidden) {
        this.deprecatedAndHidden = deprecatedAndHidden;
    }


    /**
     * Gets the keyPrefix value for this DescribeGlobalSObjectResult.
     * 
     * @return keyPrefix
     */
    public java.lang.String getKeyPrefix() {
        return keyPrefix;
    }


    /**
     * Sets the keyPrefix value for this DescribeGlobalSObjectResult.
     * 
     * @param keyPrefix
     */
    public void setKeyPrefix(java.lang.String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }


    /**
     * Gets the label value for this DescribeGlobalSObjectResult.
     * 
     * @return label
     */
    public java.lang.String getLabel() {
        return label;
    }


    /**
     * Sets the label value for this DescribeGlobalSObjectResult.
     * 
     * @param label
     */
    public void setLabel(java.lang.String label) {
        this.label = label;
    }


    /**
     * Gets the labelPlural value for this DescribeGlobalSObjectResult.
     * 
     * @return labelPlural
     */
    public java.lang.String getLabelPlural() {
        return labelPlural;
    }


    /**
     * Sets the labelPlural value for this DescribeGlobalSObjectResult.
     * 
     * @param labelPlural
     */
    public void setLabelPlural(java.lang.String labelPlural) {
        this.labelPlural = labelPlural;
    }


    /**
     * Gets the layoutable value for this DescribeGlobalSObjectResult.
     * 
     * @return layoutable
     */
    public boolean isLayoutable() {
        return layoutable;
    }


    /**
     * Sets the layoutable value for this DescribeGlobalSObjectResult.
     * 
     * @param layoutable
     */
    public void setLayoutable(boolean layoutable) {
        this.layoutable = layoutable;
    }


    /**
     * Gets the mergeable value for this DescribeGlobalSObjectResult.
     * 
     * @return mergeable
     */
    public boolean isMergeable() {
        return mergeable;
    }


    /**
     * Sets the mergeable value for this DescribeGlobalSObjectResult.
     * 
     * @param mergeable
     */
    public void setMergeable(boolean mergeable) {
        this.mergeable = mergeable;
    }


    /**
     * Gets the name value for this DescribeGlobalSObjectResult.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this DescribeGlobalSObjectResult.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }


    /**
     * Gets the queryable value for this DescribeGlobalSObjectResult.
     * 
     * @return queryable
     */
    public boolean isQueryable() {
        return queryable;
    }


    /**
     * Sets the queryable value for this DescribeGlobalSObjectResult.
     * 
     * @param queryable
     */
    public void setQueryable(boolean queryable) {
        this.queryable = queryable;
    }


    /**
     * Gets the replicateable value for this DescribeGlobalSObjectResult.
     * 
     * @return replicateable
     */
    public boolean isReplicateable() {
        return replicateable;
    }


    /**
     * Sets the replicateable value for this DescribeGlobalSObjectResult.
     * 
     * @param replicateable
     */
    public void setReplicateable(boolean replicateable) {
        this.replicateable = replicateable;
    }


    /**
     * Gets the retrieveable value for this DescribeGlobalSObjectResult.
     * 
     * @return retrieveable
     */
    public boolean isRetrieveable() {
        return retrieveable;
    }


    /**
     * Sets the retrieveable value for this DescribeGlobalSObjectResult.
     * 
     * @param retrieveable
     */
    public void setRetrieveable(boolean retrieveable) {
        this.retrieveable = retrieveable;
    }


    /**
     * Gets the searchable value for this DescribeGlobalSObjectResult.
     * 
     * @return searchable
     */
    public boolean isSearchable() {
        return searchable;
    }


    /**
     * Sets the searchable value for this DescribeGlobalSObjectResult.
     * 
     * @param searchable
     */
    public void setSearchable(boolean searchable) {
        this.searchable = searchable;
    }


    /**
     * Gets the triggerable value for this DescribeGlobalSObjectResult.
     * 
     * @return triggerable
     */
    public boolean isTriggerable() {
        return triggerable;
    }


    /**
     * Sets the triggerable value for this DescribeGlobalSObjectResult.
     * 
     * @param triggerable
     */
    public void setTriggerable(boolean triggerable) {
        this.triggerable = triggerable;
    }


    /**
     * Gets the undeletable value for this DescribeGlobalSObjectResult.
     * 
     * @return undeletable
     */
    public boolean isUndeletable() {
        return undeletable;
    }


    /**
     * Sets the undeletable value for this DescribeGlobalSObjectResult.
     * 
     * @param undeletable
     */
    public void setUndeletable(boolean undeletable) {
        this.undeletable = undeletable;
    }


    /**
     * Gets the updateable value for this DescribeGlobalSObjectResult.
     * 
     * @return updateable
     */
    public boolean isUpdateable() {
        return updateable;
    }


    /**
     * Sets the updateable value for this DescribeGlobalSObjectResult.
     * 
     * @param updateable
     */
    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeGlobalSObjectResult)) return false;
        DescribeGlobalSObjectResult other = (DescribeGlobalSObjectResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.activateable == other.isActivateable() &&
            this.createable == other.isCreateable() &&
            this.custom == other.isCustom() &&
            this.customSetting == other.isCustomSetting() &&
            this.deletable == other.isDeletable() &&
            this.deprecatedAndHidden == other.isDeprecatedAndHidden() &&
            ((this.keyPrefix==null && other.getKeyPrefix()==null) || 
             (this.keyPrefix!=null &&
              this.keyPrefix.equals(other.getKeyPrefix()))) &&
            ((this.label==null && other.getLabel()==null) || 
             (this.label!=null &&
              this.label.equals(other.getLabel()))) &&
            ((this.labelPlural==null && other.getLabelPlural()==null) || 
             (this.labelPlural!=null &&
              this.labelPlural.equals(other.getLabelPlural()))) &&
            this.layoutable == other.isLayoutable() &&
            this.mergeable == other.isMergeable() &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName()))) &&
            this.queryable == other.isQueryable() &&
            this.replicateable == other.isReplicateable() &&
            this.retrieveable == other.isRetrieveable() &&
            this.searchable == other.isSearchable() &&
            this.triggerable == other.isTriggerable() &&
            this.undeletable == other.isUndeletable() &&
            this.updateable == other.isUpdateable();
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        _hashCode += (isActivateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isCreateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isCustom() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isCustomSetting() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isDeletable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isDeprecatedAndHidden() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getKeyPrefix() != null) {
            _hashCode += getKeyPrefix().hashCode();
        }
        if (getLabel() != null) {
            _hashCode += getLabel().hashCode();
        }
        if (getLabelPlural() != null) {
            _hashCode += getLabelPlural().hashCode();
        }
        _hashCode += (isLayoutable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isMergeable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        _hashCode += (isQueryable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isReplicateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isRetrieveable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isSearchable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isTriggerable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isUndeletable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isUpdateable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeGlobalSObjectResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeGlobalSObjectResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("activateable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "activateable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("createable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "createable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("custom");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "custom"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("customSetting");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "customSetting"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("deletable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "deletable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("deprecatedAndHidden");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "deprecatedAndHidden"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("keyPrefix");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "keyPrefix"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("label");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "label"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("labelPlural");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "labelPlural"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layoutable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("mergeable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "mergeable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("queryable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "queryable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("replicateable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "replicateable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("retrieveable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "retrieveable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("searchable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "searchable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("triggerable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "triggerable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("undeletable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "undeletable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("updateable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "updateable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
    }

    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

    /**
     * Get Custom Serializer
     */
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanSerializer(
            _javaType, _xmlType, typeDesc);
    }

    /**
     * Get Custom Deserializer
     */
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanDeserializer(
            _javaType, _xmlType, typeDesc);
    }

}
