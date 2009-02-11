/**
 * RecordTypeMapping.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class RecordTypeMapping  implements java.io.Serializable {
    private boolean available;

    private boolean defaultRecordTypeMapping;

    private java.lang.String layoutId;

    private java.lang.String name;

    private com.sforce.soap.partner.PicklistForRecordType[] picklistsForRecordType;

    private java.lang.String recordTypeId;

    public RecordTypeMapping() {
    }

    public RecordTypeMapping(
           boolean available,
           boolean defaultRecordTypeMapping,
           java.lang.String layoutId,
           java.lang.String name,
           com.sforce.soap.partner.PicklistForRecordType[] picklistsForRecordType,
           java.lang.String recordTypeId) {
           this.available = available;
           this.defaultRecordTypeMapping = defaultRecordTypeMapping;
           this.layoutId = layoutId;
           this.name = name;
           this.picklistsForRecordType = picklistsForRecordType;
           this.recordTypeId = recordTypeId;
    }


    /**
     * Gets the available value for this RecordTypeMapping.
     * 
     * @return available
     */
    public boolean isAvailable() {
        return available;
    }


    /**
     * Sets the available value for this RecordTypeMapping.
     * 
     * @param available
     */
    public void setAvailable(boolean available) {
        this.available = available;
    }


    /**
     * Gets the defaultRecordTypeMapping value for this RecordTypeMapping.
     * 
     * @return defaultRecordTypeMapping
     */
    public boolean isDefaultRecordTypeMapping() {
        return defaultRecordTypeMapping;
    }


    /**
     * Sets the defaultRecordTypeMapping value for this RecordTypeMapping.
     * 
     * @param defaultRecordTypeMapping
     */
    public void setDefaultRecordTypeMapping(boolean defaultRecordTypeMapping) {
        this.defaultRecordTypeMapping = defaultRecordTypeMapping;
    }


    /**
     * Gets the layoutId value for this RecordTypeMapping.
     * 
     * @return layoutId
     */
    public java.lang.String getLayoutId() {
        return layoutId;
    }


    /**
     * Sets the layoutId value for this RecordTypeMapping.
     * 
     * @param layoutId
     */
    public void setLayoutId(java.lang.String layoutId) {
        this.layoutId = layoutId;
    }


    /**
     * Gets the name value for this RecordTypeMapping.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this RecordTypeMapping.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }


    /**
     * Gets the picklistsForRecordType value for this RecordTypeMapping.
     * 
     * @return picklistsForRecordType
     */
    public com.sforce.soap.partner.PicklistForRecordType[] getPicklistsForRecordType() {
        return picklistsForRecordType;
    }


    /**
     * Sets the picklistsForRecordType value for this RecordTypeMapping.
     * 
     * @param picklistsForRecordType
     */
    public void setPicklistsForRecordType(com.sforce.soap.partner.PicklistForRecordType[] picklistsForRecordType) {
        this.picklistsForRecordType = picklistsForRecordType;
    }

    public com.sforce.soap.partner.PicklistForRecordType getPicklistsForRecordType(int i) {
        return this.picklistsForRecordType[i];
    }

    public void setPicklistsForRecordType(int i, com.sforce.soap.partner.PicklistForRecordType _value) {
        this.picklistsForRecordType[i] = _value;
    }


    /**
     * Gets the recordTypeId value for this RecordTypeMapping.
     * 
     * @return recordTypeId
     */
    public java.lang.String getRecordTypeId() {
        return recordTypeId;
    }


    /**
     * Sets the recordTypeId value for this RecordTypeMapping.
     * 
     * @param recordTypeId
     */
    public void setRecordTypeId(java.lang.String recordTypeId) {
        this.recordTypeId = recordTypeId;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RecordTypeMapping)) return false;
        RecordTypeMapping other = (RecordTypeMapping) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.available == other.isAvailable() &&
            this.defaultRecordTypeMapping == other.isDefaultRecordTypeMapping() &&
            ((this.layoutId==null && other.getLayoutId()==null) || 
             (this.layoutId!=null &&
              this.layoutId.equals(other.getLayoutId()))) &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName()))) &&
            ((this.picklistsForRecordType==null && other.getPicklistsForRecordType()==null) || 
             (this.picklistsForRecordType!=null &&
              java.util.Arrays.equals(this.picklistsForRecordType, other.getPicklistsForRecordType()))) &&
            ((this.recordTypeId==null && other.getRecordTypeId()==null) || 
             (this.recordTypeId!=null &&
              this.recordTypeId.equals(other.getRecordTypeId())));
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
        _hashCode += (isAvailable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isDefaultRecordTypeMapping() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getLayoutId() != null) {
            _hashCode += getLayoutId().hashCode();
        }
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        if (getPicklistsForRecordType() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getPicklistsForRecordType());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getPicklistsForRecordType(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getRecordTypeId() != null) {
            _hashCode += getRecordTypeId().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RecordTypeMapping.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "RecordTypeMapping"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("available");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "available"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("defaultRecordTypeMapping");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "defaultRecordTypeMapping"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layoutId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("picklistsForRecordType");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "picklistsForRecordType"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "PicklistForRecordType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("recordTypeId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "recordTypeId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
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
