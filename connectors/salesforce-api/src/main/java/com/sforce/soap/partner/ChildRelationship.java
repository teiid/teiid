/**
 * ChildRelationship.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class ChildRelationship  implements java.io.Serializable {
    private boolean cascadeDelete;

    private java.lang.String childSObject;

    private java.lang.String field;

    private java.lang.String relationshipName;

    public ChildRelationship() {
    }

    public ChildRelationship(
           boolean cascadeDelete,
           java.lang.String childSObject,
           java.lang.String field,
           java.lang.String relationshipName) {
           this.cascadeDelete = cascadeDelete;
           this.childSObject = childSObject;
           this.field = field;
           this.relationshipName = relationshipName;
    }


    /**
     * Gets the cascadeDelete value for this ChildRelationship.
     * 
     * @return cascadeDelete
     */
    public boolean isCascadeDelete() {
        return cascadeDelete;
    }


    /**
     * Sets the cascadeDelete value for this ChildRelationship.
     * 
     * @param cascadeDelete
     */
    public void setCascadeDelete(boolean cascadeDelete) {
        this.cascadeDelete = cascadeDelete;
    }


    /**
     * Gets the childSObject value for this ChildRelationship.
     * 
     * @return childSObject
     */
    public java.lang.String getChildSObject() {
        return childSObject;
    }


    /**
     * Sets the childSObject value for this ChildRelationship.
     * 
     * @param childSObject
     */
    public void setChildSObject(java.lang.String childSObject) {
        this.childSObject = childSObject;
    }


    /**
     * Gets the field value for this ChildRelationship.
     * 
     * @return field
     */
    public java.lang.String getField() {
        return field;
    }


    /**
     * Sets the field value for this ChildRelationship.
     * 
     * @param field
     */
    public void setField(java.lang.String field) {
        this.field = field;
    }


    /**
     * Gets the relationshipName value for this ChildRelationship.
     * 
     * @return relationshipName
     */
    public java.lang.String getRelationshipName() {
        return relationshipName;
    }


    /**
     * Sets the relationshipName value for this ChildRelationship.
     * 
     * @param relationshipName
     */
    public void setRelationshipName(java.lang.String relationshipName) {
        this.relationshipName = relationshipName;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ChildRelationship)) return false;
        ChildRelationship other = (ChildRelationship) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.cascadeDelete == other.isCascadeDelete() &&
            ((this.childSObject==null && other.getChildSObject()==null) || 
             (this.childSObject!=null &&
              this.childSObject.equals(other.getChildSObject()))) &&
            ((this.field==null && other.getField()==null) || 
             (this.field!=null &&
              this.field.equals(other.getField()))) &&
            ((this.relationshipName==null && other.getRelationshipName()==null) || 
             (this.relationshipName!=null &&
              this.relationshipName.equals(other.getRelationshipName())));
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
        _hashCode += (isCascadeDelete() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getChildSObject() != null) {
            _hashCode += getChildSObject().hashCode();
        }
        if (getField() != null) {
            _hashCode += getField().hashCode();
        }
        if (getRelationshipName() != null) {
            _hashCode += getRelationshipName().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(ChildRelationship.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ChildRelationship"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("cascadeDelete");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "cascadeDelete"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("childSObject");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "childSObject"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("field");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "field"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("relationshipName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "relationshipName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
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
