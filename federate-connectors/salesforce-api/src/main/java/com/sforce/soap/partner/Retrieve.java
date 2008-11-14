/**
 * Retrieve.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class Retrieve  implements java.io.Serializable {
    private java.lang.String fieldList;

    private java.lang.String sObjectType;

    private java.lang.String[] ids;

    public Retrieve() {
    }

    public Retrieve(
           java.lang.String fieldList,
           java.lang.String sObjectType,
           java.lang.String[] ids) {
           this.fieldList = fieldList;
           this.sObjectType = sObjectType;
           this.ids = ids;
    }


    /**
     * Gets the fieldList value for this Retrieve.
     * 
     * @return fieldList
     */
    public java.lang.String getFieldList() {
        return fieldList;
    }


    /**
     * Sets the fieldList value for this Retrieve.
     * 
     * @param fieldList
     */
    public void setFieldList(java.lang.String fieldList) {
        this.fieldList = fieldList;
    }


    /**
     * Gets the sObjectType value for this Retrieve.
     * 
     * @return sObjectType
     */
    public java.lang.String getSObjectType() {
        return sObjectType;
    }


    /**
     * Sets the sObjectType value for this Retrieve.
     * 
     * @param sObjectType
     */
    public void setSObjectType(java.lang.String sObjectType) {
        this.sObjectType = sObjectType;
    }


    /**
     * Gets the ids value for this Retrieve.
     * 
     * @return ids
     */
    public java.lang.String[] getIds() {
        return ids;
    }


    /**
     * Sets the ids value for this Retrieve.
     * 
     * @param ids
     */
    public void setIds(java.lang.String[] ids) {
        this.ids = ids;
    }

    public java.lang.String getIds(int i) {
        return this.ids[i];
    }

    public void setIds(int i, java.lang.String _value) {
        this.ids[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof Retrieve)) return false;
        Retrieve other = (Retrieve) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.fieldList==null && other.getFieldList()==null) || 
             (this.fieldList!=null &&
              this.fieldList.equals(other.getFieldList()))) &&
            ((this.sObjectType==null && other.getSObjectType()==null) || 
             (this.sObjectType!=null &&
              this.sObjectType.equals(other.getSObjectType()))) &&
            ((this.ids==null && other.getIds()==null) || 
             (this.ids!=null &&
              java.util.Arrays.equals(this.ids, other.getIds())));
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
        if (getFieldList() != null) {
            _hashCode += getFieldList().hashCode();
        }
        if (getSObjectType() != null) {
            _hashCode += getSObjectType().hashCode();
        }
        if (getIds() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getIds());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getIds(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(Retrieve.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">retrieve"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fieldList");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "fieldList"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SObjectType");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sObjectType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ids");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ids"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
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
