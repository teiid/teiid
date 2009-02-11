/**
 * DescribeLayoutType0.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutType0  implements java.io.Serializable {
    private java.lang.String sObjectType;

    private java.lang.String[] recordTypeIds;

    public DescribeLayoutType0() {
    }

    public DescribeLayoutType0(
           java.lang.String sObjectType,
           java.lang.String[] recordTypeIds) {
           this.sObjectType = sObjectType;
           this.recordTypeIds = recordTypeIds;
    }


    /**
     * Gets the sObjectType value for this DescribeLayoutType0.
     * 
     * @return sObjectType
     */
    public java.lang.String getSObjectType() {
        return sObjectType;
    }


    /**
     * Sets the sObjectType value for this DescribeLayoutType0.
     * 
     * @param sObjectType
     */
    public void setSObjectType(java.lang.String sObjectType) {
        this.sObjectType = sObjectType;
    }


    /**
     * Gets the recordTypeIds value for this DescribeLayoutType0.
     * 
     * @return recordTypeIds
     */
    public java.lang.String[] getRecordTypeIds() {
        return recordTypeIds;
    }


    /**
     * Sets the recordTypeIds value for this DescribeLayoutType0.
     * 
     * @param recordTypeIds
     */
    public void setRecordTypeIds(java.lang.String[] recordTypeIds) {
        this.recordTypeIds = recordTypeIds;
    }

    public java.lang.String getRecordTypeIds(int i) {
        return this.recordTypeIds[i];
    }

    public void setRecordTypeIds(int i, java.lang.String _value) {
        this.recordTypeIds[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutType0)) return false;
        DescribeLayoutType0 other = (DescribeLayoutType0) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.sObjectType==null && other.getSObjectType()==null) || 
             (this.sObjectType!=null &&
              this.sObjectType.equals(other.getSObjectType()))) &&
            ((this.recordTypeIds==null && other.getRecordTypeIds()==null) || 
             (this.recordTypeIds!=null &&
              java.util.Arrays.equals(this.recordTypeIds, other.getRecordTypeIds())));
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
        if (getSObjectType() != null) {
            _hashCode += getSObjectType().hashCode();
        }
        if (getRecordTypeIds() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getRecordTypeIds());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getRecordTypeIds(), i);
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
        new org.apache.axis.description.TypeDesc(DescribeLayoutType0.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">describeLayout"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SObjectType");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sObjectType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("recordTypeIds");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "recordTypeIds"));
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
