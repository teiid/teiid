/**
 * Upsert.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class Upsert  implements java.io.Serializable {
    private java.lang.String externalIDFieldName;

    private com.sforce.soap.partner.sobject.SObject[] sObjects;

    public Upsert() {
    }

    public Upsert(
           java.lang.String externalIDFieldName,
           com.sforce.soap.partner.sobject.SObject[] sObjects) {
           this.externalIDFieldName = externalIDFieldName;
           this.sObjects = sObjects;
    }


    /**
     * Gets the externalIDFieldName value for this Upsert.
     * 
     * @return externalIDFieldName
     */
    public java.lang.String getExternalIDFieldName() {
        return externalIDFieldName;
    }


    /**
     * Sets the externalIDFieldName value for this Upsert.
     * 
     * @param externalIDFieldName
     */
    public void setExternalIDFieldName(java.lang.String externalIDFieldName) {
        this.externalIDFieldName = externalIDFieldName;
    }


    /**
     * Gets the sObjects value for this Upsert.
     * 
     * @return sObjects
     */
    public com.sforce.soap.partner.sobject.SObject[] getSObjects() {
        return sObjects;
    }


    /**
     * Sets the sObjects value for this Upsert.
     * 
     * @param sObjects
     */
    public void setSObjects(com.sforce.soap.partner.sobject.SObject[] sObjects) {
        this.sObjects = sObjects;
    }

    public com.sforce.soap.partner.sobject.SObject getSObjects(int i) {
        return this.sObjects[i];
    }

    public void setSObjects(int i, com.sforce.soap.partner.sobject.SObject _value) {
        this.sObjects[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof Upsert)) return false;
        Upsert other = (Upsert) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.externalIDFieldName==null && other.getExternalIDFieldName()==null) || 
             (this.externalIDFieldName!=null &&
              this.externalIDFieldName.equals(other.getExternalIDFieldName()))) &&
            ((this.sObjects==null && other.getSObjects()==null) || 
             (this.sObjects!=null &&
              java.util.Arrays.equals(this.sObjects, other.getSObjects())));
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
        if (getExternalIDFieldName() != null) {
            _hashCode += getExternalIDFieldName().hashCode();
        }
        if (getSObjects() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getSObjects());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getSObjects(), i);
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
        new org.apache.axis.description.TypeDesc(Upsert.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">upsert"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("externalIDFieldName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "externalIDFieldName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SObjects");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sObjects"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:sobject.partner.soap.sforce.com", "sObject"));
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
