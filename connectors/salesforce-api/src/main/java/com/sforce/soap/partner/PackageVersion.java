/**
 * PackageVersion.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class PackageVersion  implements java.io.Serializable {
    private int majorNumber;

    private int minorNumber;

    private java.lang.String namespace;

    public PackageVersion() {
    }

    public PackageVersion(
           int majorNumber,
           int minorNumber,
           java.lang.String namespace) {
           this.majorNumber = majorNumber;
           this.minorNumber = minorNumber;
           this.namespace = namespace;
    }


    /**
     * Gets the majorNumber value for this PackageVersion.
     * 
     * @return majorNumber
     */
    public int getMajorNumber() {
        return majorNumber;
    }


    /**
     * Sets the majorNumber value for this PackageVersion.
     * 
     * @param majorNumber
     */
    public void setMajorNumber(int majorNumber) {
        this.majorNumber = majorNumber;
    }


    /**
     * Gets the minorNumber value for this PackageVersion.
     * 
     * @return minorNumber
     */
    public int getMinorNumber() {
        return minorNumber;
    }


    /**
     * Sets the minorNumber value for this PackageVersion.
     * 
     * @param minorNumber
     */
    public void setMinorNumber(int minorNumber) {
        this.minorNumber = minorNumber;
    }


    /**
     * Gets the namespace value for this PackageVersion.
     * 
     * @return namespace
     */
    public java.lang.String getNamespace() {
        return namespace;
    }


    /**
     * Sets the namespace value for this PackageVersion.
     * 
     * @param namespace
     */
    public void setNamespace(java.lang.String namespace) {
        this.namespace = namespace;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof PackageVersion)) return false;
        PackageVersion other = (PackageVersion) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.majorNumber == other.getMajorNumber() &&
            this.minorNumber == other.getMinorNumber() &&
            ((this.namespace==null && other.getNamespace()==null) || 
             (this.namespace!=null &&
              this.namespace.equals(other.getNamespace())));
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
        _hashCode += getMajorNumber();
        _hashCode += getMinorNumber();
        if (getNamespace() != null) {
            _hashCode += getNamespace().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(PackageVersion.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "PackageVersion"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("majorNumber");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "majorNumber"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("minorNumber");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "minorNumber"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("namespace");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "namespace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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
