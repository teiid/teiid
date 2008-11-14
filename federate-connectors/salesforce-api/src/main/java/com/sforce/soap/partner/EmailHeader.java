/**
 * EmailHeader.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class EmailHeader  implements java.io.Serializable {
    private boolean triggerAutoResponseEmail;

    private boolean triggerOtherEmail;

    private boolean triggerUserEmail;

    public EmailHeader() {
    }

    public EmailHeader(
           boolean triggerAutoResponseEmail,
           boolean triggerOtherEmail,
           boolean triggerUserEmail) {
           this.triggerAutoResponseEmail = triggerAutoResponseEmail;
           this.triggerOtherEmail = triggerOtherEmail;
           this.triggerUserEmail = triggerUserEmail;
    }


    /**
     * Gets the triggerAutoResponseEmail value for this EmailHeader.
     * 
     * @return triggerAutoResponseEmail
     */
    public boolean isTriggerAutoResponseEmail() {
        return triggerAutoResponseEmail;
    }


    /**
     * Sets the triggerAutoResponseEmail value for this EmailHeader.
     * 
     * @param triggerAutoResponseEmail
     */
    public void setTriggerAutoResponseEmail(boolean triggerAutoResponseEmail) {
        this.triggerAutoResponseEmail = triggerAutoResponseEmail;
    }


    /**
     * Gets the triggerOtherEmail value for this EmailHeader.
     * 
     * @return triggerOtherEmail
     */
    public boolean isTriggerOtherEmail() {
        return triggerOtherEmail;
    }


    /**
     * Sets the triggerOtherEmail value for this EmailHeader.
     * 
     * @param triggerOtherEmail
     */
    public void setTriggerOtherEmail(boolean triggerOtherEmail) {
        this.triggerOtherEmail = triggerOtherEmail;
    }


    /**
     * Gets the triggerUserEmail value for this EmailHeader.
     * 
     * @return triggerUserEmail
     */
    public boolean isTriggerUserEmail() {
        return triggerUserEmail;
    }


    /**
     * Sets the triggerUserEmail value for this EmailHeader.
     * 
     * @param triggerUserEmail
     */
    public void setTriggerUserEmail(boolean triggerUserEmail) {
        this.triggerUserEmail = triggerUserEmail;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof EmailHeader)) return false;
        EmailHeader other = (EmailHeader) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.triggerAutoResponseEmail == other.isTriggerAutoResponseEmail() &&
            this.triggerOtherEmail == other.isTriggerOtherEmail() &&
            this.triggerUserEmail == other.isTriggerUserEmail();
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
        _hashCode += (isTriggerAutoResponseEmail() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isTriggerOtherEmail() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isTriggerUserEmail() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(EmailHeader.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">EmailHeader"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("triggerAutoResponseEmail");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "triggerAutoResponseEmail"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("triggerOtherEmail");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "triggerOtherEmail"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("triggerUserEmail");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "triggerUserEmail"));
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
