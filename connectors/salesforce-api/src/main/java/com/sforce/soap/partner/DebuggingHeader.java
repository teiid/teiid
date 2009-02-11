/**
 * DebuggingHeader.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DebuggingHeader  implements java.io.Serializable {
    private com.sforce.soap.partner.DebugLevel debugLevel;

    public DebuggingHeader() {
    }

    public DebuggingHeader(
           com.sforce.soap.partner.DebugLevel debugLevel) {
           this.debugLevel = debugLevel;
    }


    /**
     * Gets the debugLevel value for this DebuggingHeader.
     * 
     * @return debugLevel
     */
    public com.sforce.soap.partner.DebugLevel getDebugLevel() {
        return debugLevel;
    }


    /**
     * Sets the debugLevel value for this DebuggingHeader.
     * 
     * @param debugLevel
     */
    public void setDebugLevel(com.sforce.soap.partner.DebugLevel debugLevel) {
        this.debugLevel = debugLevel;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DebuggingHeader)) return false;
        DebuggingHeader other = (DebuggingHeader) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.debugLevel==null && other.getDebugLevel()==null) || 
             (this.debugLevel!=null &&
              this.debugLevel.equals(other.getDebugLevel())));
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
        if (getDebugLevel() != null) {
            _hashCode += getDebugLevel().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DebuggingHeader.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">DebuggingHeader"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("debugLevel");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "debugLevel"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DebugLevel"));
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
