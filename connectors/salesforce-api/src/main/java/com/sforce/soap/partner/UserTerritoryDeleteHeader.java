/**
 * UserTerritoryDeleteHeader.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class UserTerritoryDeleteHeader  implements java.io.Serializable {
    private java.lang.String transferToUserId;

    public UserTerritoryDeleteHeader() {
    }

    public UserTerritoryDeleteHeader(
           java.lang.String transferToUserId) {
           this.transferToUserId = transferToUserId;
    }


    /**
     * Gets the transferToUserId value for this UserTerritoryDeleteHeader.
     * 
     * @return transferToUserId
     */
    public java.lang.String getTransferToUserId() {
        return transferToUserId;
    }


    /**
     * Sets the transferToUserId value for this UserTerritoryDeleteHeader.
     * 
     * @param transferToUserId
     */
    public void setTransferToUserId(java.lang.String transferToUserId) {
        this.transferToUserId = transferToUserId;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof UserTerritoryDeleteHeader)) return false;
        UserTerritoryDeleteHeader other = (UserTerritoryDeleteHeader) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.transferToUserId==null && other.getTransferToUserId()==null) || 
             (this.transferToUserId!=null &&
              this.transferToUserId.equals(other.getTransferToUserId())));
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
        if (getTransferToUserId() != null) {
            _hashCode += getTransferToUserId().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(UserTerritoryDeleteHeader.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">UserTerritoryDeleteHeader"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferToUserId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "transferToUserId"));
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
