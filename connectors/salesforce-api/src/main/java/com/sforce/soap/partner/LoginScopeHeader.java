/**
 * LoginScopeHeader.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class LoginScopeHeader  implements java.io.Serializable {
    private java.lang.String organizationId;

    private java.lang.String portalId;

    public LoginScopeHeader() {
    }

    public LoginScopeHeader(
           java.lang.String organizationId,
           java.lang.String portalId) {
           this.organizationId = organizationId;
           this.portalId = portalId;
    }


    /**
     * Gets the organizationId value for this LoginScopeHeader.
     * 
     * @return organizationId
     */
    public java.lang.String getOrganizationId() {
        return organizationId;
    }


    /**
     * Sets the organizationId value for this LoginScopeHeader.
     * 
     * @param organizationId
     */
    public void setOrganizationId(java.lang.String organizationId) {
        this.organizationId = organizationId;
    }


    /**
     * Gets the portalId value for this LoginScopeHeader.
     * 
     * @return portalId
     */
    public java.lang.String getPortalId() {
        return portalId;
    }


    /**
     * Sets the portalId value for this LoginScopeHeader.
     * 
     * @param portalId
     */
    public void setPortalId(java.lang.String portalId) {
        this.portalId = portalId;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof LoginScopeHeader)) return false;
        LoginScopeHeader other = (LoginScopeHeader) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.organizationId==null && other.getOrganizationId()==null) || 
             (this.organizationId!=null &&
              this.organizationId.equals(other.getOrganizationId()))) &&
            ((this.portalId==null && other.getPortalId()==null) || 
             (this.portalId!=null &&
              this.portalId.equals(other.getPortalId())));
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
        if (getOrganizationId() != null) {
            _hashCode += getOrganizationId().hashCode();
        }
        if (getPortalId() != null) {
            _hashCode += getPortalId().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(LoginScopeHeader.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">LoginScopeHeader"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("organizationId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "organizationId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("portalId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "portalId"));
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
