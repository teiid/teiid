/**
 * AssignmentRuleHeader.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class AssignmentRuleHeader  implements java.io.Serializable {
    private java.lang.String assignmentRuleId;

    private java.lang.Boolean useDefaultRule;

    public AssignmentRuleHeader() {
    }

    public AssignmentRuleHeader(
           java.lang.String assignmentRuleId,
           java.lang.Boolean useDefaultRule) {
           this.assignmentRuleId = assignmentRuleId;
           this.useDefaultRule = useDefaultRule;
    }


    /**
     * Gets the assignmentRuleId value for this AssignmentRuleHeader.
     * 
     * @return assignmentRuleId
     */
    public java.lang.String getAssignmentRuleId() {
        return assignmentRuleId;
    }


    /**
     * Sets the assignmentRuleId value for this AssignmentRuleHeader.
     * 
     * @param assignmentRuleId
     */
    public void setAssignmentRuleId(java.lang.String assignmentRuleId) {
        this.assignmentRuleId = assignmentRuleId;
    }


    /**
     * Gets the useDefaultRule value for this AssignmentRuleHeader.
     * 
     * @return useDefaultRule
     */
    public java.lang.Boolean getUseDefaultRule() {
        return useDefaultRule;
    }


    /**
     * Sets the useDefaultRule value for this AssignmentRuleHeader.
     * 
     * @param useDefaultRule
     */
    public void setUseDefaultRule(java.lang.Boolean useDefaultRule) {
        this.useDefaultRule = useDefaultRule;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof AssignmentRuleHeader)) return false;
        AssignmentRuleHeader other = (AssignmentRuleHeader) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.assignmentRuleId==null && other.getAssignmentRuleId()==null) || 
             (this.assignmentRuleId!=null &&
              this.assignmentRuleId.equals(other.getAssignmentRuleId()))) &&
            ((this.useDefaultRule==null && other.getUseDefaultRule()==null) || 
             (this.useDefaultRule!=null &&
              this.useDefaultRule.equals(other.getUseDefaultRule())));
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
        if (getAssignmentRuleId() != null) {
            _hashCode += getAssignmentRuleId().hashCode();
        }
        if (getUseDefaultRule() != null) {
            _hashCode += getUseDefaultRule().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(AssignmentRuleHeader.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", ">AssignmentRuleHeader"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("assignmentRuleId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "assignmentRuleId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("useDefaultRule");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "useDefaultRule"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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
