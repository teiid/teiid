/**
 * DescribeLayoutComponent.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutComponent  implements java.io.Serializable {
    private int displayLines;

    private int tabOrder;

    private com.sforce.soap.partner.LayoutComponentType type;

    private java.lang.String value;

    public DescribeLayoutComponent() {
    }

    public DescribeLayoutComponent(
           int displayLines,
           int tabOrder,
           com.sforce.soap.partner.LayoutComponentType type,
           java.lang.String value) {
           this.displayLines = displayLines;
           this.tabOrder = tabOrder;
           this.type = type;
           this.value = value;
    }


    /**
     * Gets the displayLines value for this DescribeLayoutComponent.
     * 
     * @return displayLines
     */
    public int getDisplayLines() {
        return displayLines;
    }


    /**
     * Sets the displayLines value for this DescribeLayoutComponent.
     * 
     * @param displayLines
     */
    public void setDisplayLines(int displayLines) {
        this.displayLines = displayLines;
    }


    /**
     * Gets the tabOrder value for this DescribeLayoutComponent.
     * 
     * @return tabOrder
     */
    public int getTabOrder() {
        return tabOrder;
    }


    /**
     * Sets the tabOrder value for this DescribeLayoutComponent.
     * 
     * @param tabOrder
     */
    public void setTabOrder(int tabOrder) {
        this.tabOrder = tabOrder;
    }


    /**
     * Gets the type value for this DescribeLayoutComponent.
     * 
     * @return type
     */
    public com.sforce.soap.partner.LayoutComponentType getType() {
        return type;
    }


    /**
     * Sets the type value for this DescribeLayoutComponent.
     * 
     * @param type
     */
    public void setType(com.sforce.soap.partner.LayoutComponentType type) {
        this.type = type;
    }


    /**
     * Gets the value value for this DescribeLayoutComponent.
     * 
     * @return value
     */
    public java.lang.String getValue() {
        return value;
    }


    /**
     * Sets the value value for this DescribeLayoutComponent.
     * 
     * @param value
     */
    public void setValue(java.lang.String value) {
        this.value = value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutComponent)) return false;
        DescribeLayoutComponent other = (DescribeLayoutComponent) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.displayLines == other.getDisplayLines() &&
            this.tabOrder == other.getTabOrder() &&
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            ((this.value==null && other.getValue()==null) || 
             (this.value!=null &&
              this.value.equals(other.getValue())));
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
        _hashCode += getDisplayLines();
        _hashCode += getTabOrder();
        if (getType() != null) {
            _hashCode += getType().hashCode();
        }
        if (getValue() != null) {
            _hashCode += getValue().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeLayoutComponent.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutComponent"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("displayLines");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "displayLines"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("tabOrder");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "tabOrder"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutComponentType"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("value");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "value"));
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
