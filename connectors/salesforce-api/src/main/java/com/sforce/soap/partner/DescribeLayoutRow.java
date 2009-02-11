/**
 * DescribeLayoutRow.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutRow  implements java.io.Serializable {
    private com.sforce.soap.partner.DescribeLayoutItem[] layoutItems;

    private int numItems;

    public DescribeLayoutRow() {
    }

    public DescribeLayoutRow(
           com.sforce.soap.partner.DescribeLayoutItem[] layoutItems,
           int numItems) {
           this.layoutItems = layoutItems;
           this.numItems = numItems;
    }


    /**
     * Gets the layoutItems value for this DescribeLayoutRow.
     * 
     * @return layoutItems
     */
    public com.sforce.soap.partner.DescribeLayoutItem[] getLayoutItems() {
        return layoutItems;
    }


    /**
     * Sets the layoutItems value for this DescribeLayoutRow.
     * 
     * @param layoutItems
     */
    public void setLayoutItems(com.sforce.soap.partner.DescribeLayoutItem[] layoutItems) {
        this.layoutItems = layoutItems;
    }

    public com.sforce.soap.partner.DescribeLayoutItem getLayoutItems(int i) {
        return this.layoutItems[i];
    }

    public void setLayoutItems(int i, com.sforce.soap.partner.DescribeLayoutItem _value) {
        this.layoutItems[i] = _value;
    }


    /**
     * Gets the numItems value for this DescribeLayoutRow.
     * 
     * @return numItems
     */
    public int getNumItems() {
        return numItems;
    }


    /**
     * Sets the numItems value for this DescribeLayoutRow.
     * 
     * @param numItems
     */
    public void setNumItems(int numItems) {
        this.numItems = numItems;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutRow)) return false;
        DescribeLayoutRow other = (DescribeLayoutRow) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.layoutItems==null && other.getLayoutItems()==null) || 
             (this.layoutItems!=null &&
              java.util.Arrays.equals(this.layoutItems, other.getLayoutItems()))) &&
            this.numItems == other.getNumItems();
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
        if (getLayoutItems() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getLayoutItems());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getLayoutItems(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += getNumItems();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeLayoutRow.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutRow"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layoutItems");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutItems"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutItem"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numItems");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "numItems"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
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
