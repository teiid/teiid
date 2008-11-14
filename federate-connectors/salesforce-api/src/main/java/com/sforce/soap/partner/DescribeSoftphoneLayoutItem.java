/**
 * DescribeSoftphoneLayoutItem.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeSoftphoneLayoutItem  implements java.io.Serializable {
    private java.lang.String itemApiName;

    public DescribeSoftphoneLayoutItem() {
    }

    public DescribeSoftphoneLayoutItem(
           java.lang.String itemApiName) {
           this.itemApiName = itemApiName;
    }


    /**
     * Gets the itemApiName value for this DescribeSoftphoneLayoutItem.
     * 
     * @return itemApiName
     */
    public java.lang.String getItemApiName() {
        return itemApiName;
    }


    /**
     * Sets the itemApiName value for this DescribeSoftphoneLayoutItem.
     * 
     * @param itemApiName
     */
    public void setItemApiName(java.lang.String itemApiName) {
        this.itemApiName = itemApiName;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeSoftphoneLayoutItem)) return false;
        DescribeSoftphoneLayoutItem other = (DescribeSoftphoneLayoutItem) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.itemApiName==null && other.getItemApiName()==null) || 
             (this.itemApiName!=null &&
              this.itemApiName.equals(other.getItemApiName())));
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
        if (getItemApiName() != null) {
            _hashCode += getItemApiName().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeSoftphoneLayoutItem.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutItem"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("itemApiName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "itemApiName"));
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
