/**
 * DescribeSoftphoneLayoutSection.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeSoftphoneLayoutSection  implements java.io.Serializable {
    private java.lang.String entityApiName;

    private com.sforce.soap.partner.DescribeSoftphoneLayoutItem[] items;

    public DescribeSoftphoneLayoutSection() {
    }

    public DescribeSoftphoneLayoutSection(
           java.lang.String entityApiName,
           com.sforce.soap.partner.DescribeSoftphoneLayoutItem[] items) {
           this.entityApiName = entityApiName;
           this.items = items;
    }


    /**
     * Gets the entityApiName value for this DescribeSoftphoneLayoutSection.
     * 
     * @return entityApiName
     */
    public java.lang.String getEntityApiName() {
        return entityApiName;
    }


    /**
     * Sets the entityApiName value for this DescribeSoftphoneLayoutSection.
     * 
     * @param entityApiName
     */
    public void setEntityApiName(java.lang.String entityApiName) {
        this.entityApiName = entityApiName;
    }


    /**
     * Gets the items value for this DescribeSoftphoneLayoutSection.
     * 
     * @return items
     */
    public com.sforce.soap.partner.DescribeSoftphoneLayoutItem[] getItems() {
        return items;
    }


    /**
     * Sets the items value for this DescribeSoftphoneLayoutSection.
     * 
     * @param items
     */
    public void setItems(com.sforce.soap.partner.DescribeSoftphoneLayoutItem[] items) {
        this.items = items;
    }

    public com.sforce.soap.partner.DescribeSoftphoneLayoutItem getItems(int i) {
        return this.items[i];
    }

    public void setItems(int i, com.sforce.soap.partner.DescribeSoftphoneLayoutItem _value) {
        this.items[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeSoftphoneLayoutSection)) return false;
        DescribeSoftphoneLayoutSection other = (DescribeSoftphoneLayoutSection) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.entityApiName==null && other.getEntityApiName()==null) || 
             (this.entityApiName!=null &&
              this.entityApiName.equals(other.getEntityApiName()))) &&
            ((this.items==null && other.getItems()==null) || 
             (this.items!=null &&
              java.util.Arrays.equals(this.items, other.getItems())));
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
        if (getEntityApiName() != null) {
            _hashCode += getEntityApiName().hashCode();
        }
        if (getItems() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getItems());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getItems(), i);
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
        new org.apache.axis.description.TypeDesc(DescribeSoftphoneLayoutSection.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutSection"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("entityApiName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "entityApiName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("items");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "items"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutItem"));
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
