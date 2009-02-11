/**
 * DescribeSoftphoneLayoutResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeSoftphoneLayoutResult  implements java.io.Serializable {
    private com.sforce.soap.partner.DescribeSoftphoneLayoutCallType[] callTypes;

    private java.lang.String id;

    private java.lang.String name;

    public DescribeSoftphoneLayoutResult() {
    }

    public DescribeSoftphoneLayoutResult(
           com.sforce.soap.partner.DescribeSoftphoneLayoutCallType[] callTypes,
           java.lang.String id,
           java.lang.String name) {
           this.callTypes = callTypes;
           this.id = id;
           this.name = name;
    }


    /**
     * Gets the callTypes value for this DescribeSoftphoneLayoutResult.
     * 
     * @return callTypes
     */
    public com.sforce.soap.partner.DescribeSoftphoneLayoutCallType[] getCallTypes() {
        return callTypes;
    }


    /**
     * Sets the callTypes value for this DescribeSoftphoneLayoutResult.
     * 
     * @param callTypes
     */
    public void setCallTypes(com.sforce.soap.partner.DescribeSoftphoneLayoutCallType[] callTypes) {
        this.callTypes = callTypes;
    }

    public com.sforce.soap.partner.DescribeSoftphoneLayoutCallType getCallTypes(int i) {
        return this.callTypes[i];
    }

    public void setCallTypes(int i, com.sforce.soap.partner.DescribeSoftphoneLayoutCallType _value) {
        this.callTypes[i] = _value;
    }


    /**
     * Gets the id value for this DescribeSoftphoneLayoutResult.
     * 
     * @return id
     */
    public java.lang.String getId() {
        return id;
    }


    /**
     * Sets the id value for this DescribeSoftphoneLayoutResult.
     * 
     * @param id
     */
    public void setId(java.lang.String id) {
        this.id = id;
    }


    /**
     * Gets the name value for this DescribeSoftphoneLayoutResult.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this DescribeSoftphoneLayoutResult.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeSoftphoneLayoutResult)) return false;
        DescribeSoftphoneLayoutResult other = (DescribeSoftphoneLayoutResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.callTypes==null && other.getCallTypes()==null) || 
             (this.callTypes!=null &&
              java.util.Arrays.equals(this.callTypes, other.getCallTypes()))) &&
            ((this.id==null && other.getId()==null) || 
             (this.id!=null &&
              this.id.equals(other.getId()))) &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName())));
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
        if (getCallTypes() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getCallTypes());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getCallTypes(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getId() != null) {
            _hashCode += getId().hashCode();
        }
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeSoftphoneLayoutResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("callTypes");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "callTypes"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutCallType"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("id");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "id"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
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
