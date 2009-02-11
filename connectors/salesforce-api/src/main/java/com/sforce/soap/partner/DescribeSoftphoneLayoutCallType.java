/**
 * DescribeSoftphoneLayoutCallType.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeSoftphoneLayoutCallType  implements java.io.Serializable {
    private com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField[] infoFields;

    private java.lang.String name;

    private com.sforce.soap.partner.DescribeSoftphoneLayoutSection[] sections;

    public DescribeSoftphoneLayoutCallType() {
    }

    public DescribeSoftphoneLayoutCallType(
           com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField[] infoFields,
           java.lang.String name,
           com.sforce.soap.partner.DescribeSoftphoneLayoutSection[] sections) {
           this.infoFields = infoFields;
           this.name = name;
           this.sections = sections;
    }


    /**
     * Gets the infoFields value for this DescribeSoftphoneLayoutCallType.
     * 
     * @return infoFields
     */
    public com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField[] getInfoFields() {
        return infoFields;
    }


    /**
     * Sets the infoFields value for this DescribeSoftphoneLayoutCallType.
     * 
     * @param infoFields
     */
    public void setInfoFields(com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField[] infoFields) {
        this.infoFields = infoFields;
    }

    public com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField getInfoFields(int i) {
        return this.infoFields[i];
    }

    public void setInfoFields(int i, com.sforce.soap.partner.DescribeSoftphoneLayoutInfoField _value) {
        this.infoFields[i] = _value;
    }


    /**
     * Gets the name value for this DescribeSoftphoneLayoutCallType.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this DescribeSoftphoneLayoutCallType.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }


    /**
     * Gets the sections value for this DescribeSoftphoneLayoutCallType.
     * 
     * @return sections
     */
    public com.sforce.soap.partner.DescribeSoftphoneLayoutSection[] getSections() {
        return sections;
    }


    /**
     * Sets the sections value for this DescribeSoftphoneLayoutCallType.
     * 
     * @param sections
     */
    public void setSections(com.sforce.soap.partner.DescribeSoftphoneLayoutSection[] sections) {
        this.sections = sections;
    }

    public com.sforce.soap.partner.DescribeSoftphoneLayoutSection getSections(int i) {
        return this.sections[i];
    }

    public void setSections(int i, com.sforce.soap.partner.DescribeSoftphoneLayoutSection _value) {
        this.sections[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeSoftphoneLayoutCallType)) return false;
        DescribeSoftphoneLayoutCallType other = (DescribeSoftphoneLayoutCallType) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.infoFields==null && other.getInfoFields()==null) || 
             (this.infoFields!=null &&
              java.util.Arrays.equals(this.infoFields, other.getInfoFields()))) &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName()))) &&
            ((this.sections==null && other.getSections()==null) || 
             (this.sections!=null &&
              java.util.Arrays.equals(this.sections, other.getSections())));
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
        if (getInfoFields() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getInfoFields());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getInfoFields(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        if (getSections() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getSections());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getSections(), i);
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
        new org.apache.axis.description.TypeDesc(DescribeSoftphoneLayoutCallType.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutCallType"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("infoFields");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "infoFields"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutInfoField"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sections");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sections"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeSoftphoneLayoutSection"));
        elemField.setMinOccurs(0);
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
