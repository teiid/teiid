/**
 * DescribeLayoutResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutResult  implements java.io.Serializable {
    private com.sforce.soap.partner.DescribeLayout[] layouts;

    private com.sforce.soap.partner.RecordTypeMapping[] recordTypeMappings;

    private boolean recordTypeSelectorRequired;

    public DescribeLayoutResult() {
    }

    public DescribeLayoutResult(
           com.sforce.soap.partner.DescribeLayout[] layouts,
           com.sforce.soap.partner.RecordTypeMapping[] recordTypeMappings,
           boolean recordTypeSelectorRequired) {
           this.layouts = layouts;
           this.recordTypeMappings = recordTypeMappings;
           this.recordTypeSelectorRequired = recordTypeSelectorRequired;
    }


    /**
     * Gets the layouts value for this DescribeLayoutResult.
     * 
     * @return layouts
     */
    public com.sforce.soap.partner.DescribeLayout[] getLayouts() {
        return layouts;
    }


    /**
     * Sets the layouts value for this DescribeLayoutResult.
     * 
     * @param layouts
     */
    public void setLayouts(com.sforce.soap.partner.DescribeLayout[] layouts) {
        this.layouts = layouts;
    }

    public com.sforce.soap.partner.DescribeLayout getLayouts(int i) {
        return this.layouts[i];
    }

    public void setLayouts(int i, com.sforce.soap.partner.DescribeLayout _value) {
        this.layouts[i] = _value;
    }


    /**
     * Gets the recordTypeMappings value for this DescribeLayoutResult.
     * 
     * @return recordTypeMappings
     */
    public com.sforce.soap.partner.RecordTypeMapping[] getRecordTypeMappings() {
        return recordTypeMappings;
    }


    /**
     * Sets the recordTypeMappings value for this DescribeLayoutResult.
     * 
     * @param recordTypeMappings
     */
    public void setRecordTypeMappings(com.sforce.soap.partner.RecordTypeMapping[] recordTypeMappings) {
        this.recordTypeMappings = recordTypeMappings;
    }

    public com.sforce.soap.partner.RecordTypeMapping getRecordTypeMappings(int i) {
        return this.recordTypeMappings[i];
    }

    public void setRecordTypeMappings(int i, com.sforce.soap.partner.RecordTypeMapping _value) {
        this.recordTypeMappings[i] = _value;
    }


    /**
     * Gets the recordTypeSelectorRequired value for this DescribeLayoutResult.
     * 
     * @return recordTypeSelectorRequired
     */
    public boolean isRecordTypeSelectorRequired() {
        return recordTypeSelectorRequired;
    }


    /**
     * Sets the recordTypeSelectorRequired value for this DescribeLayoutResult.
     * 
     * @param recordTypeSelectorRequired
     */
    public void setRecordTypeSelectorRequired(boolean recordTypeSelectorRequired) {
        this.recordTypeSelectorRequired = recordTypeSelectorRequired;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutResult)) return false;
        DescribeLayoutResult other = (DescribeLayoutResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.layouts==null && other.getLayouts()==null) || 
             (this.layouts!=null &&
              java.util.Arrays.equals(this.layouts, other.getLayouts()))) &&
            ((this.recordTypeMappings==null && other.getRecordTypeMappings()==null) || 
             (this.recordTypeMappings!=null &&
              java.util.Arrays.equals(this.recordTypeMappings, other.getRecordTypeMappings()))) &&
            this.recordTypeSelectorRequired == other.isRecordTypeSelectorRequired();
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
        if (getLayouts() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getLayouts());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getLayouts(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getRecordTypeMappings() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getRecordTypeMappings());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getRecordTypeMappings(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += (isRecordTypeSelectorRequired() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeLayoutResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layouts");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layouts"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayout"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("recordTypeMappings");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "recordTypeMappings"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "RecordTypeMapping"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("recordTypeSelectorRequired");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "recordTypeSelectorRequired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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
