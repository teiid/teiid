/**
 * DescribeGlobalResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeGlobalResult  implements java.io.Serializable {
    private java.lang.String encoding;

    private int maxBatchSize;

    private com.sforce.soap.partner.DescribeGlobalSObjectResult[] sobjects;

    public DescribeGlobalResult() {
    }

    public DescribeGlobalResult(
           java.lang.String encoding,
           int maxBatchSize,
           com.sforce.soap.partner.DescribeGlobalSObjectResult[] sobjects) {
           this.encoding = encoding;
           this.maxBatchSize = maxBatchSize;
           this.sobjects = sobjects;
    }


    /**
     * Gets the encoding value for this DescribeGlobalResult.
     * 
     * @return encoding
     */
    public java.lang.String getEncoding() {
        return encoding;
    }


    /**
     * Sets the encoding value for this DescribeGlobalResult.
     * 
     * @param encoding
     */
    public void setEncoding(java.lang.String encoding) {
        this.encoding = encoding;
    }


    /**
     * Gets the maxBatchSize value for this DescribeGlobalResult.
     * 
     * @return maxBatchSize
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }


    /**
     * Sets the maxBatchSize value for this DescribeGlobalResult.
     * 
     * @param maxBatchSize
     */
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    /**
     * Gets the sobjects value for this DescribeGlobalResult.
     * 
     * @return sobjects
     */
    public com.sforce.soap.partner.DescribeGlobalSObjectResult[] getSobjects() {
        return sobjects;
    }


    /**
     * Sets the sobjects value for this DescribeGlobalResult.
     * 
     * @param sobjects
     */
    public void setSobjects(com.sforce.soap.partner.DescribeGlobalSObjectResult[] sobjects) {
        this.sobjects = sobjects;
    }

    public com.sforce.soap.partner.DescribeGlobalSObjectResult getSobjects(int i) {
        return this.sobjects[i];
    }

    public void setSobjects(int i, com.sforce.soap.partner.DescribeGlobalSObjectResult _value) {
        this.sobjects[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeGlobalResult)) return false;
        DescribeGlobalResult other = (DescribeGlobalResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.encoding==null && other.getEncoding()==null) || 
             (this.encoding!=null &&
              this.encoding.equals(other.getEncoding()))) &&
            this.maxBatchSize == other.getMaxBatchSize() &&
            ((this.sobjects==null && other.getSobjects()==null) || 
             (this.sobjects!=null &&
              java.util.Arrays.equals(this.sobjects, other.getSobjects())));
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
        if (getEncoding() != null) {
            _hashCode += getEncoding().hashCode();
        }
        _hashCode += getMaxBatchSize();
        if (getSobjects() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getSobjects());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getSobjects(), i);
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
        new org.apache.axis.description.TypeDesc(DescribeGlobalResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeGlobalResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("encoding");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "encoding"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("maxBatchSize");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "maxBatchSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sobjects");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sobjects"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeGlobalSObjectResult"));
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
