/**
 * ApiQueryFault.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner.fault;

public class ApiQueryFault  extends com.sforce.soap.partner.fault.ApiFault  implements java.io.Serializable {
    private int row;

    private int column;

    public ApiQueryFault() {
    }

    public ApiQueryFault(
           com.sforce.soap.partner.fault.ExceptionCode exceptionCode,
           java.lang.String exceptionMessage,
           int row,
           int column) {
        super(
            exceptionCode,
            exceptionMessage);
        this.row = row;
        this.column = column;
    }


    /**
     * Gets the row value for this ApiQueryFault.
     * 
     * @return row
     */
    public int getRow() {
        return row;
    }


    /**
     * Sets the row value for this ApiQueryFault.
     * 
     * @param row
     */
    public void setRow(int row) {
        this.row = row;
    }


    /**
     * Gets the column value for this ApiQueryFault.
     * 
     * @return column
     */
    public int getColumn() {
        return column;
    }


    /**
     * Sets the column value for this ApiQueryFault.
     * 
     * @param column
     */
    public void setColumn(int column) {
        this.column = column;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ApiQueryFault)) return false;
        ApiQueryFault other = (ApiQueryFault) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = super.equals(obj) && 
            this.row == other.getRow() &&
            this.column == other.getColumn();
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = super.hashCode();
        _hashCode += getRow();
        _hashCode += getColumn();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(ApiQueryFault.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:fault.partner.soap.sforce.com", "ApiQueryFault"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("row");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:fault.partner.soap.sforce.com", "row"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("column");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:fault.partner.soap.sforce.com", "column"));
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


    /**
     * Writes the exception data to the faultDetails
     */
    public void writeDetails(javax.xml.namespace.QName qname, org.apache.axis.encoding.SerializationContext context) throws java.io.IOException {
        context.serialize(qname, null, this);
    }
}
