/**
 * DescribeLayoutSection.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutSection  implements java.io.Serializable {
    private int columns;

    private java.lang.String heading;

    private com.sforce.soap.partner.DescribeLayoutRow[] layoutRows;

    private int rows;

    private boolean useCollapsibleSection;

    private boolean useHeading;

    public DescribeLayoutSection() {
    }

    public DescribeLayoutSection(
           int columns,
           java.lang.String heading,
           com.sforce.soap.partner.DescribeLayoutRow[] layoutRows,
           int rows,
           boolean useCollapsibleSection,
           boolean useHeading) {
           this.columns = columns;
           this.heading = heading;
           this.layoutRows = layoutRows;
           this.rows = rows;
           this.useCollapsibleSection = useCollapsibleSection;
           this.useHeading = useHeading;
    }


    /**
     * Gets the columns value for this DescribeLayoutSection.
     * 
     * @return columns
     */
    public int getColumns() {
        return columns;
    }


    /**
     * Sets the columns value for this DescribeLayoutSection.
     * 
     * @param columns
     */
    public void setColumns(int columns) {
        this.columns = columns;
    }


    /**
     * Gets the heading value for this DescribeLayoutSection.
     * 
     * @return heading
     */
    public java.lang.String getHeading() {
        return heading;
    }


    /**
     * Sets the heading value for this DescribeLayoutSection.
     * 
     * @param heading
     */
    public void setHeading(java.lang.String heading) {
        this.heading = heading;
    }


    /**
     * Gets the layoutRows value for this DescribeLayoutSection.
     * 
     * @return layoutRows
     */
    public com.sforce.soap.partner.DescribeLayoutRow[] getLayoutRows() {
        return layoutRows;
    }


    /**
     * Sets the layoutRows value for this DescribeLayoutSection.
     * 
     * @param layoutRows
     */
    public void setLayoutRows(com.sforce.soap.partner.DescribeLayoutRow[] layoutRows) {
        this.layoutRows = layoutRows;
    }

    public com.sforce.soap.partner.DescribeLayoutRow getLayoutRows(int i) {
        return this.layoutRows[i];
    }

    public void setLayoutRows(int i, com.sforce.soap.partner.DescribeLayoutRow _value) {
        this.layoutRows[i] = _value;
    }


    /**
     * Gets the rows value for this DescribeLayoutSection.
     * 
     * @return rows
     */
    public int getRows() {
        return rows;
    }


    /**
     * Sets the rows value for this DescribeLayoutSection.
     * 
     * @param rows
     */
    public void setRows(int rows) {
        this.rows = rows;
    }


    /**
     * Gets the useCollapsibleSection value for this DescribeLayoutSection.
     * 
     * @return useCollapsibleSection
     */
    public boolean isUseCollapsibleSection() {
        return useCollapsibleSection;
    }


    /**
     * Sets the useCollapsibleSection value for this DescribeLayoutSection.
     * 
     * @param useCollapsibleSection
     */
    public void setUseCollapsibleSection(boolean useCollapsibleSection) {
        this.useCollapsibleSection = useCollapsibleSection;
    }


    /**
     * Gets the useHeading value for this DescribeLayoutSection.
     * 
     * @return useHeading
     */
    public boolean isUseHeading() {
        return useHeading;
    }


    /**
     * Sets the useHeading value for this DescribeLayoutSection.
     * 
     * @param useHeading
     */
    public void setUseHeading(boolean useHeading) {
        this.useHeading = useHeading;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutSection)) return false;
        DescribeLayoutSection other = (DescribeLayoutSection) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.columns == other.getColumns() &&
            ((this.heading==null && other.getHeading()==null) || 
             (this.heading!=null &&
              this.heading.equals(other.getHeading()))) &&
            ((this.layoutRows==null && other.getLayoutRows()==null) || 
             (this.layoutRows!=null &&
              java.util.Arrays.equals(this.layoutRows, other.getLayoutRows()))) &&
            this.rows == other.getRows() &&
            this.useCollapsibleSection == other.isUseCollapsibleSection() &&
            this.useHeading == other.isUseHeading();
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
        _hashCode += getColumns();
        if (getHeading() != null) {
            _hashCode += getHeading().hashCode();
        }
        if (getLayoutRows() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getLayoutRows());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getLayoutRows(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += getRows();
        _hashCode += (isUseCollapsibleSection() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isUseHeading() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeLayoutSection.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutSection"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("columns");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "columns"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("heading");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "heading"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layoutRows");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutRows"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutRow"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("rows");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "rows"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("useCollapsibleSection");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "useCollapsibleSection"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("useHeading");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "useHeading"));
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
