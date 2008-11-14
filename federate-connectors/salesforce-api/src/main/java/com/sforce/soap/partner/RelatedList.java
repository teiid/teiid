/**
 * RelatedList.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class RelatedList  implements java.io.Serializable {
    private com.sforce.soap.partner.RelatedListColumn[] columns;

    private boolean custom;

    private java.lang.String field;

    private java.lang.String label;

    private int limitRows;

    private java.lang.String name;

    private java.lang.String sobject;

    private com.sforce.soap.partner.RelatedListSort[] sort;

    public RelatedList() {
    }

    public RelatedList(
           com.sforce.soap.partner.RelatedListColumn[] columns,
           boolean custom,
           java.lang.String field,
           java.lang.String label,
           int limitRows,
           java.lang.String name,
           java.lang.String sobject,
           com.sforce.soap.partner.RelatedListSort[] sort) {
           this.columns = columns;
           this.custom = custom;
           this.field = field;
           this.label = label;
           this.limitRows = limitRows;
           this.name = name;
           this.sobject = sobject;
           this.sort = sort;
    }


    /**
     * Gets the columns value for this RelatedList.
     * 
     * @return columns
     */
    public com.sforce.soap.partner.RelatedListColumn[] getColumns() {
        return columns;
    }


    /**
     * Sets the columns value for this RelatedList.
     * 
     * @param columns
     */
    public void setColumns(com.sforce.soap.partner.RelatedListColumn[] columns) {
        this.columns = columns;
    }

    public com.sforce.soap.partner.RelatedListColumn getColumns(int i) {
        return this.columns[i];
    }

    public void setColumns(int i, com.sforce.soap.partner.RelatedListColumn _value) {
        this.columns[i] = _value;
    }


    /**
     * Gets the custom value for this RelatedList.
     * 
     * @return custom
     */
    public boolean isCustom() {
        return custom;
    }


    /**
     * Sets the custom value for this RelatedList.
     * 
     * @param custom
     */
    public void setCustom(boolean custom) {
        this.custom = custom;
    }


    /**
     * Gets the field value for this RelatedList.
     * 
     * @return field
     */
    public java.lang.String getField() {
        return field;
    }


    /**
     * Sets the field value for this RelatedList.
     * 
     * @param field
     */
    public void setField(java.lang.String field) {
        this.field = field;
    }


    /**
     * Gets the label value for this RelatedList.
     * 
     * @return label
     */
    public java.lang.String getLabel() {
        return label;
    }


    /**
     * Sets the label value for this RelatedList.
     * 
     * @param label
     */
    public void setLabel(java.lang.String label) {
        this.label = label;
    }


    /**
     * Gets the limitRows value for this RelatedList.
     * 
     * @return limitRows
     */
    public int getLimitRows() {
        return limitRows;
    }


    /**
     * Sets the limitRows value for this RelatedList.
     * 
     * @param limitRows
     */
    public void setLimitRows(int limitRows) {
        this.limitRows = limitRows;
    }


    /**
     * Gets the name value for this RelatedList.
     * 
     * @return name
     */
    public java.lang.String getName() {
        return name;
    }


    /**
     * Sets the name value for this RelatedList.
     * 
     * @param name
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }


    /**
     * Gets the sobject value for this RelatedList.
     * 
     * @return sobject
     */
    public java.lang.String getSobject() {
        return sobject;
    }


    /**
     * Sets the sobject value for this RelatedList.
     * 
     * @param sobject
     */
    public void setSobject(java.lang.String sobject) {
        this.sobject = sobject;
    }


    /**
     * Gets the sort value for this RelatedList.
     * 
     * @return sort
     */
    public com.sforce.soap.partner.RelatedListSort[] getSort() {
        return sort;
    }


    /**
     * Sets the sort value for this RelatedList.
     * 
     * @param sort
     */
    public void setSort(com.sforce.soap.partner.RelatedListSort[] sort) {
        this.sort = sort;
    }

    public com.sforce.soap.partner.RelatedListSort getSort(int i) {
        return this.sort[i];
    }

    public void setSort(int i, com.sforce.soap.partner.RelatedListSort _value) {
        this.sort[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RelatedList)) return false;
        RelatedList other = (RelatedList) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.columns==null && other.getColumns()==null) || 
             (this.columns!=null &&
              java.util.Arrays.equals(this.columns, other.getColumns()))) &&
            this.custom == other.isCustom() &&
            ((this.field==null && other.getField()==null) || 
             (this.field!=null &&
              this.field.equals(other.getField()))) &&
            ((this.label==null && other.getLabel()==null) || 
             (this.label!=null &&
              this.label.equals(other.getLabel()))) &&
            this.limitRows == other.getLimitRows() &&
            ((this.name==null && other.getName()==null) || 
             (this.name!=null &&
              this.name.equals(other.getName()))) &&
            ((this.sobject==null && other.getSobject()==null) || 
             (this.sobject!=null &&
              this.sobject.equals(other.getSobject()))) &&
            ((this.sort==null && other.getSort()==null) || 
             (this.sort!=null &&
              java.util.Arrays.equals(this.sort, other.getSort())));
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
        if (getColumns() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getColumns());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getColumns(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += (isCustom() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getField() != null) {
            _hashCode += getField().hashCode();
        }
        if (getLabel() != null) {
            _hashCode += getLabel().hashCode();
        }
        _hashCode += getLimitRows();
        if (getName() != null) {
            _hashCode += getName().hashCode();
        }
        if (getSobject() != null) {
            _hashCode += getSobject().hashCode();
        }
        if (getSort() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getSort());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getSort(), i);
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
        new org.apache.axis.description.TypeDesc(RelatedList.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "RelatedList"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("columns");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "columns"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "RelatedListColumn"));
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("custom");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "custom"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("field");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "field"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("label");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "label"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("limitRows");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "limitRows"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("name");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "name"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sobject");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sobject"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sort");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sort"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "RelatedListSort"));
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
