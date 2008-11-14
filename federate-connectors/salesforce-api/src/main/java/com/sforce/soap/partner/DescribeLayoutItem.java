/**
 * DescribeLayoutItem.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeLayoutItem  implements java.io.Serializable {
    private boolean editable;

    private java.lang.String label;

    private com.sforce.soap.partner.DescribeLayoutComponent[] layoutComponents;

    private boolean placeholder;

    private boolean required;

    public DescribeLayoutItem() {
    }

    public DescribeLayoutItem(
           boolean editable,
           java.lang.String label,
           com.sforce.soap.partner.DescribeLayoutComponent[] layoutComponents,
           boolean placeholder,
           boolean required) {
           this.editable = editable;
           this.label = label;
           this.layoutComponents = layoutComponents;
           this.placeholder = placeholder;
           this.required = required;
    }


    /**
     * Gets the editable value for this DescribeLayoutItem.
     * 
     * @return editable
     */
    public boolean isEditable() {
        return editable;
    }


    /**
     * Sets the editable value for this DescribeLayoutItem.
     * 
     * @param editable
     */
    public void setEditable(boolean editable) {
        this.editable = editable;
    }


    /**
     * Gets the label value for this DescribeLayoutItem.
     * 
     * @return label
     */
    public java.lang.String getLabel() {
        return label;
    }


    /**
     * Sets the label value for this DescribeLayoutItem.
     * 
     * @param label
     */
    public void setLabel(java.lang.String label) {
        this.label = label;
    }


    /**
     * Gets the layoutComponents value for this DescribeLayoutItem.
     * 
     * @return layoutComponents
     */
    public com.sforce.soap.partner.DescribeLayoutComponent[] getLayoutComponents() {
        return layoutComponents;
    }


    /**
     * Sets the layoutComponents value for this DescribeLayoutItem.
     * 
     * @param layoutComponents
     */
    public void setLayoutComponents(com.sforce.soap.partner.DescribeLayoutComponent[] layoutComponents) {
        this.layoutComponents = layoutComponents;
    }

    public com.sforce.soap.partner.DescribeLayoutComponent getLayoutComponents(int i) {
        return this.layoutComponents[i];
    }

    public void setLayoutComponents(int i, com.sforce.soap.partner.DescribeLayoutComponent _value) {
        this.layoutComponents[i] = _value;
    }


    /**
     * Gets the placeholder value for this DescribeLayoutItem.
     * 
     * @return placeholder
     */
    public boolean isPlaceholder() {
        return placeholder;
    }


    /**
     * Sets the placeholder value for this DescribeLayoutItem.
     * 
     * @param placeholder
     */
    public void setPlaceholder(boolean placeholder) {
        this.placeholder = placeholder;
    }


    /**
     * Gets the required value for this DescribeLayoutItem.
     * 
     * @return required
     */
    public boolean isRequired() {
        return required;
    }


    /**
     * Sets the required value for this DescribeLayoutItem.
     * 
     * @param required
     */
    public void setRequired(boolean required) {
        this.required = required;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeLayoutItem)) return false;
        DescribeLayoutItem other = (DescribeLayoutItem) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.editable == other.isEditable() &&
            ((this.label==null && other.getLabel()==null) || 
             (this.label!=null &&
              this.label.equals(other.getLabel()))) &&
            ((this.layoutComponents==null && other.getLayoutComponents()==null) || 
             (this.layoutComponents!=null &&
              java.util.Arrays.equals(this.layoutComponents, other.getLayoutComponents()))) &&
            this.placeholder == other.isPlaceholder() &&
            this.required == other.isRequired();
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
        _hashCode += (isEditable() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getLabel() != null) {
            _hashCode += getLabel().hashCode();
        }
        if (getLayoutComponents() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getLayoutComponents());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getLayoutComponents(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += (isPlaceholder() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isRequired() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(DescribeLayoutItem.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutItem"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("editable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "editable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("label");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "label"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("layoutComponents");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "layoutComponents"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeLayoutComponent"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("placeholder");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "placeholder"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("required");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "required"));
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
