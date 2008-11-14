/**
 * DescribeTabSetResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class DescribeTabSetResult  implements java.io.Serializable {
    private java.lang.String label;

    private java.lang.String logoUrl;

    private java.lang.String namespace;

    private boolean selected;

    private com.sforce.soap.partner.DescribeTab[] tabs;

    public DescribeTabSetResult() {
    }

    public DescribeTabSetResult(
           java.lang.String label,
           java.lang.String logoUrl,
           java.lang.String namespace,
           boolean selected,
           com.sforce.soap.partner.DescribeTab[] tabs) {
           this.label = label;
           this.logoUrl = logoUrl;
           this.namespace = namespace;
           this.selected = selected;
           this.tabs = tabs;
    }


    /**
     * Gets the label value for this DescribeTabSetResult.
     * 
     * @return label
     */
    public java.lang.String getLabel() {
        return label;
    }


    /**
     * Sets the label value for this DescribeTabSetResult.
     * 
     * @param label
     */
    public void setLabel(java.lang.String label) {
        this.label = label;
    }


    /**
     * Gets the logoUrl value for this DescribeTabSetResult.
     * 
     * @return logoUrl
     */
    public java.lang.String getLogoUrl() {
        return logoUrl;
    }


    /**
     * Sets the logoUrl value for this DescribeTabSetResult.
     * 
     * @param logoUrl
     */
    public void setLogoUrl(java.lang.String logoUrl) {
        this.logoUrl = logoUrl;
    }


    /**
     * Gets the namespace value for this DescribeTabSetResult.
     * 
     * @return namespace
     */
    public java.lang.String getNamespace() {
        return namespace;
    }


    /**
     * Sets the namespace value for this DescribeTabSetResult.
     * 
     * @param namespace
     */
    public void setNamespace(java.lang.String namespace) {
        this.namespace = namespace;
    }


    /**
     * Gets the selected value for this DescribeTabSetResult.
     * 
     * @return selected
     */
    public boolean isSelected() {
        return selected;
    }


    /**
     * Sets the selected value for this DescribeTabSetResult.
     * 
     * @param selected
     */
    public void setSelected(boolean selected) {
        this.selected = selected;
    }


    /**
     * Gets the tabs value for this DescribeTabSetResult.
     * 
     * @return tabs
     */
    public com.sforce.soap.partner.DescribeTab[] getTabs() {
        return tabs;
    }


    /**
     * Sets the tabs value for this DescribeTabSetResult.
     * 
     * @param tabs
     */
    public void setTabs(com.sforce.soap.partner.DescribeTab[] tabs) {
        this.tabs = tabs;
    }

    public com.sforce.soap.partner.DescribeTab getTabs(int i) {
        return this.tabs[i];
    }

    public void setTabs(int i, com.sforce.soap.partner.DescribeTab _value) {
        this.tabs[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof DescribeTabSetResult)) return false;
        DescribeTabSetResult other = (DescribeTabSetResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.label==null && other.getLabel()==null) || 
             (this.label!=null &&
              this.label.equals(other.getLabel()))) &&
            ((this.logoUrl==null && other.getLogoUrl()==null) || 
             (this.logoUrl!=null &&
              this.logoUrl.equals(other.getLogoUrl()))) &&
            ((this.namespace==null && other.getNamespace()==null) || 
             (this.namespace!=null &&
              this.namespace.equals(other.getNamespace()))) &&
            this.selected == other.isSelected() &&
            ((this.tabs==null && other.getTabs()==null) || 
             (this.tabs!=null &&
              java.util.Arrays.equals(this.tabs, other.getTabs())));
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
        if (getLabel() != null) {
            _hashCode += getLabel().hashCode();
        }
        if (getLogoUrl() != null) {
            _hashCode += getLogoUrl().hashCode();
        }
        if (getNamespace() != null) {
            _hashCode += getNamespace().hashCode();
        }
        _hashCode += (isSelected() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getTabs() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getTabs());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getTabs(), i);
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
        new org.apache.axis.description.TypeDesc(DescribeTabSetResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeTabSetResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("label");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "label"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("logoUrl");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "logoUrl"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("namespace");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "namespace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("selected");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "selected"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("tabs");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "tabs"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DescribeTab"));
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
