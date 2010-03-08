/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

/**
 * Books_Output_Type.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package com.metamatrix.www.BooksView_Output;

public class Books_Output_Type  implements java.io.Serializable {
    private java.lang.String FIRSTNAME;

    private java.lang.String LASTNAME;

    private java.lang.String TITLE;

    private long ID;

    public Books_Output_Type() {
    }

    public Books_Output_Type(
           java.lang.String FIRSTNAME,
           java.lang.String LASTNAME,
           java.lang.String TITLE,
           long ID) {
           this.FIRSTNAME = FIRSTNAME;
           this.LASTNAME = LASTNAME;
           this.TITLE = TITLE;
           this.ID = ID;
    }


    /**
     * Gets the FIRSTNAME value for this Books_Output_Type.
     * 
     * @return FIRSTNAME
     */
    public java.lang.String getFIRSTNAME() {
        return FIRSTNAME;
    }


    /**
     * Sets the FIRSTNAME value for this Books_Output_Type.
     * 
     * @param FIRSTNAME
     */
    public void setFIRSTNAME(java.lang.String FIRSTNAME) {
        this.FIRSTNAME = FIRSTNAME;
    }


    /**
     * Gets the LASTNAME value for this Books_Output_Type.
     * 
     * @return LASTNAME
     */
    public java.lang.String getLASTNAME() {
        return LASTNAME;
    }


    /**
     * Sets the LASTNAME value for this Books_Output_Type.
     * 
     * @param LASTNAME
     */
    public void setLASTNAME(java.lang.String LASTNAME) {
        this.LASTNAME = LASTNAME;
    }


    /**
     * Gets the TITLE value for this Books_Output_Type.
     * 
     * @return TITLE
     */
    public java.lang.String getTITLE() {
        return TITLE;
    }


    /**
     * Sets the TITLE value for this Books_Output_Type.
     * 
     * @param TITLE
     */
    public void setTITLE(java.lang.String TITLE) {
        this.TITLE = TITLE;
    }


    /**
     * Gets the ID value for this Books_Output_Type.
     * 
     * @return ID
     */
    public long getID() {
        return ID;
    }


    /**
     * Sets the ID value for this Books_Output_Type.
     * 
     * @param ID
     */
    public void setID(long ID) {
        this.ID = ID;
    }

    private java.lang.Object __equalsCalc = null;
    @Override
	public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof Books_Output_Type)) return false;
        Books_Output_Type other = (Books_Output_Type) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.FIRSTNAME==null && other.getFIRSTNAME()==null) || 
             (this.FIRSTNAME!=null &&
              this.FIRSTNAME.equals(other.getFIRSTNAME()))) &&
            ((this.LASTNAME==null && other.getLASTNAME()==null) || 
             (this.LASTNAME!=null &&
              this.LASTNAME.equals(other.getLASTNAME()))) &&
            ((this.TITLE==null && other.getTITLE()==null) || 
             (this.TITLE!=null &&
              this.TITLE.equals(other.getTITLE()))) &&
            this.ID == other.getID();
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    @Override
	public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        if (getFIRSTNAME() != null) {
            _hashCode += getFIRSTNAME().hashCode();
        }
        if (getLASTNAME() != null) {
            _hashCode += getLASTNAME().hashCode();
        }
        if (getTITLE() != null) {
            _hashCode += getTITLE().hashCode();
        }
        _hashCode += new Long(getID()).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(Books_Output_Type.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://www.metamatrix.com/BooksView_Output", "Books_Output_Type")); //$NON-NLS-1$ //$NON-NLS-2$
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("FIRSTNAME"); //$NON-NLS-1$
        elemField.setXmlName(new javax.xml.namespace.QName("", "FIRSTNAME")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("LASTNAME"); //$NON-NLS-1$
        elemField.setXmlName(new javax.xml.namespace.QName("", "LASTNAME")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("TITLE"); //$NON-NLS-1$
        elemField.setXmlName(new javax.xml.namespace.QName("", "TITLE")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ID"); //$NON-NLS-1$
        elemField.setXmlName(new javax.xml.namespace.QName("", "ID")); //$NON-NLS-1$ //$NON-NLS-2$
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "long")); //$NON-NLS-1$ //$NON-NLS-2$
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
