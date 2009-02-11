/**
 * GetDeletedResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class GetDeletedResult  implements java.io.Serializable {
    private com.sforce.soap.partner.DeletedRecord[] deletedRecords;

    private java.util.Calendar earliestDateAvailable;

    private java.util.Calendar latestDateCovered;

    private java.lang.String sforceReserved;

    public GetDeletedResult() {
    }

    public GetDeletedResult(
           com.sforce.soap.partner.DeletedRecord[] deletedRecords,
           java.util.Calendar earliestDateAvailable,
           java.util.Calendar latestDateCovered,
           java.lang.String sforceReserved) {
           this.deletedRecords = deletedRecords;
           this.earliestDateAvailable = earliestDateAvailable;
           this.latestDateCovered = latestDateCovered;
           this.sforceReserved = sforceReserved;
    }


    /**
     * Gets the deletedRecords value for this GetDeletedResult.
     * 
     * @return deletedRecords
     */
    public com.sforce.soap.partner.DeletedRecord[] getDeletedRecords() {
        return deletedRecords;
    }


    /**
     * Sets the deletedRecords value for this GetDeletedResult.
     * 
     * @param deletedRecords
     */
    public void setDeletedRecords(com.sforce.soap.partner.DeletedRecord[] deletedRecords) {
        this.deletedRecords = deletedRecords;
    }

    public com.sforce.soap.partner.DeletedRecord getDeletedRecords(int i) {
        return this.deletedRecords[i];
    }

    public void setDeletedRecords(int i, com.sforce.soap.partner.DeletedRecord _value) {
        this.deletedRecords[i] = _value;
    }


    /**
     * Gets the earliestDateAvailable value for this GetDeletedResult.
     * 
     * @return earliestDateAvailable
     */
    public java.util.Calendar getEarliestDateAvailable() {
        return earliestDateAvailable;
    }


    /**
     * Sets the earliestDateAvailable value for this GetDeletedResult.
     * 
     * @param earliestDateAvailable
     */
    public void setEarliestDateAvailable(java.util.Calendar earliestDateAvailable) {
        this.earliestDateAvailable = earliestDateAvailable;
    }


    /**
     * Gets the latestDateCovered value for this GetDeletedResult.
     * 
     * @return latestDateCovered
     */
    public java.util.Calendar getLatestDateCovered() {
        return latestDateCovered;
    }


    /**
     * Sets the latestDateCovered value for this GetDeletedResult.
     * 
     * @param latestDateCovered
     */
    public void setLatestDateCovered(java.util.Calendar latestDateCovered) {
        this.latestDateCovered = latestDateCovered;
    }


    /**
     * Gets the sforceReserved value for this GetDeletedResult.
     * 
     * @return sforceReserved
     */
    public java.lang.String getSforceReserved() {
        return sforceReserved;
    }


    /**
     * Sets the sforceReserved value for this GetDeletedResult.
     * 
     * @param sforceReserved
     */
    public void setSforceReserved(java.lang.String sforceReserved) {
        this.sforceReserved = sforceReserved;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof GetDeletedResult)) return false;
        GetDeletedResult other = (GetDeletedResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.deletedRecords==null && other.getDeletedRecords()==null) || 
             (this.deletedRecords!=null &&
              java.util.Arrays.equals(this.deletedRecords, other.getDeletedRecords()))) &&
            ((this.earliestDateAvailable==null && other.getEarliestDateAvailable()==null) || 
             (this.earliestDateAvailable!=null &&
              this.earliestDateAvailable.equals(other.getEarliestDateAvailable()))) &&
            ((this.latestDateCovered==null && other.getLatestDateCovered()==null) || 
             (this.latestDateCovered!=null &&
              this.latestDateCovered.equals(other.getLatestDateCovered()))) &&
            ((this.sforceReserved==null && other.getSforceReserved()==null) || 
             (this.sforceReserved!=null &&
              this.sforceReserved.equals(other.getSforceReserved())));
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
        if (getDeletedRecords() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getDeletedRecords());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getDeletedRecords(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getEarliestDateAvailable() != null) {
            _hashCode += getEarliestDateAvailable().hashCode();
        }
        if (getLatestDateCovered() != null) {
            _hashCode += getLatestDateCovered().hashCode();
        }
        if (getSforceReserved() != null) {
            _hashCode += getSforceReserved().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(GetDeletedResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "GetDeletedResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("deletedRecords");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "deletedRecords"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "DeletedRecord"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("earliestDateAvailable");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "earliestDateAvailable"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("latestDateCovered");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "latestDateCovered"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sforceReserved");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "sforceReserved"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
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
