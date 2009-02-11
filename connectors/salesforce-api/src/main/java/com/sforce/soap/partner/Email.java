/**
 * Email.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class Email  implements java.io.Serializable {
    private java.lang.Boolean bccSender;

    private com.sforce.soap.partner.EmailPriority emailPriority;

    private java.lang.String replyTo;

    private java.lang.Boolean saveAsActivity;

    private java.lang.String senderDisplayName;

    private java.lang.String subject;

    private java.lang.Boolean useSignature;

    public Email() {
    }

    public Email(
           java.lang.Boolean bccSender,
           com.sforce.soap.partner.EmailPriority emailPriority,
           java.lang.String replyTo,
           java.lang.Boolean saveAsActivity,
           java.lang.String senderDisplayName,
           java.lang.String subject,
           java.lang.Boolean useSignature) {
           this.bccSender = bccSender;
           this.emailPriority = emailPriority;
           this.replyTo = replyTo;
           this.saveAsActivity = saveAsActivity;
           this.senderDisplayName = senderDisplayName;
           this.subject = subject;
           this.useSignature = useSignature;
    }


    /**
     * Gets the bccSender value for this Email.
     * 
     * @return bccSender
     */
    public java.lang.Boolean getBccSender() {
        return bccSender;
    }


    /**
     * Sets the bccSender value for this Email.
     * 
     * @param bccSender
     */
    public void setBccSender(java.lang.Boolean bccSender) {
        this.bccSender = bccSender;
    }


    /**
     * Gets the emailPriority value for this Email.
     * 
     * @return emailPriority
     */
    public com.sforce.soap.partner.EmailPriority getEmailPriority() {
        return emailPriority;
    }


    /**
     * Sets the emailPriority value for this Email.
     * 
     * @param emailPriority
     */
    public void setEmailPriority(com.sforce.soap.partner.EmailPriority emailPriority) {
        this.emailPriority = emailPriority;
    }


    /**
     * Gets the replyTo value for this Email.
     * 
     * @return replyTo
     */
    public java.lang.String getReplyTo() {
        return replyTo;
    }


    /**
     * Sets the replyTo value for this Email.
     * 
     * @param replyTo
     */
    public void setReplyTo(java.lang.String replyTo) {
        this.replyTo = replyTo;
    }


    /**
     * Gets the saveAsActivity value for this Email.
     * 
     * @return saveAsActivity
     */
    public java.lang.Boolean getSaveAsActivity() {
        return saveAsActivity;
    }


    /**
     * Sets the saveAsActivity value for this Email.
     * 
     * @param saveAsActivity
     */
    public void setSaveAsActivity(java.lang.Boolean saveAsActivity) {
        this.saveAsActivity = saveAsActivity;
    }


    /**
     * Gets the senderDisplayName value for this Email.
     * 
     * @return senderDisplayName
     */
    public java.lang.String getSenderDisplayName() {
        return senderDisplayName;
    }


    /**
     * Sets the senderDisplayName value for this Email.
     * 
     * @param senderDisplayName
     */
    public void setSenderDisplayName(java.lang.String senderDisplayName) {
        this.senderDisplayName = senderDisplayName;
    }


    /**
     * Gets the subject value for this Email.
     * 
     * @return subject
     */
    public java.lang.String getSubject() {
        return subject;
    }


    /**
     * Sets the subject value for this Email.
     * 
     * @param subject
     */
    public void setSubject(java.lang.String subject) {
        this.subject = subject;
    }


    /**
     * Gets the useSignature value for this Email.
     * 
     * @return useSignature
     */
    public java.lang.Boolean getUseSignature() {
        return useSignature;
    }


    /**
     * Sets the useSignature value for this Email.
     * 
     * @param useSignature
     */
    public void setUseSignature(java.lang.Boolean useSignature) {
        this.useSignature = useSignature;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof Email)) return false;
        Email other = (Email) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.bccSender==null && other.getBccSender()==null) || 
             (this.bccSender!=null &&
              this.bccSender.equals(other.getBccSender()))) &&
            ((this.emailPriority==null && other.getEmailPriority()==null) || 
             (this.emailPriority!=null &&
              this.emailPriority.equals(other.getEmailPriority()))) &&
            ((this.replyTo==null && other.getReplyTo()==null) || 
             (this.replyTo!=null &&
              this.replyTo.equals(other.getReplyTo()))) &&
            ((this.saveAsActivity==null && other.getSaveAsActivity()==null) || 
             (this.saveAsActivity!=null &&
              this.saveAsActivity.equals(other.getSaveAsActivity()))) &&
            ((this.senderDisplayName==null && other.getSenderDisplayName()==null) || 
             (this.senderDisplayName!=null &&
              this.senderDisplayName.equals(other.getSenderDisplayName()))) &&
            ((this.subject==null && other.getSubject()==null) || 
             (this.subject!=null &&
              this.subject.equals(other.getSubject()))) &&
            ((this.useSignature==null && other.getUseSignature()==null) || 
             (this.useSignature!=null &&
              this.useSignature.equals(other.getUseSignature())));
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
        if (getBccSender() != null) {
            _hashCode += getBccSender().hashCode();
        }
        if (getEmailPriority() != null) {
            _hashCode += getEmailPriority().hashCode();
        }
        if (getReplyTo() != null) {
            _hashCode += getReplyTo().hashCode();
        }
        if (getSaveAsActivity() != null) {
            _hashCode += getSaveAsActivity().hashCode();
        }
        if (getSenderDisplayName() != null) {
            _hashCode += getSenderDisplayName().hashCode();
        }
        if (getSubject() != null) {
            _hashCode += getSubject().hashCode();
        }
        if (getUseSignature() != null) {
            _hashCode += getUseSignature().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(Email.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "Email"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("bccSender");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "bccSender"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("emailPriority");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "emailPriority"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "EmailPriority"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("replyTo");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "replyTo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("saveAsActivity");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "saveAsActivity"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("senderDisplayName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "senderDisplayName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("subject");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "subject"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("useSignature");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "useSignature"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(true);
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
