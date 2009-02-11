/**
 * SingleEmailMessage.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class SingleEmailMessage  extends com.sforce.soap.partner.Email  implements java.io.Serializable {
    private java.lang.String[] bccAddresses;

    private java.lang.String[] ccAddresses;

    private java.lang.String charset;

    private java.lang.String[] documentAttachments;

    private java.lang.String htmlBody;

    private com.sforce.soap.partner.EmailFileAttachment[] fileAttachments;

    private java.lang.String plainTextBody;

    private java.lang.String targetObjectId;

    private java.lang.String templateId;

    private java.lang.String[] toAddresses;

    private java.lang.String whatId;

    public SingleEmailMessage() {
    }

    public SingleEmailMessage(
           java.lang.Boolean bccSender,
           com.sforce.soap.partner.EmailPriority emailPriority,
           java.lang.String replyTo,
           java.lang.Boolean saveAsActivity,
           java.lang.String senderDisplayName,
           java.lang.String subject,
           java.lang.Boolean useSignature,
           java.lang.String[] bccAddresses,
           java.lang.String[] ccAddresses,
           java.lang.String charset,
           java.lang.String[] documentAttachments,
           java.lang.String htmlBody,
           com.sforce.soap.partner.EmailFileAttachment[] fileAttachments,
           java.lang.String plainTextBody,
           java.lang.String targetObjectId,
           java.lang.String templateId,
           java.lang.String[] toAddresses,
           java.lang.String whatId) {
        super(
            bccSender,
            emailPriority,
            replyTo,
            saveAsActivity,
            senderDisplayName,
            subject,
            useSignature);
        this.bccAddresses = bccAddresses;
        this.ccAddresses = ccAddresses;
        this.charset = charset;
        this.documentAttachments = documentAttachments;
        this.htmlBody = htmlBody;
        this.fileAttachments = fileAttachments;
        this.plainTextBody = plainTextBody;
        this.targetObjectId = targetObjectId;
        this.templateId = templateId;
        this.toAddresses = toAddresses;
        this.whatId = whatId;
    }


    /**
     * Gets the bccAddresses value for this SingleEmailMessage.
     * 
     * @return bccAddresses
     */
    public java.lang.String[] getBccAddresses() {
        return bccAddresses;
    }


    /**
     * Sets the bccAddresses value for this SingleEmailMessage.
     * 
     * @param bccAddresses
     */
    public void setBccAddresses(java.lang.String[] bccAddresses) {
        this.bccAddresses = bccAddresses;
    }

    public java.lang.String getBccAddresses(int i) {
        return this.bccAddresses[i];
    }

    public void setBccAddresses(int i, java.lang.String _value) {
        this.bccAddresses[i] = _value;
    }


    /**
     * Gets the ccAddresses value for this SingleEmailMessage.
     * 
     * @return ccAddresses
     */
    public java.lang.String[] getCcAddresses() {
        return ccAddresses;
    }


    /**
     * Sets the ccAddresses value for this SingleEmailMessage.
     * 
     * @param ccAddresses
     */
    public void setCcAddresses(java.lang.String[] ccAddresses) {
        this.ccAddresses = ccAddresses;
    }

    public java.lang.String getCcAddresses(int i) {
        return this.ccAddresses[i];
    }

    public void setCcAddresses(int i, java.lang.String _value) {
        this.ccAddresses[i] = _value;
    }


    /**
     * Gets the charset value for this SingleEmailMessage.
     * 
     * @return charset
     */
    public java.lang.String getCharset() {
        return charset;
    }


    /**
     * Sets the charset value for this SingleEmailMessage.
     * 
     * @param charset
     */
    public void setCharset(java.lang.String charset) {
        this.charset = charset;
    }


    /**
     * Gets the documentAttachments value for this SingleEmailMessage.
     * 
     * @return documentAttachments
     */
    public java.lang.String[] getDocumentAttachments() {
        return documentAttachments;
    }


    /**
     * Sets the documentAttachments value for this SingleEmailMessage.
     * 
     * @param documentAttachments
     */
    public void setDocumentAttachments(java.lang.String[] documentAttachments) {
        this.documentAttachments = documentAttachments;
    }

    public java.lang.String getDocumentAttachments(int i) {
        return this.documentAttachments[i];
    }

    public void setDocumentAttachments(int i, java.lang.String _value) {
        this.documentAttachments[i] = _value;
    }


    /**
     * Gets the htmlBody value for this SingleEmailMessage.
     * 
     * @return htmlBody
     */
    public java.lang.String getHtmlBody() {
        return htmlBody;
    }


    /**
     * Sets the htmlBody value for this SingleEmailMessage.
     * 
     * @param htmlBody
     */
    public void setHtmlBody(java.lang.String htmlBody) {
        this.htmlBody = htmlBody;
    }


    /**
     * Gets the fileAttachments value for this SingleEmailMessage.
     * 
     * @return fileAttachments
     */
    public com.sforce.soap.partner.EmailFileAttachment[] getFileAttachments() {
        return fileAttachments;
    }


    /**
     * Sets the fileAttachments value for this SingleEmailMessage.
     * 
     * @param fileAttachments
     */
    public void setFileAttachments(com.sforce.soap.partner.EmailFileAttachment[] fileAttachments) {
        this.fileAttachments = fileAttachments;
    }

    public com.sforce.soap.partner.EmailFileAttachment getFileAttachments(int i) {
        return this.fileAttachments[i];
    }

    public void setFileAttachments(int i, com.sforce.soap.partner.EmailFileAttachment _value) {
        this.fileAttachments[i] = _value;
    }


    /**
     * Gets the plainTextBody value for this SingleEmailMessage.
     * 
     * @return plainTextBody
     */
    public java.lang.String getPlainTextBody() {
        return plainTextBody;
    }


    /**
     * Sets the plainTextBody value for this SingleEmailMessage.
     * 
     * @param plainTextBody
     */
    public void setPlainTextBody(java.lang.String plainTextBody) {
        this.plainTextBody = plainTextBody;
    }


    /**
     * Gets the targetObjectId value for this SingleEmailMessage.
     * 
     * @return targetObjectId
     */
    public java.lang.String getTargetObjectId() {
        return targetObjectId;
    }


    /**
     * Sets the targetObjectId value for this SingleEmailMessage.
     * 
     * @param targetObjectId
     */
    public void setTargetObjectId(java.lang.String targetObjectId) {
        this.targetObjectId = targetObjectId;
    }


    /**
     * Gets the templateId value for this SingleEmailMessage.
     * 
     * @return templateId
     */
    public java.lang.String getTemplateId() {
        return templateId;
    }


    /**
     * Sets the templateId value for this SingleEmailMessage.
     * 
     * @param templateId
     */
    public void setTemplateId(java.lang.String templateId) {
        this.templateId = templateId;
    }


    /**
     * Gets the toAddresses value for this SingleEmailMessage.
     * 
     * @return toAddresses
     */
    public java.lang.String[] getToAddresses() {
        return toAddresses;
    }


    /**
     * Sets the toAddresses value for this SingleEmailMessage.
     * 
     * @param toAddresses
     */
    public void setToAddresses(java.lang.String[] toAddresses) {
        this.toAddresses = toAddresses;
    }

    public java.lang.String getToAddresses(int i) {
        return this.toAddresses[i];
    }

    public void setToAddresses(int i, java.lang.String _value) {
        this.toAddresses[i] = _value;
    }


    /**
     * Gets the whatId value for this SingleEmailMessage.
     * 
     * @return whatId
     */
    public java.lang.String getWhatId() {
        return whatId;
    }


    /**
     * Sets the whatId value for this SingleEmailMessage.
     * 
     * @param whatId
     */
    public void setWhatId(java.lang.String whatId) {
        this.whatId = whatId;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SingleEmailMessage)) return false;
        SingleEmailMessage other = (SingleEmailMessage) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = super.equals(obj) && 
            ((this.bccAddresses==null && other.getBccAddresses()==null) || 
             (this.bccAddresses!=null &&
              java.util.Arrays.equals(this.bccAddresses, other.getBccAddresses()))) &&
            ((this.ccAddresses==null && other.getCcAddresses()==null) || 
             (this.ccAddresses!=null &&
              java.util.Arrays.equals(this.ccAddresses, other.getCcAddresses()))) &&
            ((this.charset==null && other.getCharset()==null) || 
             (this.charset!=null &&
              this.charset.equals(other.getCharset()))) &&
            ((this.documentAttachments==null && other.getDocumentAttachments()==null) || 
             (this.documentAttachments!=null &&
              java.util.Arrays.equals(this.documentAttachments, other.getDocumentAttachments()))) &&
            ((this.htmlBody==null && other.getHtmlBody()==null) || 
             (this.htmlBody!=null &&
              this.htmlBody.equals(other.getHtmlBody()))) &&
            ((this.fileAttachments==null && other.getFileAttachments()==null) || 
             (this.fileAttachments!=null &&
              java.util.Arrays.equals(this.fileAttachments, other.getFileAttachments()))) &&
            ((this.plainTextBody==null && other.getPlainTextBody()==null) || 
             (this.plainTextBody!=null &&
              this.plainTextBody.equals(other.getPlainTextBody()))) &&
            ((this.targetObjectId==null && other.getTargetObjectId()==null) || 
             (this.targetObjectId!=null &&
              this.targetObjectId.equals(other.getTargetObjectId()))) &&
            ((this.templateId==null && other.getTemplateId()==null) || 
             (this.templateId!=null &&
              this.templateId.equals(other.getTemplateId()))) &&
            ((this.toAddresses==null && other.getToAddresses()==null) || 
             (this.toAddresses!=null &&
              java.util.Arrays.equals(this.toAddresses, other.getToAddresses()))) &&
            ((this.whatId==null && other.getWhatId()==null) || 
             (this.whatId!=null &&
              this.whatId.equals(other.getWhatId())));
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
        if (getBccAddresses() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getBccAddresses());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getBccAddresses(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getCcAddresses() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getCcAddresses());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getCcAddresses(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getCharset() != null) {
            _hashCode += getCharset().hashCode();
        }
        if (getDocumentAttachments() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getDocumentAttachments());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getDocumentAttachments(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getHtmlBody() != null) {
            _hashCode += getHtmlBody().hashCode();
        }
        if (getFileAttachments() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getFileAttachments());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getFileAttachments(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getPlainTextBody() != null) {
            _hashCode += getPlainTextBody().hashCode();
        }
        if (getTargetObjectId() != null) {
            _hashCode += getTargetObjectId().hashCode();
        }
        if (getTemplateId() != null) {
            _hashCode += getTemplateId().hashCode();
        }
        if (getToAddresses() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getToAddresses());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getToAddresses(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getWhatId() != null) {
            _hashCode += getWhatId().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SingleEmailMessage.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "SingleEmailMessage"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("bccAddresses");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "bccAddresses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ccAddresses");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ccAddresses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("charset");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "charset"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("documentAttachments");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "documentAttachments"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("htmlBody");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "htmlBody"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileAttachments");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "fileAttachments"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "EmailFileAttachment"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("plainTextBody");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "plainTextBody"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetObjectId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "targetObjectId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("templateId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "templateId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("toAddresses");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "toAddresses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("whatId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "whatId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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
