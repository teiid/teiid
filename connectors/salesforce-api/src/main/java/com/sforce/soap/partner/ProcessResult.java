/**
 * ProcessResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class ProcessResult  implements java.io.Serializable {
    private java.lang.String[] actorIds;

    private java.lang.String entityId;

    private com.sforce.soap.partner.Error[] errors;

    private java.lang.String instanceId;

    private java.lang.String instanceStatus;

    private java.lang.String[] newWorkitemIds;

    private boolean success;

    public ProcessResult() {
    }

    public ProcessResult(
           java.lang.String[] actorIds,
           java.lang.String entityId,
           com.sforce.soap.partner.Error[] errors,
           java.lang.String instanceId,
           java.lang.String instanceStatus,
           java.lang.String[] newWorkitemIds,
           boolean success) {
           this.actorIds = actorIds;
           this.entityId = entityId;
           this.errors = errors;
           this.instanceId = instanceId;
           this.instanceStatus = instanceStatus;
           this.newWorkitemIds = newWorkitemIds;
           this.success = success;
    }


    /**
     * Gets the actorIds value for this ProcessResult.
     * 
     * @return actorIds
     */
    public java.lang.String[] getActorIds() {
        return actorIds;
    }


    /**
     * Sets the actorIds value for this ProcessResult.
     * 
     * @param actorIds
     */
    public void setActorIds(java.lang.String[] actorIds) {
        this.actorIds = actorIds;
    }

    public java.lang.String getActorIds(int i) {
        return this.actorIds[i];
    }

    public void setActorIds(int i, java.lang.String _value) {
        this.actorIds[i] = _value;
    }


    /**
     * Gets the entityId value for this ProcessResult.
     * 
     * @return entityId
     */
    public java.lang.String getEntityId() {
        return entityId;
    }


    /**
     * Sets the entityId value for this ProcessResult.
     * 
     * @param entityId
     */
    public void setEntityId(java.lang.String entityId) {
        this.entityId = entityId;
    }


    /**
     * Gets the errors value for this ProcessResult.
     * 
     * @return errors
     */
    public com.sforce.soap.partner.Error[] getErrors() {
        return errors;
    }


    /**
     * Sets the errors value for this ProcessResult.
     * 
     * @param errors
     */
    public void setErrors(com.sforce.soap.partner.Error[] errors) {
        this.errors = errors;
    }

    public com.sforce.soap.partner.Error getErrors(int i) {
        return this.errors[i];
    }

    public void setErrors(int i, com.sforce.soap.partner.Error _value) {
        this.errors[i] = _value;
    }


    /**
     * Gets the instanceId value for this ProcessResult.
     * 
     * @return instanceId
     */
    public java.lang.String getInstanceId() {
        return instanceId;
    }


    /**
     * Sets the instanceId value for this ProcessResult.
     * 
     * @param instanceId
     */
    public void setInstanceId(java.lang.String instanceId) {
        this.instanceId = instanceId;
    }


    /**
     * Gets the instanceStatus value for this ProcessResult.
     * 
     * @return instanceStatus
     */
    public java.lang.String getInstanceStatus() {
        return instanceStatus;
    }


    /**
     * Sets the instanceStatus value for this ProcessResult.
     * 
     * @param instanceStatus
     */
    public void setInstanceStatus(java.lang.String instanceStatus) {
        this.instanceStatus = instanceStatus;
    }


    /**
     * Gets the newWorkitemIds value for this ProcessResult.
     * 
     * @return newWorkitemIds
     */
    public java.lang.String[] getNewWorkitemIds() {
        return newWorkitemIds;
    }


    /**
     * Sets the newWorkitemIds value for this ProcessResult.
     * 
     * @param newWorkitemIds
     */
    public void setNewWorkitemIds(java.lang.String[] newWorkitemIds) {
        this.newWorkitemIds = newWorkitemIds;
    }

    public java.lang.String getNewWorkitemIds(int i) {
        return this.newWorkitemIds[i];
    }

    public void setNewWorkitemIds(int i, java.lang.String _value) {
        this.newWorkitemIds[i] = _value;
    }


    /**
     * Gets the success value for this ProcessResult.
     * 
     * @return success
     */
    public boolean isSuccess() {
        return success;
    }


    /**
     * Sets the success value for this ProcessResult.
     * 
     * @param success
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ProcessResult)) return false;
        ProcessResult other = (ProcessResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.actorIds==null && other.getActorIds()==null) || 
             (this.actorIds!=null &&
              java.util.Arrays.equals(this.actorIds, other.getActorIds()))) &&
            ((this.entityId==null && other.getEntityId()==null) || 
             (this.entityId!=null &&
              this.entityId.equals(other.getEntityId()))) &&
            ((this.errors==null && other.getErrors()==null) || 
             (this.errors!=null &&
              java.util.Arrays.equals(this.errors, other.getErrors()))) &&
            ((this.instanceId==null && other.getInstanceId()==null) || 
             (this.instanceId!=null &&
              this.instanceId.equals(other.getInstanceId()))) &&
            ((this.instanceStatus==null && other.getInstanceStatus()==null) || 
             (this.instanceStatus!=null &&
              this.instanceStatus.equals(other.getInstanceStatus()))) &&
            ((this.newWorkitemIds==null && other.getNewWorkitemIds()==null) || 
             (this.newWorkitemIds!=null &&
              java.util.Arrays.equals(this.newWorkitemIds, other.getNewWorkitemIds()))) &&
            this.success == other.isSuccess();
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
        if (getActorIds() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getActorIds());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getActorIds(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getEntityId() != null) {
            _hashCode += getEntityId().hashCode();
        }
        if (getErrors() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getErrors());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getErrors(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getInstanceId() != null) {
            _hashCode += getInstanceId().hashCode();
        }
        if (getInstanceStatus() != null) {
            _hashCode += getInstanceStatus().hashCode();
        }
        if (getNewWorkitemIds() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getNewWorkitemIds());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getNewWorkitemIds(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        _hashCode += (isSuccess() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(ProcessResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ProcessResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("actorIds");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "actorIds"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("entityId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "entityId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("errors");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "errors"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "Error"));
        elemField.setMinOccurs(0);
        elemField.setNillable(false);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("instanceId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "instanceId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("instanceStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "instanceStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newWorkitemIds");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "newWorkitemIds"));
        elemField.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "ID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        elemField.setMaxOccursUnbounded(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("success");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "success"));
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
