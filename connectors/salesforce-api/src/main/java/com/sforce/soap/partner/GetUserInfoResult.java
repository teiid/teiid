/**
 * GetUserInfoResult.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.sforce.soap.partner;

public class GetUserInfoResult  implements java.io.Serializable {
    private boolean accessibilityMode;

    private java.lang.String currencySymbol;

    private java.lang.String orgDefaultCurrencyIsoCode;

    private boolean orgHasPersonAccounts;

    private java.lang.String organizationId;

    private boolean organizationMultiCurrency;

    private java.lang.String organizationName;

    private java.lang.String profileId;

    private java.lang.String roleId;

    private java.lang.String userDefaultCurrencyIsoCode;

    private java.lang.String userEmail;

    private java.lang.String userFullName;

    private java.lang.String userId;

    private java.lang.String userLanguage;

    private java.lang.String userLocale;

    private java.lang.String userName;

    private java.lang.String userTimeZone;

    private java.lang.String userType;

    private java.lang.String userUiSkin;

    public GetUserInfoResult() {
    }

    public GetUserInfoResult(
           boolean accessibilityMode,
           java.lang.String currencySymbol,
           java.lang.String orgDefaultCurrencyIsoCode,
           boolean orgHasPersonAccounts,
           java.lang.String organizationId,
           boolean organizationMultiCurrency,
           java.lang.String organizationName,
           java.lang.String profileId,
           java.lang.String roleId,
           java.lang.String userDefaultCurrencyIsoCode,
           java.lang.String userEmail,
           java.lang.String userFullName,
           java.lang.String userId,
           java.lang.String userLanguage,
           java.lang.String userLocale,
           java.lang.String userName,
           java.lang.String userTimeZone,
           java.lang.String userType,
           java.lang.String userUiSkin) {
           this.accessibilityMode = accessibilityMode;
           this.currencySymbol = currencySymbol;
           this.orgDefaultCurrencyIsoCode = orgDefaultCurrencyIsoCode;
           this.orgHasPersonAccounts = orgHasPersonAccounts;
           this.organizationId = organizationId;
           this.organizationMultiCurrency = organizationMultiCurrency;
           this.organizationName = organizationName;
           this.profileId = profileId;
           this.roleId = roleId;
           this.userDefaultCurrencyIsoCode = userDefaultCurrencyIsoCode;
           this.userEmail = userEmail;
           this.userFullName = userFullName;
           this.userId = userId;
           this.userLanguage = userLanguage;
           this.userLocale = userLocale;
           this.userName = userName;
           this.userTimeZone = userTimeZone;
           this.userType = userType;
           this.userUiSkin = userUiSkin;
    }


    /**
     * Gets the accessibilityMode value for this GetUserInfoResult.
     * 
     * @return accessibilityMode
     */
    public boolean isAccessibilityMode() {
        return accessibilityMode;
    }


    /**
     * Sets the accessibilityMode value for this GetUserInfoResult.
     * 
     * @param accessibilityMode
     */
    public void setAccessibilityMode(boolean accessibilityMode) {
        this.accessibilityMode = accessibilityMode;
    }


    /**
     * Gets the currencySymbol value for this GetUserInfoResult.
     * 
     * @return currencySymbol
     */
    public java.lang.String getCurrencySymbol() {
        return currencySymbol;
    }


    /**
     * Sets the currencySymbol value for this GetUserInfoResult.
     * 
     * @param currencySymbol
     */
    public void setCurrencySymbol(java.lang.String currencySymbol) {
        this.currencySymbol = currencySymbol;
    }


    /**
     * Gets the orgDefaultCurrencyIsoCode value for this GetUserInfoResult.
     * 
     * @return orgDefaultCurrencyIsoCode
     */
    public java.lang.String getOrgDefaultCurrencyIsoCode() {
        return orgDefaultCurrencyIsoCode;
    }


    /**
     * Sets the orgDefaultCurrencyIsoCode value for this GetUserInfoResult.
     * 
     * @param orgDefaultCurrencyIsoCode
     */
    public void setOrgDefaultCurrencyIsoCode(java.lang.String orgDefaultCurrencyIsoCode) {
        this.orgDefaultCurrencyIsoCode = orgDefaultCurrencyIsoCode;
    }


    /**
     * Gets the orgHasPersonAccounts value for this GetUserInfoResult.
     * 
     * @return orgHasPersonAccounts
     */
    public boolean isOrgHasPersonAccounts() {
        return orgHasPersonAccounts;
    }


    /**
     * Sets the orgHasPersonAccounts value for this GetUserInfoResult.
     * 
     * @param orgHasPersonAccounts
     */
    public void setOrgHasPersonAccounts(boolean orgHasPersonAccounts) {
        this.orgHasPersonAccounts = orgHasPersonAccounts;
    }


    /**
     * Gets the organizationId value for this GetUserInfoResult.
     * 
     * @return organizationId
     */
    public java.lang.String getOrganizationId() {
        return organizationId;
    }


    /**
     * Sets the organizationId value for this GetUserInfoResult.
     * 
     * @param organizationId
     */
    public void setOrganizationId(java.lang.String organizationId) {
        this.organizationId = organizationId;
    }


    /**
     * Gets the organizationMultiCurrency value for this GetUserInfoResult.
     * 
     * @return organizationMultiCurrency
     */
    public boolean isOrganizationMultiCurrency() {
        return organizationMultiCurrency;
    }


    /**
     * Sets the organizationMultiCurrency value for this GetUserInfoResult.
     * 
     * @param organizationMultiCurrency
     */
    public void setOrganizationMultiCurrency(boolean organizationMultiCurrency) {
        this.organizationMultiCurrency = organizationMultiCurrency;
    }


    /**
     * Gets the organizationName value for this GetUserInfoResult.
     * 
     * @return organizationName
     */
    public java.lang.String getOrganizationName() {
        return organizationName;
    }


    /**
     * Sets the organizationName value for this GetUserInfoResult.
     * 
     * @param organizationName
     */
    public void setOrganizationName(java.lang.String organizationName) {
        this.organizationName = organizationName;
    }


    /**
     * Gets the profileId value for this GetUserInfoResult.
     * 
     * @return profileId
     */
    public java.lang.String getProfileId() {
        return profileId;
    }


    /**
     * Sets the profileId value for this GetUserInfoResult.
     * 
     * @param profileId
     */
    public void setProfileId(java.lang.String profileId) {
        this.profileId = profileId;
    }


    /**
     * Gets the roleId value for this GetUserInfoResult.
     * 
     * @return roleId
     */
    public java.lang.String getRoleId() {
        return roleId;
    }


    /**
     * Sets the roleId value for this GetUserInfoResult.
     * 
     * @param roleId
     */
    public void setRoleId(java.lang.String roleId) {
        this.roleId = roleId;
    }


    /**
     * Gets the userDefaultCurrencyIsoCode value for this GetUserInfoResult.
     * 
     * @return userDefaultCurrencyIsoCode
     */
    public java.lang.String getUserDefaultCurrencyIsoCode() {
        return userDefaultCurrencyIsoCode;
    }


    /**
     * Sets the userDefaultCurrencyIsoCode value for this GetUserInfoResult.
     * 
     * @param userDefaultCurrencyIsoCode
     */
    public void setUserDefaultCurrencyIsoCode(java.lang.String userDefaultCurrencyIsoCode) {
        this.userDefaultCurrencyIsoCode = userDefaultCurrencyIsoCode;
    }


    /**
     * Gets the userEmail value for this GetUserInfoResult.
     * 
     * @return userEmail
     */
    public java.lang.String getUserEmail() {
        return userEmail;
    }


    /**
     * Sets the userEmail value for this GetUserInfoResult.
     * 
     * @param userEmail
     */
    public void setUserEmail(java.lang.String userEmail) {
        this.userEmail = userEmail;
    }


    /**
     * Gets the userFullName value for this GetUserInfoResult.
     * 
     * @return userFullName
     */
    public java.lang.String getUserFullName() {
        return userFullName;
    }


    /**
     * Sets the userFullName value for this GetUserInfoResult.
     * 
     * @param userFullName
     */
    public void setUserFullName(java.lang.String userFullName) {
        this.userFullName = userFullName;
    }


    /**
     * Gets the userId value for this GetUserInfoResult.
     * 
     * @return userId
     */
    public java.lang.String getUserId() {
        return userId;
    }


    /**
     * Sets the userId value for this GetUserInfoResult.
     * 
     * @param userId
     */
    public void setUserId(java.lang.String userId) {
        this.userId = userId;
    }


    /**
     * Gets the userLanguage value for this GetUserInfoResult.
     * 
     * @return userLanguage
     */
    public java.lang.String getUserLanguage() {
        return userLanguage;
    }


    /**
     * Sets the userLanguage value for this GetUserInfoResult.
     * 
     * @param userLanguage
     */
    public void setUserLanguage(java.lang.String userLanguage) {
        this.userLanguage = userLanguage;
    }


    /**
     * Gets the userLocale value for this GetUserInfoResult.
     * 
     * @return userLocale
     */
    public java.lang.String getUserLocale() {
        return userLocale;
    }


    /**
     * Sets the userLocale value for this GetUserInfoResult.
     * 
     * @param userLocale
     */
    public void setUserLocale(java.lang.String userLocale) {
        this.userLocale = userLocale;
    }


    /**
     * Gets the userName value for this GetUserInfoResult.
     * 
     * @return userName
     */
    public java.lang.String getUserName() {
        return userName;
    }


    /**
     * Sets the userName value for this GetUserInfoResult.
     * 
     * @param userName
     */
    public void setUserName(java.lang.String userName) {
        this.userName = userName;
    }


    /**
     * Gets the userTimeZone value for this GetUserInfoResult.
     * 
     * @return userTimeZone
     */
    public java.lang.String getUserTimeZone() {
        return userTimeZone;
    }


    /**
     * Sets the userTimeZone value for this GetUserInfoResult.
     * 
     * @param userTimeZone
     */
    public void setUserTimeZone(java.lang.String userTimeZone) {
        this.userTimeZone = userTimeZone;
    }


    /**
     * Gets the userType value for this GetUserInfoResult.
     * 
     * @return userType
     */
    public java.lang.String getUserType() {
        return userType;
    }


    /**
     * Sets the userType value for this GetUserInfoResult.
     * 
     * @param userType
     */
    public void setUserType(java.lang.String userType) {
        this.userType = userType;
    }


    /**
     * Gets the userUiSkin value for this GetUserInfoResult.
     * 
     * @return userUiSkin
     */
    public java.lang.String getUserUiSkin() {
        return userUiSkin;
    }


    /**
     * Sets the userUiSkin value for this GetUserInfoResult.
     * 
     * @param userUiSkin
     */
    public void setUserUiSkin(java.lang.String userUiSkin) {
        this.userUiSkin = userUiSkin;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof GetUserInfoResult)) return false;
        GetUserInfoResult other = (GetUserInfoResult) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.accessibilityMode == other.isAccessibilityMode() &&
            ((this.currencySymbol==null && other.getCurrencySymbol()==null) || 
             (this.currencySymbol!=null &&
              this.currencySymbol.equals(other.getCurrencySymbol()))) &&
            ((this.orgDefaultCurrencyIsoCode==null && other.getOrgDefaultCurrencyIsoCode()==null) || 
             (this.orgDefaultCurrencyIsoCode!=null &&
              this.orgDefaultCurrencyIsoCode.equals(other.getOrgDefaultCurrencyIsoCode()))) &&
            this.orgHasPersonAccounts == other.isOrgHasPersonAccounts() &&
            ((this.organizationId==null && other.getOrganizationId()==null) || 
             (this.organizationId!=null &&
              this.organizationId.equals(other.getOrganizationId()))) &&
            this.organizationMultiCurrency == other.isOrganizationMultiCurrency() &&
            ((this.organizationName==null && other.getOrganizationName()==null) || 
             (this.organizationName!=null &&
              this.organizationName.equals(other.getOrganizationName()))) &&
            ((this.profileId==null && other.getProfileId()==null) || 
             (this.profileId!=null &&
              this.profileId.equals(other.getProfileId()))) &&
            ((this.roleId==null && other.getRoleId()==null) || 
             (this.roleId!=null &&
              this.roleId.equals(other.getRoleId()))) &&
            ((this.userDefaultCurrencyIsoCode==null && other.getUserDefaultCurrencyIsoCode()==null) || 
             (this.userDefaultCurrencyIsoCode!=null &&
              this.userDefaultCurrencyIsoCode.equals(other.getUserDefaultCurrencyIsoCode()))) &&
            ((this.userEmail==null && other.getUserEmail()==null) || 
             (this.userEmail!=null &&
              this.userEmail.equals(other.getUserEmail()))) &&
            ((this.userFullName==null && other.getUserFullName()==null) || 
             (this.userFullName!=null &&
              this.userFullName.equals(other.getUserFullName()))) &&
            ((this.userId==null && other.getUserId()==null) || 
             (this.userId!=null &&
              this.userId.equals(other.getUserId()))) &&
            ((this.userLanguage==null && other.getUserLanguage()==null) || 
             (this.userLanguage!=null &&
              this.userLanguage.equals(other.getUserLanguage()))) &&
            ((this.userLocale==null && other.getUserLocale()==null) || 
             (this.userLocale!=null &&
              this.userLocale.equals(other.getUserLocale()))) &&
            ((this.userName==null && other.getUserName()==null) || 
             (this.userName!=null &&
              this.userName.equals(other.getUserName()))) &&
            ((this.userTimeZone==null && other.getUserTimeZone()==null) || 
             (this.userTimeZone!=null &&
              this.userTimeZone.equals(other.getUserTimeZone()))) &&
            ((this.userType==null && other.getUserType()==null) || 
             (this.userType!=null &&
              this.userType.equals(other.getUserType()))) &&
            ((this.userUiSkin==null && other.getUserUiSkin()==null) || 
             (this.userUiSkin!=null &&
              this.userUiSkin.equals(other.getUserUiSkin())));
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
        _hashCode += (isAccessibilityMode() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getCurrencySymbol() != null) {
            _hashCode += getCurrencySymbol().hashCode();
        }
        if (getOrgDefaultCurrencyIsoCode() != null) {
            _hashCode += getOrgDefaultCurrencyIsoCode().hashCode();
        }
        _hashCode += (isOrgHasPersonAccounts() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getOrganizationId() != null) {
            _hashCode += getOrganizationId().hashCode();
        }
        _hashCode += (isOrganizationMultiCurrency() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getOrganizationName() != null) {
            _hashCode += getOrganizationName().hashCode();
        }
        if (getProfileId() != null) {
            _hashCode += getProfileId().hashCode();
        }
        if (getRoleId() != null) {
            _hashCode += getRoleId().hashCode();
        }
        if (getUserDefaultCurrencyIsoCode() != null) {
            _hashCode += getUserDefaultCurrencyIsoCode().hashCode();
        }
        if (getUserEmail() != null) {
            _hashCode += getUserEmail().hashCode();
        }
        if (getUserFullName() != null) {
            _hashCode += getUserFullName().hashCode();
        }
        if (getUserId() != null) {
            _hashCode += getUserId().hashCode();
        }
        if (getUserLanguage() != null) {
            _hashCode += getUserLanguage().hashCode();
        }
        if (getUserLocale() != null) {
            _hashCode += getUserLocale().hashCode();
        }
        if (getUserName() != null) {
            _hashCode += getUserName().hashCode();
        }
        if (getUserTimeZone() != null) {
            _hashCode += getUserTimeZone().hashCode();
        }
        if (getUserType() != null) {
            _hashCode += getUserType().hashCode();
        }
        if (getUserUiSkin() != null) {
            _hashCode += getUserUiSkin().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(GetUserInfoResult.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "GetUserInfoResult"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("accessibilityMode");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "accessibilityMode"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("currencySymbol");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "currencySymbol"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("orgDefaultCurrencyIsoCode");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "orgDefaultCurrencyIsoCode"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("orgHasPersonAccounts");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "orgHasPersonAccounts"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("organizationId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "organizationId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("organizationMultiCurrency");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "organizationMultiCurrency"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("organizationName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "organizationName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("profileId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "profileId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("roleId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "roleId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userDefaultCurrencyIsoCode");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userDefaultCurrencyIsoCode"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userEmail");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userEmail"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userFullName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userFullName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userId");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userLanguage");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userLanguage"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userLocale");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userLocale"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userName");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userName"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userTimeZone");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userTimeZone"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userType");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userUiSkin");
        elemField.setXmlName(new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "userUiSkin"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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
