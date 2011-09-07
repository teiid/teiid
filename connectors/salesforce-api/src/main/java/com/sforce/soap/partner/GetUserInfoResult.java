
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for GetUserInfoResult complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="GetUserInfoResult">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="accessibilityMode" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="currencySymbol" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="orgAttachmentFileSizeLimit" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="orgDefaultCurrencyIsoCode" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="orgDisallowHtmlAttachments" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="orgHasPersonAccounts" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="organizationId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="organizationMultiCurrency" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="organizationName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="profileId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="roleId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="sessionSecondsValid" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="userDefaultCurrencyIsoCode" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userEmail" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userFullName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="userLanguage" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userLocale" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userTimeZone" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="userUiSkin" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetUserInfoResult", propOrder = {
    "accessibilityMode",
    "currencySymbol",
    "orgAttachmentFileSizeLimit",
    "orgDefaultCurrencyIsoCode",
    "orgDisallowHtmlAttachments",
    "orgHasPersonAccounts",
    "organizationId",
    "organizationMultiCurrency",
    "organizationName",
    "profileId",
    "roleId",
    "sessionSecondsValid",
    "userDefaultCurrencyIsoCode",
    "userEmail",
    "userFullName",
    "userId",
    "userLanguage",
    "userLocale",
    "userName",
    "userTimeZone",
    "userType",
    "userUiSkin"
})
public class GetUserInfoResult {

    protected boolean accessibilityMode;
    @XmlElement(required = true, nillable = true)
    protected String currencySymbol;
    protected int orgAttachmentFileSizeLimit;
    @XmlElement(required = true, nillable = true)
    protected String orgDefaultCurrencyIsoCode;
    protected boolean orgDisallowHtmlAttachments;
    protected boolean orgHasPersonAccounts;
    @XmlElement(required = true)
    protected String organizationId;
    protected boolean organizationMultiCurrency;
    @XmlElement(required = true)
    protected String organizationName;
    @XmlElement(required = true)
    protected String profileId;
    @XmlElement(required = true, nillable = true)
    protected String roleId;
    protected int sessionSecondsValid;
    @XmlElement(required = true, nillable = true)
    protected String userDefaultCurrencyIsoCode;
    @XmlElement(required = true)
    protected String userEmail;
    @XmlElement(required = true)
    protected String userFullName;
    @XmlElement(required = true)
    protected String userId;
    @XmlElement(required = true)
    protected String userLanguage;
    @XmlElement(required = true)
    protected String userLocale;
    @XmlElement(required = true)
    protected String userName;
    @XmlElement(required = true)
    protected String userTimeZone;
    @XmlElement(required = true)
    protected String userType;
    @XmlElement(required = true)
    protected String userUiSkin;

    /**
     * Gets the value of the accessibilityMode property.
     * 
     */
    public boolean isAccessibilityMode() {
        return accessibilityMode;
    }

    /**
     * Sets the value of the accessibilityMode property.
     * 
     */
    public void setAccessibilityMode(boolean value) {
        this.accessibilityMode = value;
    }

    /**
     * Gets the value of the currencySymbol property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCurrencySymbol() {
        return currencySymbol;
    }

    /**
     * Sets the value of the currencySymbol property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCurrencySymbol(String value) {
        this.currencySymbol = value;
    }

    /**
     * Gets the value of the orgAttachmentFileSizeLimit property.
     * 
     */
    public int getOrgAttachmentFileSizeLimit() {
        return orgAttachmentFileSizeLimit;
    }

    /**
     * Sets the value of the orgAttachmentFileSizeLimit property.
     * 
     */
    public void setOrgAttachmentFileSizeLimit(int value) {
        this.orgAttachmentFileSizeLimit = value;
    }

    /**
     * Gets the value of the orgDefaultCurrencyIsoCode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getOrgDefaultCurrencyIsoCode() {
        return orgDefaultCurrencyIsoCode;
    }

    /**
     * Sets the value of the orgDefaultCurrencyIsoCode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setOrgDefaultCurrencyIsoCode(String value) {
        this.orgDefaultCurrencyIsoCode = value;
    }

    /**
     * Gets the value of the orgDisallowHtmlAttachments property.
     * 
     */
    public boolean isOrgDisallowHtmlAttachments() {
        return orgDisallowHtmlAttachments;
    }

    /**
     * Sets the value of the orgDisallowHtmlAttachments property.
     * 
     */
    public void setOrgDisallowHtmlAttachments(boolean value) {
        this.orgDisallowHtmlAttachments = value;
    }

    /**
     * Gets the value of the orgHasPersonAccounts property.
     * 
     */
    public boolean isOrgHasPersonAccounts() {
        return orgHasPersonAccounts;
    }

    /**
     * Sets the value of the orgHasPersonAccounts property.
     * 
     */
    public void setOrgHasPersonAccounts(boolean value) {
        this.orgHasPersonAccounts = value;
    }

    /**
     * Gets the value of the organizationId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getOrganizationId() {
        return organizationId;
    }

    /**
     * Sets the value of the organizationId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setOrganizationId(String value) {
        this.organizationId = value;
    }

    /**
     * Gets the value of the organizationMultiCurrency property.
     * 
     */
    public boolean isOrganizationMultiCurrency() {
        return organizationMultiCurrency;
    }

    /**
     * Sets the value of the organizationMultiCurrency property.
     * 
     */
    public void setOrganizationMultiCurrency(boolean value) {
        this.organizationMultiCurrency = value;
    }

    /**
     * Gets the value of the organizationName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getOrganizationName() {
        return organizationName;
    }

    /**
     * Sets the value of the organizationName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setOrganizationName(String value) {
        this.organizationName = value;
    }

    /**
     * Gets the value of the profileId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getProfileId() {
        return profileId;
    }

    /**
     * Sets the value of the profileId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setProfileId(String value) {
        this.profileId = value;
    }

    /**
     * Gets the value of the roleId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getRoleId() {
        return roleId;
    }

    /**
     * Sets the value of the roleId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setRoleId(String value) {
        this.roleId = value;
    }

    /**
     * Gets the value of the sessionSecondsValid property.
     * 
     */
    public int getSessionSecondsValid() {
        return sessionSecondsValid;
    }

    /**
     * Sets the value of the sessionSecondsValid property.
     * 
     */
    public void setSessionSecondsValid(int value) {
        this.sessionSecondsValid = value;
    }

    /**
     * Gets the value of the userDefaultCurrencyIsoCode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserDefaultCurrencyIsoCode() {
        return userDefaultCurrencyIsoCode;
    }

    /**
     * Sets the value of the userDefaultCurrencyIsoCode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserDefaultCurrencyIsoCode(String value) {
        this.userDefaultCurrencyIsoCode = value;
    }

    /**
     * Gets the value of the userEmail property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserEmail() {
        return userEmail;
    }

    /**
     * Sets the value of the userEmail property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserEmail(String value) {
        this.userEmail = value;
    }

    /**
     * Gets the value of the userFullName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserFullName() {
        return userFullName;
    }

    /**
     * Sets the value of the userFullName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserFullName(String value) {
        this.userFullName = value;
    }

    /**
     * Gets the value of the userId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Sets the value of the userId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserId(String value) {
        this.userId = value;
    }

    /**
     * Gets the value of the userLanguage property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserLanguage() {
        return userLanguage;
    }

    /**
     * Sets the value of the userLanguage property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserLanguage(String value) {
        this.userLanguage = value;
    }

    /**
     * Gets the value of the userLocale property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserLocale() {
        return userLocale;
    }

    /**
     * Sets the value of the userLocale property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserLocale(String value) {
        this.userLocale = value;
    }

    /**
     * Gets the value of the userName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the value of the userName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserName(String value) {
        this.userName = value;
    }

    /**
     * Gets the value of the userTimeZone property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserTimeZone() {
        return userTimeZone;
    }

    /**
     * Sets the value of the userTimeZone property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserTimeZone(String value) {
        this.userTimeZone = value;
    }

    /**
     * Gets the value of the userType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserType() {
        return userType;
    }

    /**
     * Sets the value of the userType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserType(String value) {
        this.userType = value;
    }

    /**
     * Gets the value of the userUiSkin property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUserUiSkin() {
        return userUiSkin;
    }

    /**
     * Sets the value of the userUiSkin property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUserUiSkin(String value) {
        this.userUiSkin = value;
    }

}
