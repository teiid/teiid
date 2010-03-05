
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for SingleEmailMessage complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SingleEmailMessage">
 *   &lt;complexContent>
 *     &lt;extension base="{urn:partner.soap.sforce.com}Email">
 *       &lt;sequence>
 *         &lt;element name="bccAddresses" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="25" minOccurs="0"/>
 *         &lt;element name="ccAddresses" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="25" minOccurs="0"/>
 *         &lt;element name="charset" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="documentAttachments" type="{urn:partner.soap.sforce.com}ID" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="htmlBody" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="inReplyTo" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="fileAttachments" type="{urn:partner.soap.sforce.com}EmailFileAttachment" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="orgWideEmailAddressId" type="{urn:partner.soap.sforce.com}ID" minOccurs="0"/>
 *         &lt;element name="plainTextBody" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="references" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="targetObjectId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="templateId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="toAddresses" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="100" minOccurs="0"/>
 *         &lt;element name="whatId" type="{urn:partner.soap.sforce.com}ID"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SingleEmailMessage", propOrder = {
    "bccAddresses",
    "ccAddresses",
    "charset",
    "documentAttachments",
    "htmlBody",
    "inReplyTo",
    "fileAttachments",
    "orgWideEmailAddressId",
    "plainTextBody",
    "references",
    "targetObjectId",
    "templateId",
    "toAddresses",
    "whatId"
})
public class SingleEmailMessage
    extends Email
{

    @XmlElement(nillable = true)
    protected List<String> bccAddresses;
    @XmlElement(nillable = true)
    protected List<String> ccAddresses;
    @XmlElement(required = true, nillable = true)
    protected String charset;
    protected List<String> documentAttachments;
    @XmlElement(required = true, nillable = true)
    protected String htmlBody;
    @XmlElementRef(name = "inReplyTo", namespace = "urn:partner.soap.sforce.com", type = JAXBElement.class)
    protected JAXBElement<String> inReplyTo;
    protected List<EmailFileAttachment> fileAttachments;
    @XmlElementRef(name = "orgWideEmailAddressId", namespace = "urn:partner.soap.sforce.com", type = JAXBElement.class)
    protected JAXBElement<String> orgWideEmailAddressId;
    @XmlElement(required = true, nillable = true)
    protected String plainTextBody;
    @XmlElementRef(name = "references", namespace = "urn:partner.soap.sforce.com", type = JAXBElement.class)
    protected JAXBElement<String> references;
    @XmlElement(required = true, nillable = true)
    protected String targetObjectId;
    @XmlElement(required = true, nillable = true)
    protected String templateId;
    @XmlElement(nillable = true)
    protected List<String> toAddresses;
    @XmlElement(required = true, nillable = true)
    protected String whatId;

    /**
     * Gets the value of the bccAddresses property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the bccAddresses property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getBccAddresses().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getBccAddresses() {
        if (bccAddresses == null) {
            bccAddresses = new ArrayList<String>();
        }
        return this.bccAddresses;
    }

    /**
     * Gets the value of the ccAddresses property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the ccAddresses property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCcAddresses().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getCcAddresses() {
        if (ccAddresses == null) {
            ccAddresses = new ArrayList<String>();
        }
        return this.ccAddresses;
    }

    /**
     * Gets the value of the charset property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCharset() {
        return charset;
    }

    /**
     * Sets the value of the charset property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCharset(String value) {
        this.charset = value;
    }

    /**
     * Gets the value of the documentAttachments property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the documentAttachments property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDocumentAttachments().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getDocumentAttachments() {
        if (documentAttachments == null) {
            documentAttachments = new ArrayList<String>();
        }
        return this.documentAttachments;
    }

    /**
     * Gets the value of the htmlBody property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getHtmlBody() {
        return htmlBody;
    }

    /**
     * Sets the value of the htmlBody property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setHtmlBody(String value) {
        this.htmlBody = value;
    }

    /**
     * Gets the value of the inReplyTo property.
     * 
     * @return
     *     possible object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public JAXBElement<String> getInReplyTo() {
        return inReplyTo;
    }

    /**
     * Sets the value of the inReplyTo property.
     * 
     * @param value
     *     allowed object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public void setInReplyTo(JAXBElement<String> value) {
        this.inReplyTo = ((JAXBElement<String> ) value);
    }

    /**
     * Gets the value of the fileAttachments property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the fileAttachments property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getFileAttachments().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link EmailFileAttachment }
     * 
     * 
     */
    public List<EmailFileAttachment> getFileAttachments() {
        if (fileAttachments == null) {
            fileAttachments = new ArrayList<EmailFileAttachment>();
        }
        return this.fileAttachments;
    }

    /**
     * Gets the value of the orgWideEmailAddressId property.
     * 
     * @return
     *     possible object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public JAXBElement<String> getOrgWideEmailAddressId() {
        return orgWideEmailAddressId;
    }

    /**
     * Sets the value of the orgWideEmailAddressId property.
     * 
     * @param value
     *     allowed object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public void setOrgWideEmailAddressId(JAXBElement<String> value) {
        this.orgWideEmailAddressId = ((JAXBElement<String> ) value);
    }

    /**
     * Gets the value of the plainTextBody property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPlainTextBody() {
        return plainTextBody;
    }

    /**
     * Sets the value of the plainTextBody property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPlainTextBody(String value) {
        this.plainTextBody = value;
    }

    /**
     * Gets the value of the references property.
     * 
     * @return
     *     possible object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public JAXBElement<String> getReferences() {
        return references;
    }

    /**
     * Sets the value of the references property.
     * 
     * @param value
     *     allowed object is
     *     {@link JAXBElement }{@code <}{@link String }{@code >}
     *     
     */
    public void setReferences(JAXBElement<String> value) {
        this.references = ((JAXBElement<String> ) value);
    }

    /**
     * Gets the value of the targetObjectId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTargetObjectId() {
        return targetObjectId;
    }

    /**
     * Sets the value of the targetObjectId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTargetObjectId(String value) {
        this.targetObjectId = value;
    }

    /**
     * Gets the value of the templateId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTemplateId() {
        return templateId;
    }

    /**
     * Sets the value of the templateId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTemplateId(String value) {
        this.templateId = value;
    }

    /**
     * Gets the value of the toAddresses property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the toAddresses property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getToAddresses().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getToAddresses() {
        if (toAddresses == null) {
            toAddresses = new ArrayList<String>();
        }
        return this.toAddresses;
    }

    /**
     * Gets the value of the whatId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getWhatId() {
        return whatId;
    }

    /**
     * Sets the value of the whatId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setWhatId(String value) {
        this.whatId = value;
    }

}
