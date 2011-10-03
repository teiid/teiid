
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeSoftphoneLayoutCallType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeSoftphoneLayoutCallType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="infoFields" type="{urn:partner.soap.sforce.com}DescribeSoftphoneLayoutInfoField" maxOccurs="unbounded"/>
 *         &lt;element name="name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="screenPopOptions" type="{urn:partner.soap.sforce.com}DescribeSoftphoneScreenPopOption" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="screenPopsOpenWithin" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="sections" type="{urn:partner.soap.sforce.com}DescribeSoftphoneLayoutSection" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeSoftphoneLayoutCallType", propOrder = {
    "infoFields",
    "name",
    "screenPopOptions",
    "screenPopsOpenWithin",
    "sections"
})
public class DescribeSoftphoneLayoutCallType {

    @XmlElement(required = true)
    protected List<DescribeSoftphoneLayoutInfoField> infoFields;
    @XmlElement(required = true)
    protected String name;
    protected List<DescribeSoftphoneScreenPopOption> screenPopOptions;
    protected String screenPopsOpenWithin;
    protected List<DescribeSoftphoneLayoutSection> sections;

    /**
     * Gets the value of the infoFields property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the infoFields property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getInfoFields().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeSoftphoneLayoutInfoField }
     * 
     * 
     */
    public List<DescribeSoftphoneLayoutInfoField> getInfoFields() {
        if (infoFields == null) {
            infoFields = new ArrayList<DescribeSoftphoneLayoutInfoField>();
        }
        return this.infoFields;
    }

    /**
     * Gets the value of the name property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setName(String value) {
        this.name = value;
    }

    /**
     * Gets the value of the screenPopOptions property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the screenPopOptions property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getScreenPopOptions().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeSoftphoneScreenPopOption }
     * 
     * 
     */
    public List<DescribeSoftphoneScreenPopOption> getScreenPopOptions() {
        if (screenPopOptions == null) {
            screenPopOptions = new ArrayList<DescribeSoftphoneScreenPopOption>();
        }
        return this.screenPopOptions;
    }

    /**
     * Gets the value of the screenPopsOpenWithin property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getScreenPopsOpenWithin() {
        return screenPopsOpenWithin;
    }

    /**
     * Sets the value of the screenPopsOpenWithin property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setScreenPopsOpenWithin(String value) {
        this.screenPopsOpenWithin = value;
    }

    /**
     * Gets the value of the sections property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the sections property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSections().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeSoftphoneLayoutSection }
     * 
     * 
     */
    public List<DescribeSoftphoneLayoutSection> getSections() {
        if (sections == null) {
            sections = new ArrayList<DescribeSoftphoneLayoutSection>();
        }
        return this.sections;
    }

}
