
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeSoftphoneScreenPopOption complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeSoftphoneScreenPopOption">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="matchType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="screenPopData" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="screenPopType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeSoftphoneScreenPopOption", propOrder = {
    "matchType",
    "screenPopData",
    "screenPopType"
})
public class DescribeSoftphoneScreenPopOption {

    @XmlElement(required = true)
    protected String matchType;
    @XmlElement(required = true)
    protected String screenPopData;
    @XmlElement(required = true)
    protected String screenPopType;

    /**
     * Gets the value of the matchType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMatchType() {
        return matchType;
    }

    /**
     * Sets the value of the matchType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMatchType(String value) {
        this.matchType = value;
    }

    /**
     * Gets the value of the screenPopData property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getScreenPopData() {
        return screenPopData;
    }

    /**
     * Sets the value of the screenPopData property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setScreenPopData(String value) {
        this.screenPopData = value;
    }

    /**
     * Gets the value of the screenPopType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getScreenPopType() {
        return screenPopType;
    }

    /**
     * Sets the value of the screenPopType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setScreenPopType(String value) {
        this.screenPopType = value;
    }

}
