
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeSoftphoneLayoutSection complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeSoftphoneLayoutSection">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="entityApiName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="items" type="{urn:partner.soap.sforce.com}DescribeSoftphoneLayoutItem" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeSoftphoneLayoutSection", propOrder = {
    "entityApiName",
    "items"
})
public class DescribeSoftphoneLayoutSection {

    @XmlElement(required = true)
    protected String entityApiName;
    @XmlElement(required = true)
    protected List<DescribeSoftphoneLayoutItem> items;

    /**
     * Gets the value of the entityApiName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEntityApiName() {
        return entityApiName;
    }

    /**
     * Sets the value of the entityApiName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEntityApiName(String value) {
        this.entityApiName = value;
    }

    /**
     * Gets the value of the items property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the items property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getItems().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeSoftphoneLayoutItem }
     * 
     * 
     */
    public List<DescribeSoftphoneLayoutItem> getItems() {
        if (items == null) {
            items = new ArrayList<DescribeSoftphoneLayoutItem>();
        }
        return this.items;
    }

}
