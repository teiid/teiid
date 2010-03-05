
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for PicklistForRecordType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PicklistForRecordType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="picklistName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="picklistValues" type="{urn:partner.soap.sforce.com}PicklistEntry" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PicklistForRecordType", propOrder = {
    "picklistName",
    "picklistValues"
})
public class PicklistForRecordType {

    @XmlElement(required = true)
    protected String picklistName;
    @XmlElement(nillable = true)
    protected List<PicklistEntry> picklistValues;

    /**
     * Gets the value of the picklistName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPicklistName() {
        return picklistName;
    }

    /**
     * Sets the value of the picklistName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPicklistName(String value) {
        this.picklistName = value;
    }

    /**
     * Gets the value of the picklistValues property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the picklistValues property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getPicklistValues().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link PicklistEntry }
     * 
     * 
     */
    public List<PicklistEntry> getPicklistValues() {
        if (picklistValues == null) {
            picklistValues = new ArrayList<PicklistEntry>();
        }
        return this.picklistValues;
    }

}
