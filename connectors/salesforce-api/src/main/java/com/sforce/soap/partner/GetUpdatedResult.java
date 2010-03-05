
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.XMLGregorianCalendar;


/**
 * <p>Java class for GetUpdatedResult complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="GetUpdatedResult">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="ids" type="{urn:partner.soap.sforce.com}ID" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="latestDateCovered" type="{http://www.w3.org/2001/XMLSchema}dateTime"/>
 *         &lt;element name="sforceReserved" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetUpdatedResult", propOrder = {
    "ids",
    "latestDateCovered",
    "sforceReserved"
})
public class GetUpdatedResult {

    protected List<String> ids;
    @XmlElement(required = true)
    @XmlSchemaType(name = "dateTime")
    protected XMLGregorianCalendar latestDateCovered;
    protected String sforceReserved;

    /**
     * Gets the value of the ids property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the ids property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getIds().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getIds() {
        if (ids == null) {
            ids = new ArrayList<String>();
        }
        return this.ids;
    }

    /**
     * Gets the value of the latestDateCovered property.
     * 
     * @return
     *     possible object is
     *     {@link XMLGregorianCalendar }
     *     
     */
    public XMLGregorianCalendar getLatestDateCovered() {
        return latestDateCovered;
    }

    /**
     * Sets the value of the latestDateCovered property.
     * 
     * @param value
     *     allowed object is
     *     {@link XMLGregorianCalendar }
     *     
     */
    public void setLatestDateCovered(XMLGregorianCalendar value) {
        this.latestDateCovered = value;
    }

    /**
     * Gets the value of the sforceReserved property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSforceReserved() {
        return sforceReserved;
    }

    /**
     * Sets the value of the sforceReserved property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSforceReserved(String value) {
        this.sforceReserved = value;
    }

}
