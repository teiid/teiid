
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for SearchResult complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="SearchResult">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="searchRecords" type="{urn:partner.soap.sforce.com}SearchRecord" maxOccurs="unbounded" minOccurs="0"/>
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
@XmlType(name = "SearchResult", propOrder = {
    "searchRecords",
    "sforceReserved"
})
public class SearchResult {

    protected List<SearchRecord> searchRecords;
    protected String sforceReserved;

    /**
     * Gets the value of the searchRecords property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the searchRecords property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getSearchRecords().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link SearchRecord }
     * 
     * 
     */
    public List<SearchRecord> getSearchRecords() {
        if (searchRecords == null) {
            searchRecords = new ArrayList<SearchRecord>();
        }
        return this.searchRecords;
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
