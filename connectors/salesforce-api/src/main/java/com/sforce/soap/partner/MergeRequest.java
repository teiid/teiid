
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import com.sforce.soap.partner.sobject.SObject;


/**
 * <p>Java class for MergeRequest complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="MergeRequest">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="masterRecord" type="{urn:sobject.partner.soap.sforce.com}sObject"/>
 *         &lt;element name="recordToMergeIds" type="{urn:partner.soap.sforce.com}ID" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "MergeRequest", propOrder = {
    "masterRecord",
    "recordToMergeIds"
})
public class MergeRequest {

    @XmlElement(required = true)
    protected SObject masterRecord;
    @XmlElement(required = true)
    protected List<String> recordToMergeIds;

    /**
     * Gets the value of the masterRecord property.
     * 
     * @return
     *     possible object is
     *     {@link SObject }
     *     
     */
    public SObject getMasterRecord() {
        return masterRecord;
    }

    /**
     * Sets the value of the masterRecord property.
     * 
     * @param value
     *     allowed object is
     *     {@link SObject }
     *     
     */
    public void setMasterRecord(SObject value) {
        this.masterRecord = value;
    }

    /**
     * Gets the value of the recordToMergeIds property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the recordToMergeIds property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getRecordToMergeIds().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getRecordToMergeIds() {
        if (recordToMergeIds == null) {
            recordToMergeIds = new ArrayList<String>();
        }
        return this.recordToMergeIds;
    }

}
