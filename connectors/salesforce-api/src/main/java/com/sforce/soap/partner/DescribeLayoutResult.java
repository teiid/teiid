
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeLayoutResult complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeLayoutResult">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="layouts" type="{urn:partner.soap.sforce.com}DescribeLayout" maxOccurs="unbounded"/>
 *         &lt;element name="recordTypeMappings" type="{urn:partner.soap.sforce.com}RecordTypeMapping" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="recordTypeSelectorRequired" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeLayoutResult", propOrder = {
    "layouts",
    "recordTypeMappings",
    "recordTypeSelectorRequired"
})
public class DescribeLayoutResult {

    @XmlElement(required = true)
    protected List<DescribeLayout2> layouts;
    protected List<RecordTypeMapping> recordTypeMappings;
    protected boolean recordTypeSelectorRequired;

    /**
     * Gets the value of the layouts property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the layouts property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getLayouts().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeLayout2 }
     * 
     * 
     */
    public List<DescribeLayout2> getLayouts() {
        if (layouts == null) {
            layouts = new ArrayList<DescribeLayout2>();
        }
        return this.layouts;
    }

    /**
     * Gets the value of the recordTypeMappings property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the recordTypeMappings property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getRecordTypeMappings().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link RecordTypeMapping }
     * 
     * 
     */
    public List<RecordTypeMapping> getRecordTypeMappings() {
        if (recordTypeMappings == null) {
            recordTypeMappings = new ArrayList<RecordTypeMapping>();
        }
        return this.recordTypeMappings;
    }

    /**
     * Gets the value of the recordTypeSelectorRequired property.
     * 
     */
    public boolean isRecordTypeSelectorRequired() {
        return recordTypeSelectorRequired;
    }

    /**
     * Sets the value of the recordTypeSelectorRequired property.
     * 
     */
    public void setRecordTypeSelectorRequired(boolean value) {
        this.recordTypeSelectorRequired = value;
    }

}
