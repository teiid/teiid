
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="pairs" type="{urn:partner.soap.sforce.com}DataCategoryGroupSobjectTypePair" maxOccurs="100" minOccurs="0"/>
 *         &lt;element name="topCategoriesOnly" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "pairs",
    "topCategoriesOnly"
})
@XmlRootElement(name = "describeDataCategoryGroupStructures")
public class DescribeDataCategoryGroupStructures {

    protected List<DataCategoryGroupSobjectTypePair> pairs;
    protected boolean topCategoriesOnly;

    /**
     * Gets the value of the pairs property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the pairs property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getPairs().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DataCategoryGroupSobjectTypePair }
     * 
     * 
     */
    public List<DataCategoryGroupSobjectTypePair> getPairs() {
        if (pairs == null) {
            pairs = new ArrayList<DataCategoryGroupSobjectTypePair>();
        }
        return this.pairs;
    }

    /**
     * Gets the value of the topCategoriesOnly property.
     * 
     */
    public boolean isTopCategoriesOnly() {
        return topCategoriesOnly;
    }

    /**
     * Sets the value of the topCategoriesOnly property.
     * 
     */
    public void setTopCategoriesOnly(boolean value) {
        this.topCategoriesOnly = value;
    }

}
