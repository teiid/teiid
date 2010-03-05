
package com.sforce.soap.partner;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeLayoutButtonSection complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeLayoutButtonSection">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="detailButtons" type="{urn:partner.soap.sforce.com}DescribeLayoutButton" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeLayoutButtonSection", propOrder = {
    "detailButtons"
})
public class DescribeLayoutButtonSection {

    @XmlElement(required = true)
    protected List<DescribeLayoutButton> detailButtons;

    /**
     * Gets the value of the detailButtons property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the detailButtons property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDetailButtons().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DescribeLayoutButton }
     * 
     * 
     */
    public List<DescribeLayoutButton> getDetailButtons() {
        if (detailButtons == null) {
            detailButtons = new ArrayList<DescribeLayoutButton>();
        }
        return this.detailButtons;
    }

}
