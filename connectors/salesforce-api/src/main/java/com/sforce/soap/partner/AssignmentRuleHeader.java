
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
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
 *         &lt;element name="assignmentRuleId" type="{urn:partner.soap.sforce.com}ID"/>
 *         &lt;element name="useDefaultRule" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
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
    "assignmentRuleId",
    "useDefaultRule"
})
@XmlRootElement(name = "AssignmentRuleHeader")
public class AssignmentRuleHeader {

    @XmlElement(required = true, nillable = true)
    protected String assignmentRuleId;
    @XmlElement(required = true, type = Boolean.class, nillable = true)
    protected Boolean useDefaultRule;

    /**
     * Gets the value of the assignmentRuleId property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAssignmentRuleId() {
        return assignmentRuleId;
    }

    /**
     * Sets the value of the assignmentRuleId property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAssignmentRuleId(String value) {
        this.assignmentRuleId = value;
    }

    /**
     * Gets the value of the useDefaultRule property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isUseDefaultRule() {
        return useDefaultRule;
    }

    /**
     * Sets the value of the useDefaultRule property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setUseDefaultRule(Boolean value) {
        this.useDefaultRule = value;
    }

}
