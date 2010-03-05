
package com.sforce.soap.partner;

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
 *         &lt;element name="triggerAutoResponseEmail" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="triggerOtherEmail" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="triggerUserEmail" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
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
    "triggerAutoResponseEmail",
    "triggerOtherEmail",
    "triggerUserEmail"
})
@XmlRootElement(name = "EmailHeader")
public class EmailHeader {

    protected boolean triggerAutoResponseEmail;
    protected boolean triggerOtherEmail;
    protected boolean triggerUserEmail;

    /**
     * Gets the value of the triggerAutoResponseEmail property.
     * 
     */
    public boolean isTriggerAutoResponseEmail() {
        return triggerAutoResponseEmail;
    }

    /**
     * Sets the value of the triggerAutoResponseEmail property.
     * 
     */
    public void setTriggerAutoResponseEmail(boolean value) {
        this.triggerAutoResponseEmail = value;
    }

    /**
     * Gets the value of the triggerOtherEmail property.
     * 
     */
    public boolean isTriggerOtherEmail() {
        return triggerOtherEmail;
    }

    /**
     * Sets the value of the triggerOtherEmail property.
     * 
     */
    public void setTriggerOtherEmail(boolean value) {
        this.triggerOtherEmail = value;
    }

    /**
     * Gets the value of the triggerUserEmail property.
     * 
     */
    public boolean isTriggerUserEmail() {
        return triggerUserEmail;
    }

    /**
     * Sets the value of the triggerUserEmail property.
     * 
     */
    public void setTriggerUserEmail(boolean value) {
        this.triggerUserEmail = value;
    }

}
