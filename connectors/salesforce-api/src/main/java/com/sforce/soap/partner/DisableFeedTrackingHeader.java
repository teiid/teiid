
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
 *         &lt;element name="disableFeedTracking" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
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
    "disableFeedTracking"
})
@XmlRootElement(name = "DisableFeedTrackingHeader")
public class DisableFeedTrackingHeader {

    protected boolean disableFeedTracking;

    /**
     * Gets the value of the disableFeedTracking property.
     * 
     */
    public boolean isDisableFeedTracking() {
        return disableFeedTracking;
    }

    /**
     * Sets the value of the disableFeedTracking property.
     * 
     */
    public void setDisableFeedTracking(boolean value) {
        this.disableFeedTracking = value;
    }

}
