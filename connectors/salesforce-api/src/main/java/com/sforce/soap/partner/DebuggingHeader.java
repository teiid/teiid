
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
 *         &lt;element name="debugLevel" type="{urn:partner.soap.sforce.com}DebugLevel"/>
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
    "debugLevel"
})
@XmlRootElement(name = "DebuggingHeader")
public class DebuggingHeader {

    @XmlElement(required = true)
    protected DebugLevel debugLevel;

    /**
     * Gets the value of the debugLevel property.
     * 
     * @return
     *     possible object is
     *     {@link DebugLevel }
     *     
     */
    public DebugLevel getDebugLevel() {
        return debugLevel;
    }

    /**
     * Sets the value of the debugLevel property.
     * 
     * @param value
     *     allowed object is
     *     {@link DebugLevel }
     *     
     */
    public void setDebugLevel(DebugLevel value) {
        this.debugLevel = value;
    }

}
