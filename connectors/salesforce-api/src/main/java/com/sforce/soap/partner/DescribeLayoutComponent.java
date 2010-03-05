
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DescribeLayoutComponent complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DescribeLayoutComponent">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="displayLines" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="tabOrder" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="type" type="{urn:partner.soap.sforce.com}layoutComponentType"/>
 *         &lt;element name="value" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DescribeLayoutComponent", propOrder = {
    "displayLines",
    "tabOrder",
    "type",
    "value"
})
public class DescribeLayoutComponent {

    protected int displayLines;
    protected int tabOrder;
    @XmlElement(required = true)
    protected LayoutComponentType type;
    @XmlElement(required = true)
    protected String value;

    /**
     * Gets the value of the displayLines property.
     * 
     */
    public int getDisplayLines() {
        return displayLines;
    }

    /**
     * Sets the value of the displayLines property.
     * 
     */
    public void setDisplayLines(int value) {
        this.displayLines = value;
    }

    /**
     * Gets the value of the tabOrder property.
     * 
     */
    public int getTabOrder() {
        return tabOrder;
    }

    /**
     * Sets the value of the tabOrder property.
     * 
     */
    public void setTabOrder(int value) {
        this.tabOrder = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link LayoutComponentType }
     *     
     */
    public LayoutComponentType getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link LayoutComponentType }
     *     
     */
    public void setType(LayoutComponentType value) {
        this.type = value;
    }

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValue(String value) {
        this.value = value;
    }

}
