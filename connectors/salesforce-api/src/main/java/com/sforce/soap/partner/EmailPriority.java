
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for EmailPriority.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="EmailPriority">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="Highest"/>
 *     &lt;enumeration value="High"/>
 *     &lt;enumeration value="Normal"/>
 *     &lt;enumeration value="Low"/>
 *     &lt;enumeration value="Lowest"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "EmailPriority")
@XmlEnum
public enum EmailPriority {

    @XmlEnumValue("Highest")
    HIGHEST("Highest"),
    @XmlEnumValue("High")
    HIGH("High"),
    @XmlEnumValue("Normal")
    NORMAL("Normal"),
    @XmlEnumValue("Low")
    LOW("Low"),
    @XmlEnumValue("Lowest")
    LOWEST("Lowest");
    private final String value;

    EmailPriority(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static EmailPriority fromValue(String v) {
        for (EmailPriority c: EmailPriority.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
