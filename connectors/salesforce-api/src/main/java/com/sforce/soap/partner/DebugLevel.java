
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DebugLevel.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="DebugLevel">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="None"/>
 *     &lt;enumeration value="DebugOnly"/>
 *     &lt;enumeration value="Db"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "DebugLevel")
@XmlEnum
public enum DebugLevel {

    @XmlEnumValue("None")
    NONE("None"),
    @XmlEnumValue("DebugOnly")
    DEBUG_ONLY("DebugOnly"),
    @XmlEnumValue("Db")
    DB("Db");
    private final String value;

    DebugLevel(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static DebugLevel fromValue(String v) {
        for (DebugLevel c: DebugLevel.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
