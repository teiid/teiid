
package com.sforce.soap.partner;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for fieldType.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="fieldType">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="string"/>
 *     &lt;enumeration value="picklist"/>
 *     &lt;enumeration value="multipicklist"/>
 *     &lt;enumeration value="combobox"/>
 *     &lt;enumeration value="reference"/>
 *     &lt;enumeration value="base64"/>
 *     &lt;enumeration value="boolean"/>
 *     &lt;enumeration value="currency"/>
 *     &lt;enumeration value="textarea"/>
 *     &lt;enumeration value="int"/>
 *     &lt;enumeration value="double"/>
 *     &lt;enumeration value="percent"/>
 *     &lt;enumeration value="phone"/>
 *     &lt;enumeration value="id"/>
 *     &lt;enumeration value="date"/>
 *     &lt;enumeration value="datetime"/>
 *     &lt;enumeration value="time"/>
 *     &lt;enumeration value="url"/>
 *     &lt;enumeration value="email"/>
 *     &lt;enumeration value="encryptedstring"/>
 *     &lt;enumeration value="datacategorygroupreference"/>
 *     &lt;enumeration value="anyType"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "fieldType")
@XmlEnum
public enum FieldType {

    @XmlEnumValue("string")
    STRING("string"),
    @XmlEnumValue("picklist")
    PICKLIST("picklist"),
    @XmlEnumValue("multipicklist")
    MULTIPICKLIST("multipicklist"),
    @XmlEnumValue("combobox")
    COMBOBOX("combobox"),
    @XmlEnumValue("reference")
    REFERENCE("reference"),
    @XmlEnumValue("base64")
    BASE_64("base64"),
    @XmlEnumValue("boolean")
    BOOLEAN("boolean"),
    @XmlEnumValue("currency")
    CURRENCY("currency"),
    @XmlEnumValue("textarea")
    TEXTAREA("textarea"),
    @XmlEnumValue("int")
    INT("int"),
    @XmlEnumValue("double")
    DOUBLE("double"),
    @XmlEnumValue("percent")
    PERCENT("percent"),
    @XmlEnumValue("phone")
    PHONE("phone"),
    @XmlEnumValue("id")
    ID("id"),
    @XmlEnumValue("date")
    DATE("date"),
    @XmlEnumValue("datetime")
    DATETIME("datetime"),
    @XmlEnumValue("time")
    TIME("time"),
    @XmlEnumValue("url")
    URL("url"),
    @XmlEnumValue("email")
    EMAIL("email"),
    @XmlEnumValue("encryptedstring")
    ENCRYPTEDSTRING("encryptedstring"),
    @XmlEnumValue("datacategorygroupreference")
    DATACATEGORYGROUPREFERENCE("datacategorygroupreference"),
    @XmlEnumValue("anyType")
    ANY_TYPE("anyType");
    private final String value;

    FieldType(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static FieldType fromValue(String v) {
        for (FieldType c: FieldType.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
