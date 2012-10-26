package org.teiid.resource.adapter.google.metadata;


import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

@XmlEnum
public enum SpreadsheetColumnType {

	 @XmlEnumValue("string")
	 STRING,
	 @XmlEnumValue("double")
	 DOUBLE,
	 @XmlEnumValue("date")
	 DATE, 
	 @XmlEnumValue("time")
	 TIME,
	 @XmlEnumValue("float")
	 FLOAT,
	 @XmlEnumValue("char")
	 CHAR,
	 @XmlEnumValue("big_integer")
	 BIG_INTEGER, 
	 @XmlEnumValue("big_decimal")
	 BIG_DECIMAL,
	 @XmlEnumValue("integer")
	 INTEGER,
	 @XmlEnumValue("boolean")
	 BOOLEAN,
	 @XmlEnumValue("long")
	 LONG,
	 @XmlEnumValue("short")
	 SHORT
}
