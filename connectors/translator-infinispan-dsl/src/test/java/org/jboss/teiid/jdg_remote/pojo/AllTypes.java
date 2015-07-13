package org.jboss.teiid.jdg_remote.pojo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.io.Serializable;
import java.lang.Character;
import java.math.BigDecimal;
import java.math.BigInteger;

public class AllTypes implements Serializable{
	
	private static final long serialVersionUID = -3821001741608384452L;
	
	private String stringNum;
	private Character charValue;
	private Double doubleNum;
	private BigInteger bigIntegerValue;
	private Short shortValue;
	private Float floatNum;
	private Object  objectValue;
	private Integer intNum;
	private BigDecimal bigDecimalValue;
	private Long longNum;
	private Boolean booleanValue;
	private Timestamp timeStampValue;
	private Integer intKey;
	private String stringKey;
	private Time timeValue;
	private Date dateValue;

	public String getStringNum() {
		return stringNum;
	}

	public void setStringNum(String stringNum) {
		this.stringNum = stringNum;
	}

	public Character getCharValue() {
		return charValue;
	}

	public void setCharValue(Character charValue) {
		this.charValue = charValue;
	}

	public Double getDoubleNum() {
		return doubleNum;
	}

	public void setDoubleNum(Double doubleNum) {
		this.doubleNum = doubleNum;
	}

	public java.math.BigInteger getBigIntegerValue() {
		return bigIntegerValue;
	}

	public void setBigIntegerValue(BigInteger bigIntegerValue) {
		this.bigIntegerValue = bigIntegerValue;
	}

	public Short getShortValue() {
		return shortValue;
	}

	public void setShortValue(Short shortValue) {
		this.shortValue = shortValue;
	}

	public Float getFloatNum() {
		return floatNum;
	}

	public void setFloatNum(Float floatNum) {
		this.floatNum = floatNum;
	}

	public Object getObjectValue() {
		return objectValue;
	}

	public void setObjectValue(Object objectValue) {
		this.objectValue = objectValue;
	}

	public Integer getIntNum() {
		return intNum;
	}

	public void setIntNum(Integer intNum) {
		this.intNum = intNum;
	}

	public BigDecimal getBigDecimalValue() {
		return bigDecimalValue;
	}

	public void setBigDecimalValue(BigDecimal bigDecimalValue) {
		this.bigDecimalValue = bigDecimalValue;
	}

	public Long getLongNum() {
		return longNum;
	}

	public void setLongNum(Long longNum) {
		this.longNum = longNum;
	}

	public Boolean getBooleanValue() {
		return booleanValue;
	}

	public void setBooleanValue(Boolean booleanValue) {
		this.booleanValue = booleanValue;
	}

	public Timestamp getTimeStampValue() {
		return timeStampValue;
	}

	public void setTimeStampValue(Timestamp timeStampValue) {
		this.timeStampValue = timeStampValue;
	}

	public Integer getIntKey() {
		return intKey;
	}

	public void setIntKey(Integer intKey) {
		this.intKey = intKey;
	}

	public String getStringKey() {
		return stringKey;
	}

	public void setStringKey(String stringKey) {
		this.stringKey = stringKey;
	}

	public Time getTimeValue() {
		return timeValue;
	}

	public void setTimeValue(Time timeValue) {
		this.timeValue = timeValue;
	}

	public Date getDateValue() {
		return dateValue;
	}

	public void setDateValue(Date dateValue) {
		this.dateValue = dateValue;
	}
	

	@Override
	public String toString() {
		return "SmallA [stringNum=" + stringNum + ", charValue=" + charValue + ", doubleNum=" + doubleNum + ", bigIntegerNum=" + bigIntegerValue + ", shortValue=" + shortValue + ", floatNum=" + floatNum + ", ObjectValue=" + objectValue + ", intNum=" + intNum + ", bigDecimalValue=" + bigDecimalValue + ", longNum=" + longNum + ", booelanValue=" + booleanValue + ", timeStampValue=" + timeStampValue +  ", intKey=" + intKey + ", stringKey=" + stringKey + ", timeValue=" + timeValue + ", dateValue=" + dateValue + "]";
	}
}
