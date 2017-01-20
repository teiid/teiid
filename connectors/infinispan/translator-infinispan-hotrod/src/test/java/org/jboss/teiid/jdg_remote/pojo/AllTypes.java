package org.jboss.teiid.jdg_remote.pojo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public class AllTypes implements Serializable{
	
	private static final long serialVersionUID = -3821001741608384452L;
	
	private String stringNum;
	private int charValue;
	private Double doubleNum;
	private BigInteger bigIntegerValue;
	private Short shortValue;
	private Float floatNum;
	private byte[]  byteArrayValue;
	
	private Integer intNum;
	private BigDecimal bigDecimalValue;
	
	private Long longNum;
	private Boolean booleanValue;
	private Timestamp timeStampValue;
	
	private Integer intKey;
	private String stringKey;
	private Time timeValue;
	private Date dateValue;

	@ProtoField(number = 2)
	public String getStringNum() {
		return stringNum;
	}

	public void setStringNum(String stringNum) {
		this.stringNum = stringNum;
	}

	public int getCharValue() {
		return charValue;
	}

	public void setCharValue(int charValue) {
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

	@ProtoField(number = 7)
	public byte[] getByteArrayValue() {
		return byteArrayValue;
	}

	public void setByteArrayValue(byte[] objectValue) {
		this.byteArrayValue = objectValue;
	}
	

	@ProtoField(number = 8)
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

	@ProtoField(number = 9)
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

	@ProtoField(number = 1, required = true)
	public Integer getIntKey() {
		return intKey;
	}

	public void setIntKey(Integer intKey) {
		this.intKey = intKey;
	}

	@ProtoField(number = 13, required = true)
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
		return "SmallA [stringNum=" + stringNum + ", charValue=" + charValue + ", doubleNum=" + doubleNum + ", bigIntegerNum=" + bigIntegerValue + ", shortValue=" + shortValue + ", floatNum=" + floatNum + ", "
				// "//ObjectValue=" + byteArrayValue + ""
						+ ", intNum=" + intNum + ", bigDecimalValue=" + bigDecimalValue + ", longNum=" + longNum + ", booelanValue=" + booleanValue + ", timeStampValue=" + timeStampValue +  ", intKey=" + intKey + ", stringKey=" + stringKey + ", timeValue=" + timeValue + ", dateValue=" + dateValue + "]";
	}
}
