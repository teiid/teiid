package org.teiid.eclipselink.platform.model;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "Marketdata")
public class Marketdata implements Serializable {
	

	private static final long serialVersionUID = 1783712327461134953L;

	@Id
	private String symbol;

	@Column
	private BigDecimal price;

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

}
