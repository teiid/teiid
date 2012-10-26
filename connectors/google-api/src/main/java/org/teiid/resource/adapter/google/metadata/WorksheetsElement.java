package org.teiid.resource.adapter.google.metadata;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;

/**
 * JAXB mapping for worksheets element.
 * @author fnguyen
 *
 */
@XmlRootElement(name="worksheets")
@XmlAccessorType(XmlAccessType.NONE)
public class WorksheetsElement {
	@XmlElement(name="worksheet")
	private List<WorksheetElement> worksheets = new ArrayList<WorksheetElement>();

	public static WorksheetsElement parse(String xml) {
		if (xml == null || xml.trim().equals(""))
			return new WorksheetsElement();
		
		JAXBContext context = null;
		try {
			context = JAXBContext.newInstance(WorksheetsElement.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			return ((WorksheetsElement) unmarshaller
					.unmarshal(new StringReader(xml)));

		} catch (JAXBException e) {
			throw new SpreadsheetOperationException(e);
		}
	}
	
	public List<WorksheetElement> getWorksheets() {
		return worksheets;
	}

	public void setWorksheets(List<WorksheetElement> worksheets) {
		this.worksheets = worksheets;
	}

	@XmlRootElement(name = "worksheet")
	@XmlAccessorType(XmlAccessType.NONE)
	public static class WorksheetElement {
		@XmlAttribute(required = true)
		private String name;
		@XmlElement(name = "column")
		private List<ColumnElement> columns = new ArrayList<ColumnElement>();

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<ColumnElement> getColumns() {
			return columns;
		}

		public void setColumns(List<ColumnElement> columns) {
			this.columns = columns;
		}		

	}

	@XmlRootElement(name = "column")
	@XmlAccessorType(XmlAccessType.NONE)
	public static class ColumnElement {
		@XmlAttribute(required = true)
		private String alphaName;
		@XmlAttribute(required = true)
		private SpreadsheetColumnType dataType;
		public String getAlphaName() {
			return alphaName;
		}
		public void setAlphaName(String alphaName) {
			this.alphaName = alphaName;
		}
		public SpreadsheetColumnType getDataType() {
			return dataType;
		}
		public void setDataType(SpreadsheetColumnType dataType) {
			this.dataType = dataType;
		}
	}
}

