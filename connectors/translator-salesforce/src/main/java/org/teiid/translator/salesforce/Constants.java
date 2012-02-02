package org.teiid.translator.salesforce;

public interface Constants {

	public static final String PICKLIST_TYPE = "picklist"; //$NON-NLS-1$

	public static final String MULTIPICKLIST_TYPE = "multipicklist"; //$NON-NLS-1$

	public static final String COMBOBOX_TYPE = "combobox"; //$NON-NLS-1$

	public static final String ANYTYPE_TYPE = "anyType"; //$NON-NLS-1$

	public static final String REFERENCE_TYPE = "reference"; //$NON-NLS-1$

	public static final String STRING_TYPE = "string"; //$NON-NLS-1$

	public static final String BASE64_TYPE = "base64"; //$NON-NLS-1$

	public static final String BOOLEAN_TYPE = "boolean"; //$NON-NLS-1$

	public static final String CURRENCY_TYPE = "currency"; //$NON-NLS-1$

	public static final String TEXTAREA_TYPE = "textarea"; //$NON-NLS-1$

	public static final String INT_TYPE = "int"; //$NON-NLS-1$

	public static final String DOUBLE_TYPE = "double"; //$NON-NLS-1$

	public static final String PERCENT_TYPE = "percent"; //$NON-NLS-1$

	public static final String PHONE_TYPE = "phone"; //$NON-NLS-1$

	public static final String ID_TYPE = "id"; //$NON-NLS-1$

	public static final String DATE_TYPE = "date"; //$NON-NLS-1$

	public static final String DATETIME_TYPE = "datetime"; //$NON-NLS-1$

	public static final String URL_TYPE = "url"; //$NON-NLS-1$

	public static final String EMAIL_TYPE = "email"; //$NON-NLS-1$

	public static final String EXTENSION_URI = "{http://www.teiid.org/translator/salesforce/2012}"; //$NON-NLS-1$

	public static final String RESTRICTED_PICKLIST_TYPE = "restrictedpicklist"; //$NON-NLS-1$
	
	public static final String RESTRICTED_MULTISELECT_PICKLIST_TYPE = "restrictedmultiselectpicklist"; //$NON-NLS-1$

	public static final String SUPPORTS_QUERY = EXTENSION_URI +"Supports Query";//$NON-NLS-1$
	
	public static final String SUPPORTS_RETRIEVE = EXTENSION_URI +"Supports Retrieve";//$NON-NLS-1$

}
