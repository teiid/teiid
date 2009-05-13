/*
 * Copyright 2000-2008 MetaMatrix, Inc. All rights reserved.
 */
package com.metamatrix.console.util;

/**
 * Data class to represent the info on UDDI registy that is saved to the properties file.
 * 
 * @since 5.5.3
 */
public class SavedUDDIRegistryInfo {
	private String name;
	private String userName;
	private String inquiryUrl;
	private String publishUrl;

	public SavedUDDIRegistryInfo( String name,
	                              String userName,
	                              String inquiryUrl,
	                              String publishUrl ) {
		super();
		this.name = name;
		this.userName = userName;
		this.inquiryUrl = inquiryUrl;
		this.publishUrl = publishUrl;
	}

	public String getName() {
		return name;
	}

	public void setName( String name ) {
		this.name = name;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName( String userName ) {
		this.userName = userName;
	}

	public String getInquiryUrl() {
		return inquiryUrl;
	}

	public void setInquiryUrl( String inquiryUrl ) {
		this.inquiryUrl = inquiryUrl;
	}

	public String getPublishUrl() {
		return publishUrl;
	}

	public void setPublishUrl( String publishUrl ) {
		this.publishUrl = publishUrl;
	}

	public boolean equals( Object obj ) {
		boolean same;
		if (obj == this) {
			same = true;
		} else if (!(obj instanceof SavedUDDIRegistryInfo)) {
			same = false;
		} else {
			SavedUDDIRegistryInfo that = (SavedUDDIRegistryInfo)obj;
			boolean nameMatches;
			boolean userNameMatches = false;
			boolean inquiryUrlMatches = false;
			boolean publishUrlMatches = false;
			String thatName = that.getName();
			if (name == null) {
				nameMatches = (thatName == null);
			} else {
				nameMatches = name.equals(thatName);
			}
			if (nameMatches) {
				String thatUserName = that.getUserName();
				if (userName == null) {
					userNameMatches = (thatUserName == null);
				} else {
					userNameMatches = userName.equals(thatUserName);
				}
				if (userNameMatches) {
					String thatInquiryUrl = that.getInquiryUrl();
					if (inquiryUrl == null) {
						inquiryUrlMatches = (thatInquiryUrl == null);
					} else {
						inquiryUrlMatches = inquiryUrl.equals(thatInquiryUrl);
					}
					if (inquiryUrlMatches) {
						String thatPublishUrl = that.getPublishUrl();
						if (publishUrl == null) {
							publishUrlMatches = (thatPublishUrl == null);
						} else {
							publishUrlMatches = publishUrl.equals(thatPublishUrl);
						}
					}
				}
			}
			same = (nameMatches && userNameMatches && inquiryUrlMatches && publishUrlMatches);
		}
		return same;
	}
}
