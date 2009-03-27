<%@ page import="com.metamatrix.common.util.WSDLServletUtil"%>
<%@ page import="com.metamatrix.soap.object.WSDLUrl"%>
<%@ page import="java.util.List"%>
<%@ page import="java.util.Iterator"%>
<!--
/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
-->
<html>
<%
			 
			List wsdlUrls = (List) session
					.getAttribute(WSDLServletUtil.DISCOVERED_WSDL);

			%>
<head>


<title>JBoss Enterprise Data Services Platform (JBEDSP) WSDL URLs for Active Deployed Web Service Virtual
Databases</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<style type="text/css">
		<!--
			.MMSOAPTitle {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 18pt;
				font-style: normal;
				font-weight: bold				
			}
			
			a {
				visited : #666699; link: #666699; active: #666699;
				hover: #1E90FF; text-decoration: none; 
			}
}
			
		-->
		</style>
</head>

<body bgcolor="#FFFFFF">
<table width="900" border="0" cellspacing="1" cellpadding="1">
	<tr bgcolor="#edf0f6">
		<td width="300"><span class="MMSOAPTitle"><a href="http://www.jboss.com/products/platforms/dataservices/">JBEDSP</a></td>
		<td width="600">JBEDSP Active Web Service VDB
		URLs</span></td>
	</tr>
	<tr>
		<td colspan="2">
		<hr />
		</td>
	</tr>
	<tr width="900">
	<%Iterator wsdlUrlIter = wsdlUrls.listIterator();
			if (wsdlUrls.size() == 0) {

			%>
		<td colspan="2">No active MetaMatrix Web Service VDBs were discovered.</td>
	<%} else {
				while (wsdlUrlIter.hasNext()) {
					WSDLUrl wsdlUrl = (WSDLUrl) wsdlUrlIter.next();
					%>
		<td colspan="2"><a href="<%= wsdlUrl.toString() %>"><%=wsdlUrl.toString()%></a></td>
		</tr>
	<%}
			}

		%>
</table>
</body>
</html>
