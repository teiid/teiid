<%@ page import="com.metamatrix.common.util.WSDLServletUtil" %>

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
	<head>
		<title>JBEDSP Web Service Utilities</title>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
		<style type="text/css">
		<!--
			.MMSOAPTitle {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 18pt;
				font-style: normal;
				font-weight: bold				
			}
			
			.deprecated {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 12pt;
				font-style: italic;
				color: red;
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
  		<div align="center">
  		<p><table width="900" border="0" cellspacing="0" cellpadding="0">
  		  <tr bgcolor="#edf0f6">
	  		  <td colspan="2" align="center">
	  		  	<span class="MMSOAPTitle"><a href="http://www.jboss.com/products/platforms/dataservices/" target="_blank">JBoss Enterprise Data Services Platform (JBEDSP)</a></span>
	  		  </td>
	  	  </tr>
	  	  <tr bgcolor="#edf0f6">
	  		  <td colspan="2" align="center">
	  		  	<span class="MMSOAPTitle">&nbsp;Web Service Utilities</span>
	  		  </td>
  		  </tr>
  		   <tr>
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>JBEDSP Web Service WSDL URL Generator</u></b><td>		      
   		  </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>VDBs that contain Web Service models expose WSDL when deployed to a JBEDSP Server that describes the web service operations in those models. This form can be used to generate a parameterized WSDL URL for such a VDB.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td><a href="wsdlurlframe.htm" target="_self" title="Web Service WSDL URL Generator">Web Service WSDL URL Generator</a><td>	
		      <td>&nbsp;<td>	
	      </tr>
   	      <tr>
			  <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>Discover JBEDSP Web Services</u></b><td>		      
   		  </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>To auto discover web service VDB WSDL URLs using predefined JBEDSP server values, the JBEDSP Server or cluster of JBEDSP servers must be defined in the web.xml file of the JBEDSP SOAP web application. This WSDL URL is more concise than the parameterized version, but is only valid with JBEDSP SOAP web applications were the JBEDSP server values have been defined.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td><a href="<%= request.getContextPath() %>/servlet/DiscoverWSDLServlet" target="wsdlUrls" title="Discover MetaMatrix Web Services">Discover JBEDSP Web Services</a><td>	
		      <td>&nbsp;<td>	
	      </tr>
   	      <tr>
			  <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>	      
	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>SQL Query Web Service WSDL</u></b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>Click on the link below to view the SQL Query Web Service WSDL for this server.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left"><a target="_blank" href="<%= WSDLServletUtil.getSqlQueryWebServiceUrl(request.getServerName(), request.getContextPath(), false) %>" title="SQL Query Web Service WSDL (Non-secure)">SQL Query Web Service WSDL (Non-secure)</a><td>	
   		      <td>&nbsp;<td>
	      </tr>	      
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left"><a target="_blank" href="<%= WSDLServletUtil.getSqlQueryWebServiceUrl(request.getServerName(), request.getContextPath(), true) %>" title="SQL Query Web Service WSDL (Secure SSL)">SQL Query Web Service WSDL (Secure SSL)</a><td>	
   		      <td>&nbsp;<td>
	      </tr>	      
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><hr><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
<!--	      <tr>
		      <td colspan="2"><b>Click on the link below to view the Web Service Utilites page for the <span class="deprecated">DEPRECATED</span> SQL Query Web Service</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left" colspan="2"><a href="deprecated.jsp" alt="Deprecated SQL Query Web Service Utilities Page"><span class="deprecated">(DEPRECATED)</span> SQL Query Web Service Web Service Utilies Page</a><td>	
	      </tr>	
	      -->      
		  </table>  		
</div>
  	</body>
</html>
