<%@ page import="com.metamatrix.soap.util.ServletClientConstants" %>
<%@ page import="com.metamatrix.soap.util.SOAPConstants" %>
<%@ page import="javax.naming.Context" %>
<%@ page import="javax.naming.InitialContext" %>
<%@ page import="javax.naming.NamingException" %>
<html>
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
<%
            String targetServer = request.getServerName();
            int targetPort = request.getServerPort();
            %>
	<head>


		<title>JBoss Enterprise Data Services Platform (JBEDSP) WSDL URL Generator</title>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
		<style type="text/css">
		<!--
			.MMSOAPTitle {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 18pt;
				font-style: normal;
				font-weight: bold;
				}
				
				a {
				visited : #666699; link: #666699; active: #666699;
				hover: #1E90FF; text-decoration: none; 
				}			
		-->
		.required { font-family: Arial, Helvetica, sans-serif; font-size: 10pt; color: red; font-weight: bold; }
		</style>
	</head>

	<body bgcolor="#FFFFFF">
  		<div align="center">
  		<p>
  		   <table width="900" border="0" cellspacing="0" cellpadding="0">
	  		   <tr bgcolor="#edf0f6">
		  		   <td align="center"><span class="MMSOAPTitle">
		  		   		<a href="http://www.jboss.com/products/platforms/dataservices/" target="_blank">JBoss Enterprise Data Services Platform (JBEDSP)</a></span>
		  		   </td>
	  		   </tr>
	  		   <tr bgcolor="#edf0f6">
		  		   <td align="center"><span class="MMSOAPTitle"> WSDL URL Generator</span>
		  		   </td>
	  		   </tr>
  		   </table>
  		</p>
  		<p>&nbsp;</p>
  		<form name="form" method="post" target="wsdlUrl" ACTION="<%= request.getContextPath() %>/servlet/WSDLURLGenerator">
    		<p><b>JBEDSP Server Information</b> (use commas to delimit multiple host:port combinations)</p>
    		<table width="66%" border="0" cellspacing="0" cellpadding="0" bgcolor="#edf0f6">
      			<tr>
      				<td width="48%"><b>JBEDSP Server host:port</b><span class='required'>&nbsp;*</span> </td>
      				<td width="52%">
        				<input type="text" name="MMServerHostAndPort" value="<mmServerHost>:<mmServerPort>" size="80">
      				</td>
    			</tr>
			</table>
  			<p><b>JBEDSP VDB Information</b> </p>
  			<table width="25%" border="0" cellspacing="0" cellpadding="0" bgcolor="#edf0f6">
    			<tr>
      				<td width="54%">
        				<div align="center"><b>VDB Name</b><span class='required'>&nbsp;*</span> </div>
      				</td>
      				<td width="46%">
        				<div align="center" title="Optional - the latest active version will be used if not entered."><b>VDB Version <br/></b></div>
      				</td>
    			</tr>
    			<tr>
      				<td width="54%">
        				<input type="text" name="VDBName" value="<vdbName>">
      				</td>
      				<td width="46%">
        				<input type="text" name="VDBVersion" value="" title="Optional - the latest active version will be used if not entered.">
      				</td>
    			</tr>
  			</table>
  			<p><b>WSDL URL Options</b></p>
    		<table width="66%" border="0" cellspacing="0" cellpadding="0" bgcolor="#edf0f6">
      			<tr>
      				<td width="48%"><b>Target Webservice Host</b><span class='required'>&nbsp;*</span> </td>
      				<td width="52%">
        				<input type="text" name="TargetHost" value="<%=targetServer%>" size="80">
      				</td>
    			</tr>
    			<tr>
			      	<td width="48%"><b>Target Webservice Port</b> </td>
			      	<td width="52%">
			        	<input type="text" name="TargetPort" value="<%=targetPort%>" size="10">
			      	</td>
			    </tr>
	    		<tr>
      				<td width="48%"><b>Use Secure JBEDSP Protocol (mms) </b><span class='required'>&nbsp;*</span></td>
      				<td width="52%">
        				<input type="checkbox" name="Secure" value="Secure">
      				</td>
    			</tr>
      			<tr>
      				<TD ALIGN="center" colspan='3'>&nbsp;</TD>		
				</TR>	
    			</tr>
    			<TR ALIGN="center" VALIGN="middle">	
    				<TD ALIGN="center" colspan='3'><span class='required'>* = Required Field</span></TD>		
				</TR>	
			</div>
			</table>
  			<p>&nbsp;</p>
		    <p>
		      	<input type="submit" name="submit" value="Get WSDL URL">
		    </p>
   		    <p>
		      	<a href="index.jsp" target="_top">Back to Web Service Utilities page</a>
		    </p>		    
		</form>
  		</div>
	</body>
</html>
