<%@ page import="com.metamatrix.soap.util.ServletClientConstants" %>
<%@ page import="com.metamatrix.soap.util.SOAPConstants" %>
<%@ page import="javax.naming.Context" %>
<%@ page import="javax.naming.InitialContext" %>
<%@ page import="javax.naming.NamingException" %>
<html>
<%/*
             * Copyright © 2000-2005 MetaMatrix, Inc.
             * All rights reserved.
             */
            String targetServer = request.getServerName();
            int targetPort = request.getServerPort();
            %>
	<head>


		<title>MetaMatrix WSDL URL Generator</title>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
		<style type="text/css">
		<!--
			.MMSOAPTitle {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 18pt;
				font-style: normal;
				font-weight: bold
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
  		   <td width="300">
  		   		<a href="http:metamatrix.com"><img src="images/mmlogo.gif" border="no"></img></a>
  		   </td>
  		   <td width="600">
  		   		<span class="MMSOAPTitle">MetaMatrix WSDL URL Generator</span>
  		   </td>
  		   </tr>
  		   </table>
  		</p>
  		<p>&nbsp;</p>
  		<form name="form" method="post" target="wsdlUrl" ACTION="../metamatrix-soap/servlet/WSDLURLGenerator">
    		<p><b>MetaMatrix Server Information</b> (use commas to delimit multiple host:port combinations)</p>
    		<table width="66%" border="0" cellspacing="0" cellpadding="0" bgcolor="#edf0f6">
      			<tr>
      				<td width="48%"><b>MetaMatrix Server host:port</b><span class='required'>&nbsp;*</span> </td>
      				<td width="52%">
        				<input type="text" name="MMServerHostAndPort" value="<mmServerHost>:<mmServerPort>" size="80">
      				</td>
    			</tr>
			</table>
  			<p><b>MetaMatrix VDB Information</b> </p>
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
      				<td width="48%"><b>Use Secure MetaMatrix Protocol (mms) </b><span class='required'>&nbsp;*</span></td>
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
