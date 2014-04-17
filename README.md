This is a simple helper library for Scala based on DataStax Java Driver.<br/>
It is used in UMCAT project in NLP2CT Lab.<br/>
<br/>
Features:<br/>
&nbsp;&nbsp;Define CQL3 tables as Scala classes.<br/>
&nbsp;&nbsp;DDL generation.<br/>
&nbsp;&nbsp;API enhancements.<br/>
<br/>
TODOs:<br/>
&nbsp;&nbsp;Implement full collection type supports.<br/>
&nbsp;&nbsp;Add option support to table definition and DDL generation.<br/>
<br/>
An Example:<br/>
<br/>
&nbsp;&nbsp;val User = new Table("user") {<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Id = "id" INT PK<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Name = "name" TEXT<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Email = "email" TEXT<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Passhash = "p_hash" BLOB<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Tags = "tags" SET (TEXT)<br/>
&nbsp;&nbsp;}<br/>
&nbsp;&nbsp;<br/>
&nbsp;&nbsp;import User._<br/>
&nbsp;&nbsp;val row = //from query<br/>
&nbsp;&nbsp;val userName = row %: Name<br/>
&nbsp;&nbsp;<br/>
&nbsp;&nbsp;<br/>
License: LGPL v3.0<br/>
Use at your own risk.