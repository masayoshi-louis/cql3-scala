<p>
This is a simple helper library for Scala 2.10 based on DataStax Java Driver.<br/>
It is used in UMCAT project in NLP2CT Lab.<br/>
</p>
<p>
Features:<br/>
&nbsp;&nbsp;Define CQL3 tables as Scala classes.<br/>
&nbsp;&nbsp;DDL generation.<br/>
&nbsp;&nbsp;API enhancements.<br/>
</p>
<p>
TODOs:<br/>
&nbsp;&nbsp;Implement full collection type supports.<br/>
</p>
<p>
An Example:<br/>
<br/>
&nbsp;&nbsp;val User = new Table("user") {<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Id = "id" INT PK<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Name = "name" TEXT<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Email = "email" TEXT<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Passhash = "p_hash" BLOB<br/>
&nbsp;&nbsp;&nbsp;&nbsp;val Tags = "tags" SET (TEXT)<br/>
<br/>
&nbsp;&nbsp;&nbsp;&nbsp;WITH(<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;CompactStorage,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Caching.All,<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Compression := "{ 'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64 }")<br/>
&nbsp;&nbsp;}<br/>
&nbsp;&nbsp;<br/>
&nbsp;&nbsp;import User._<br/>
&nbsp;&nbsp;val row = //from query<br/>
&nbsp;&nbsp;val userName = row %: Name<br/>
</p>
<p>
License: LGPL v3.0<br/>
Use at your own risk.
</p>