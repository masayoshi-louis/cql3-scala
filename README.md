This is a simple helper library for Scala based on DataStax Java Driver.
It is used in UMCAT project in NLP2CT Lab.

Feature:
	Define CQL3 tables as Scala classes.
	DDL generation.
	API enhancements.
	
	
An Example:

  val User = new Table("user") {
    val Id = "id" INT PK
    val Name = "name" TEXT
    val Email = "email" TEXT
    val Passhash = "p_hash" BLOB
  }
  
  import User._
  val row = /*from query*/
  val userName = row %: Name
  
  
License: LGPL v3.0