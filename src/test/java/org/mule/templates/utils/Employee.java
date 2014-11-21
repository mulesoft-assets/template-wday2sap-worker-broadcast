/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.utils;

public class Employee {

	  private String givenName;
	  private String familyName;
	  private String middleName;
	  
	  private String id;
	  
	  public Employee(String givenNameValue, String familyNameValue, String middleName, String idValue) {
	  	givenName = givenNameValue;
	  	familyName = familyNameValue;	
	  	this.middleName = middleName;
	    id = idValue;
	  }
	  
	  public String getGivenName() {
			return givenName;
		}
	  
	  

		public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

		public void setGivenName(String givenName) {
			this.givenName = givenName;
		}

		public String getFamilyName() {
			return familyName;
		}

		public void setFamilyName(String familyName) {
			this.familyName = familyName;
		}
		

		@Override
		public String toString() {
			return "Employee: " + givenName + " " + familyName + " " + middleName;
					
		}

		public String getMiddleName() {
			return middleName;
		}

		public void setMiddleName(String middleName) {
			this.middleName = middleName;
		}
		
		
}
