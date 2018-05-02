
# Anypoint Template: Workday to SAP Worker Broadcast

+ [License Agreement](#licenseagreement)
+ [Use Case](#usecase)
+ [Considerations](#considerations)
	* [SAP Considerations](#sapconsiderations)
	* [Workday Considerations](#workdayconsiderations)
+ [Run it!](#runit)
	* [Running on premise](#runonopremise)
	* [Running on Studio](#runonstudio)
	* [Running on Mule ESB stand alone](#runonmuleesbstandalone)
	* [Running on CloudHub](#runoncloudhub)
	* [Deploying your Anypoint Template on CloudHub](#deployingyouranypointtemplateoncloudhub)
	* [Properties to be configured (With examples)](#propertiestobeconfigured)
+ [API Calls](#apicalls)
+ [Customize It!](#customizeit)
	* [config.xml](#configxml)
	* [businessLogic.xml](#businesslogicxml)
	* [endpoints.xml](#endpointsxml)
	* [errorHandling.xml](#errorhandlingxml)


# License Agreement <a name="licenseagreement"/>
Note that using this template is subject to the conditions of this [License Agreement](AnypointTemplateLicense.pdf).
Please review the terms of the license before downloading and using this template. In short, you are allowed to use the template for free with Mule ESB Enterprise Edition, CloudHub, or as a trial in Anypoint Studio.

# Use Case <a name="usecase"/>
As a Workday admin I want to synchronize Workers from Workday to SAP.

As implemented, this Anypoint Template leverages the [Batch Module](http://www.mulesoft.org/documentation/display/current/Batch+Processing).
The batch job is divided in Process and On Complete stages.

1. The integration is triggered by scheduler to Workday instance. New or modified workers are passed to the batch as input. Workers without at least one e-mail address assigned are filtered.
2. In the batch the employee is fetched from SAP by the e-mail and mapped to SAP input data structure.
3. Afterwards every employee is sent to destination instance - to SAP where it is asynchronously updated or created.

# Considerations <a name="considerations"/>

There are certain pre-requisites that must be considered to run this Anypoint Template. All of them deal with the preparations in both source and destination systems, that must be made in order for all to run smoothly. 
**Failing to do so could lead to unexpected behavior of the template.**
There are a couple of things you should take into account before running this Anypoint Template:
**Workday email uniqueness**: The email can be repeated for two or more workers (or missing). Therefore Workday workers with duplicate emails will be removed from processing in the Process stage.

## Disclaimer
This Anypoint template uses a few private Maven dependencies from Mulesoft in order to work. If you intend to run this template with Maven support, you need to add three extra dependencies for SAP to the pom.xml.


## SAP Considerations <a name="sapconsiderations"/>

There may be a few things that you need to know regarding SAP, in order for this template to work.


### As destination of data

This template uses custom BAPI functions. To create them please use following steps:

1. Create function module `ZHCMFM_NUMBER_GET_NEXT` in transaction `SE37` as per source file `ZHCMFM_NUMBER_GET_NEXT.abap`

Referenced files are in [src/main/resources] directory.



## Workday Considerations <a name="workdayconsiderations"/>

### As source of data

There are no particular considerations for this Anypoint Template regarding Workday as data origin.







# Run it! <a name="runit"/>
Simple steps to get Workday to SAP Worker Broadcast running.


## Running on premise <a name="runonopremise"/>
In this section we detail the way you should run your Anypoint Template on your computer.


### Where to Download Mule Studio and Mule ESB
First thing to know if you are a newcomer to Mule is where to get the tools.

+ You can download Mule Studio from this [Location](http://www.mulesoft.com/platform/mule-studio)
+ You can download Mule ESB from this [Location](http://www.mulesoft.com/platform/soa/mule-esb-open-source-esb)


### Importing an Anypoint Template into Studio
Mule Studio offers several ways to import a project into the workspace, for instance: 

+ Anypoint Studio Project from File System
+ Packaged mule application (.jar)

You can find a detailed description on how to do so in this [Documentation Page](http://www.mulesoft.org/documentation/display/current/Importing+and+Exporting+in+Studio).


### Running on Studio <a name="runonstudio"/>
Once you have imported you Anypoint Template into Anypoint Studio you need to follow these steps to run it:

+ Locate the properties file `mule.dev.properties`, in src/main/resources
+ Complete all the properties required as per the examples in the section [Properties to be configured](#propertiestobeconfigured)
+ Once that is done, right click on you Anypoint Template project folder 
+ Hover you mouse over `"Run as"`
+ Click on  `"Mule Application"`


### Running on Mule ESB stand alone <a name="runonmuleesbstandalone"/>
Complete all properties in one of the property files, for example in [mule.prod.properties] (../master/src/main/resources/mule.prod.properties) and run your app with the corresponding environment variable to use it. To follow the example, this will be `mule.env=prod`. 


## Running on CloudHub <a name="runoncloudhub"/>
While [creating your application on CloudHub](http://www.mulesoft.org/documentation/display/current/Hello+World+on+CloudHub) (Or you can do it later as a next step), you need to go to Deployment > Advanced to set all environment variables detailed in **Properties to be configured** as well as the **mule.env**.


### Deploying your Anypoint Template on CloudHub <a name="deployingyouranypointtemplateoncloudhub"/>
Mule Studio provides you with really easy way to deploy your Template directly to CloudHub, for the specific steps to do so please check this [link](http://www.mulesoft.org/documentation/display/current/Deploying+Mule+Applications#DeployingMuleApplications-DeploytoCloudHub)


## Properties to be configured (With examples) <a name="propertiestobeconfigured"/>
In order to use this Mule Anypoint Template you need to configure properties (Credentials, configurations, etc.) either in properties file or in CloudHub as Environment Variables. Detail list with examples:
### Application configuration
#### Application configuration

scheduler.frequency `40000`
scheduler.start.delay `0`

# Watermarking default last query timestamp e.g. 2017-12-13T03:00:59Z
watermark.default.expression `2017-12-13T03:00:59Z`

#### Workday Connector configuration
+ wday.username `bob.dylan@orga`
+ wday.tenant `org457`
+ wday.password `DylanPassword123`
+ wday.host `servise425546.workday.com`

#### SAP Connector configuration
+ sap.jco.ashost `your.sap.address.com`
+ sap.jco.user `SAP_USER`
+ sap.jco.passwd `SAP_PASS`
+ sap.jco.sysnr `14`
+ sap.jco.client `800`
+ sap.jco.lang `EN`

### SAP HR configuration

+ sap.hire.org.COMP_CODE `3000`
+ sap.hire.org.PERS_AREA `300`
+ sap.hire.org.EMPLOYEE_GROUP `1`
+ sap.hire.org.EMPLOYEE_SUBGROUP `U5`
+ sap.hire.org.PERSONNEL_SUBAREA `0001`
+ sap.hire.org.LEGAL_PERSON `0001`
+ sap.hire.org.PAYROLL_AREA `PR`
+ sap.hire.org.COSTCENTER `4130`
+ sap.hire.org.ORG_UNIT `50000590`
+ sap.hire.org.POSITION `50000046`
+ sap.hire.org.JOB `50052752`
+ sap.hire.personal.PERSIDNO `50052755`
+ sap.hire.hr_infotype.TO_DATE `50052757`
+ sap.hire.default.dob `1980-01-01`

# API Calls <a name="apicalls"/>
There are no special considerations regarding API calls.


# Customize It!<a name="customizeit"/>
This brief guide intends to give a high level idea of how this Anypoint Template is built and how you can change it according to your needs.
As mule applications are based on XML files, this page will be organized by describing all the XML that conform the Anypoint Template.
Of course more files will be found such as Test Classes and [Mule Application Files](http://www.mulesoft.org/documentation/display/current/Application+Format), but to keep it simple we will focus on the XMLs.

Here is a list of the main XML files you'll find in this application:

* [config.xml](#configxml)
* [endpoints.xml](#endpointsxml)
* [businessLogic.xml](#businesslogicxml)
* [errorHandling.xml](#errorhandlingxml)


## config.xml<a name="configxml"/>
Configuration for Connectors and [Configuration Properties](http://www.mulesoft.org/documentation/display/current/Configuring+Properties) are set in this file. **Even you can change the configuration here, all parameters that can be modified here are in properties file, and this is the recommended place to do it so.** Of course if you want to do core changes to the logic you will probably need to modify this file.

In the visual editor they can be found on the *Global Element* tab.


## businessLogic.xml<a name="businesslogicxml"/>
This file holds the functional aspect of the template (points 2. to 3. described in the template overview). Its main component is a Batch job, and it includes *steps* for executing the broadcast operation from Workday to SAP.



## endpoints.xml<a name="endpointsxml"/>
This file should contain every inbound endpoint of your integration app. It is intended to contain the application API.
In this particular template, this file contains a scheduler endpoint that queries Workday for updates using a watermark.



## errorHandling.xml<a name="errorhandlingxml"/>
This is the right place to handle how your integration will react depending on the different exceptions. 
This file holds a [Error Handling](http://www.mulesoft.org/documentation/display/current/Error+Handling) that is referenced by the main flow in the business logic.



