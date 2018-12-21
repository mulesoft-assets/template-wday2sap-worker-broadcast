
# Anypoint Template: Workday to SAP Worker Broadcast	

<!-- Header (start) -->
As worker information is added or removed in Workday, you may find the need to update employees in SAP that you use as your ERP system. This template allows you to broadcast using a one way sync those changes to workers in Workday to employees in SAP in real time. The detection criteria, and fields that should be moved are configurable. Additional systems can easily added to be notified of changes. Real time synchronization is achieved via rapid polling of Workday or you can slow down the interval to something near real time. This template uses both our batching and our watermarking capabilities to ensure that only recent changes are captured and so that it efficiently processes large amounts of records if you choose to slow down the polling interval.

![2b65d5ad-7781-4b52-b14a-275c2242b54c-image.png](https://exchange2-file-upload-service-kprod.s3.us-east-1.amazonaws.com:443/2b65d5ad-7781-4b52-b14a-275c2242b54c-image.png)
<!-- Header (end) -->

# License Agreement
This template is subject to the conditions of the <a href="https://s3.amazonaws.com/templates-examples/AnypointTemplateLicense.pdf">MuleSoft License Agreement</a>. Review the terms of the license before downloading and using this template. You can use this template for free with the Mule Enterprise Edition, CloudHub, or as a trial in Anypoint Studio. 
# Use Case
<!-- Use Case (start) -->
As a Workday admin I want to synchronize Workers from Workday to SAP.

As implemented, this template leverages the Mule batch module.
The batch job is divided in Process and On Complete stages.

1. The integration is triggered by scheduler to Workday instance. New or modified workers are passed to the batch as input. Workers without at least one e-mail address assigned are filtered.
2. In the batch, the employee is fetched from SAP by the email and mapped to SAP input data structure.
3. Afterwards every employee is sent to destination instance - to SAP where it is asynchronously updated or created.
<!-- Use Case (end) -->

# Considerations
<!-- Default Considerations (start) -->

<!-- Default Considerations (end) -->

<!-- Considerations (start) -->
There are certain pre-requisites that must be considered to run this template. All of them deal with the preparations in both source and destination systems, that must be made for the template to run smoothly. Failing to do so can lead to unexpected behavior of the template.
There are a couple of things you should take into account before running this template:

Workday email uniqueness: The email can be repeated for two or more workers (or missing). Therefore Workday workers with duplicate emails are removed from processing in the Process stage.

## Disclaimer
This Anypoint template uses a few private Maven dependencies from MuleSoft to work. If you intend to run this template with Maven support, you need to add three extra dependencies for SAP to the pom.xml. See [SAP XML and Maven Support](https://docs.mulesoft.com/connectors/sap/sap-connector#xml-and-maven-support).
<!-- Considerations (end) -->


## SAP Considerations

Here's what you need to know to get this template to work with SAP.


### As a Data Destination

This template uses custom BAPI functions, the create function module `ZHCMFM_NUMBER_GET_NEXT` in transaction `SE37` as per source file `ZHCMFM_NUMBER_GET_NEXT.abap`. Referenced files are in the src/main/resources directory.

## Workday Considerations

### As a Data Source

There are no considerations with using Workday as a data origin.

# Run it!
Simple steps to get this template running.
<!-- Run it (start) -->

<!-- Run it (end) -->

## Running On Premises
In this section we help you run this template on your computer.
<!-- Running on premise (start) -->

<!-- Running on premise (end) -->

### Where to Download Anypoint Studio and the Mule Runtime
If you are new to Mule, download this software:

+ [Download Anypoint Studio](https://www.mulesoft.com/platform/studio)
+ [Download Mule runtime](https://www.mulesoft.com/lp/dl/mule-esb-enterprise)

**Note:** Anypoint Studio requires JDK 8.
<!-- Where to download (start) -->

<!-- Where to download (end) -->

### Importing a Template into Studio
In Studio, click the Exchange X icon in the upper left of the taskbar, log in with your Anypoint Platform credentials, search for the template, and click Open.
<!-- Importing into Studio (start) -->

<!-- Importing into Studio (end) -->

### Running on Studio
After you import your template into Anypoint Studio, follow these steps to run it:

1. Locate the properties file `mule.dev.properties`, in src/main/resources.
2. Complete all the properties required per the examples in the "Properties to Configure" section.
3. Right click the template project folder.
4. Hover your mouse over `Run as`.
5. Click `Mule Application (configure)`.
6. Inside the dialog, select Environment and set the variable `mule.env` to the value `dev`.
7. Click `Run`.

For this template to run in Anypoint Studio, you need to [install SAP Libraries](https://docs.mulesoft.com/connectors/sap/sap-connector#install-sap-libraries).

<!-- Running on Studio (start) -->

<!-- Running on Studio (end) -->

### Running on Mule Standalone
Update the properties in one of the property files, for example in mule.prod.properties, and run your app with a corresponding environment variable. In this example, use `mule.env=prod`.

## Running on CloudHub
When creating your application in CloudHub, go to Runtime Manager > Manage Application > Properties to set the environment variables listed in "Properties to Configure" as well as the mule.env value.
<!-- Running on Cloudhub (start) -->

<!-- Running on Cloudhub (end) -->

### Deploying a Template in CloudHub
In Studio, right click your project name in Package Explorer and select Anypoint Platform > Deploy on CloudHub.
<!-- Deploying on Cloudhub (start) -->

<!-- Deploying on Cloudhub (end) -->

## Properties to Configure
To use this template, configure properties such as credentials, configurations, etc.) in the properties file or in CloudHub from Runtime Manager > Manage Application > Properties. The sections that follow list example values.
### Application Configuration
<!-- Application Configuration (start) -->
#### Application Configuration

- scheduler.frequency `40000`
- scheduler.start.delay `0`

# Watermarking default last query timestamp for example 2018-12-13T03:00:59Z

- watermark.default.expression `2018-12-13T03:00:59Z`

#### Workday Connector Configuration

- wday.username `bob.dylan@orga`
- wday.tenant `org457`
- wday.password `DylanPassword123`
- wday.host `servise425546.workday.com`

#### SAP Connector Configuration

- sap.jco.ashost `your.sap.address.com`
- sap.jco.user `SAP_USER`
- sap.jco.passwd `SAP_PASS`
- sap.jco.sysnr `14`
- sap.jco.client `800`
- sap.jco.lang `EN`

### SAP HR Configuration

- sap.hire.org.COMP_CODE `3000`
- sap.hire.org.PERS_AREA `300`
- sap.hire.org.EMPLOYEE_GROUP `1`
- sap.hire.org.EMPLOYEE_SUBGROUP `U5`
- sap.hire.org.PERSONNEL_SUBAREA `0001`
- sap.hire.org.LEGAL_PERSON `0001`
- sap.hire.org.PAYROLL_AREA `PR`
- sap.hire.org.COSTCENTER `4130`
- sap.hire.org.ORG_UNIT `50000590`
- sap.hire.org.POSITION `50000046`
- sap.hire.org.JOB `50052752`
- sap.hire.personal.PERSIDNO `50052755`
- sap.hire.hr_infotype.TO_DATE `50052757`
- sap.hire.default.dob `1980-01-01`
<!-- Application Configuration (end) -->

# API Calls
<!-- API Calls (start) -->
There are no special considerations regarding API calls.
<!-- API Calls (end) -->

# Customize It!
This brief guide provides a high level understanding of how this template is built and how you can change it according to your needs. As Mule applications are based on XML files, this page describes the XML files used with this template. More files are available such as test classes and Mule application files, but to keep it simple, we focus on these XML files:

* config.xml
* businessLogic.xml
* endpoints.xml
* errorHandling.xml
<!-- Customize it (start) -->

<!-- Customize it (end) -->

## config.xml
<!-- Default Config XML (start) -->
This file provides the configuration for connectors and configuration properties. Only change this file to make core changes to the connector processing logic. Otherwise, all parameters that can be modified should instead be in a properties file, which is the recommended place to make changes.
<!-- Default Config XML (end) -->

<!-- Config XML (start) -->

<!-- Config XML (end) -->

## businessLogic.xml
<!-- Default Business Logic XML (start) -->
This file holds the functional aspect of the template (points 2. to 3. described in the template overview). Its main component is a Batch job, and it includes *steps* for executing the broadcast operation from Workday to SAP.
<!-- Default Business Logic XML (end) -->

<!-- Business Logic XML (start) -->

<!-- Business Logic XML (end) -->

## endpoints.xml
<!-- Default Endpoints XML (start) -->
This file should contain every inbound endpoint of your integration app. It is intended to contain the application API.
In this template, this file contains a scheduler endpoint that queries Workday for updates using a watermark.
<!-- Default Endpoints XML (end) -->

<!-- Endpoints XML (start) -->

<!-- Endpoints XML (end) -->

## errorHandling.xml
<!-- Default Error Handling XML (start) -->
This file handles how your integration reacts depending on the different exceptions. This file provides error handling that is referenced by the main flow in the business logic.
<!-- Default Error Handling XML (end) -->

<!-- Error Handling XML (start) -->

<!-- Error Handling XML (end) -->

<!-- Extras (start) -->

<!-- Extras (end) -->
