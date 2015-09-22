/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import java.io.FileInputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {

	private static final long TIMEOUT_MILLIS = 300000;
	private static final long DELAY_MILLIS = 500;
	private static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
	private static final String TEST_USER_NAME_PREFIX = "wday2sap";
	private static final Logger LOGGER = LogManager.getLogger(BusinessLogicIT.class);
	private static String WORKDAY_ID;
	private static String EMAIL_SUFFIX = "@broadcast.com";

	private BatchTestHelper helper;
	private SubflowInterceptingChainLifecycleWrapper getSapEmployeeByEmailFlow;
	private SubflowInterceptingChainLifecycleWrapper getSapEmployeeByNameFlow;
	private SubflowInterceptingChainLifecycleWrapper terminateSapEmployeeFlow;
	private SubflowInterceptingChainLifecycleWrapper updateWorkersEmailFlow;
	private SubflowInterceptingChainLifecycleWrapper updateWorkersNameFlow;

	@BeforeClass
	public static void beforeTestClass() {
		System.setProperty("poll.startDelayMillis", "8000");
		System.setProperty("poll.frequencyMillis", "30000");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -3);
		System.setProperty(
				"watermark.defaultExpression",
				"#[groovy: new GregorianCalendar(" 
						+ cal.get(Calendar.YEAR) + ","
						+ cal.get(Calendar.MONTH) + ","
						+ cal.get(Calendar.DAY_OF_MONTH) + ","
						+ cal.get(Calendar.HOUR_OF_DAY) + ","
						+ cal.get(Calendar.MINUTE) + ","
						+ cal.get(Calendar.SECOND) + ") ]");
	}

	@Before
	public void setUp() throws Exception {
		final Properties props = new Properties();
		try {
			props.load(new FileInputStream(PATH_TO_TEST_PROPERTIES));
		} catch (Exception e) {
			LOGGER.error("Error occured while reading mule.test.properties", e);
		}
		WORKDAY_ID = props.getProperty("wday.testuser.id");
		helper = new BatchTestHelper(muleContext);
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		initializeSubFlows();
	}
	
	@AfterClass
	public static void afterTestClass() {
		System.clearProperty("poll.startDelayMillis");
		System.clearProperty("poll.frequencyMillis");
		System.clearProperty("watermark.defaultExpression");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCreateFlow() throws Exception {
		// change worker's email in Workday
		Map<String, String> worker = generateWorkerEmailPayload();
		updateWorkersEmailFlow.process(getTestEvent(worker, MessageExchangePattern.REQUEST_RESPONSE));

		// this is testing the insert branch
		runMainFlow();

		// retrieve data from SAP
		MuleEvent response = getSapEmployeeByEmailFlow.process(getTestEvent(worker, MessageExchangePattern.REQUEST_RESPONSE));
		Map<String, String> sapEmployee = (Map<String, String>) response.getMessage().getPayload();
		LOGGER.info("sap employee after create: " + sapEmployee);

		Assert.assertNotNull("SAP Employee should have been synced", sapEmployee);
		Assert.assertNotNull("First name should be fetched", sapEmployee.get("FirstName"));
		Assert.assertNotNull("Last name should be fetched", sapEmployee.get("LastName"));

		// remove test data from SAP, moved here as @After would cause the
		// redundant and invalid remove call
		terminateSapEmployeeFlow.process(getTestEvent(sapEmployee));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUpdateFlow() throws Exception {
		// change worker's name in Workday
		Map<String, String> worker = generateWorkerNamePayload();
		updateWorkersNameFlow.process(getTestEvent(worker,MessageExchangePattern.REQUEST_RESPONSE));

		// this is testing the update branch
		runMainFlow();

		// retrieve data from SAP
		MuleEvent response = getSapEmployeeByNameFlow.process(getTestEvent(worker, MessageExchangePattern.REQUEST_RESPONSE));
		Map<String, String> sapEmployee = (Map<String, String>) response.getMessage().getPayload();
		LOGGER.info("sap employee: " + sapEmployee);

		Assert.assertNotNull("SAP Employee should have been synced", sapEmployee);
		Assert.assertEquals("First name should match", worker.get("FirstName"),	sapEmployee.get("FirstName"));
		Assert.assertEquals("Last name should match", worker.get("LastName"), sapEmployee.get("LastName"));

	}

	private void runMainFlow() throws Exception {
		Thread.sleep(10*1000);
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();
		helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
		helper.assertJobWasSuccessful();
	}

	private Map<String, String> generateWorkerEmailPayload() {
		Map<String, String> employee = new HashMap<String, String>();
		employee.put("Email", System.currentTimeMillis() + EMAIL_SUFFIX);
		employee.put("Id", WORKDAY_ID);
		LOGGER.debug("E-mail: " + employee);
		return employee;
	}

	private Map<String, String> generateWorkerNamePayload() {
		Map<String, String> employee = new HashMap<>();
		employee.put("FirstName", TEST_USER_NAME_PREFIX);
		employee.put("LastName", TEST_USER_NAME_PREFIX + System.currentTimeMillis());
		employee.put("MiddleName", "");
		employee.put("Id", WORKDAY_ID);
		LOGGER.debug("Worker: " + employee);
		return employee;
	}
	
	private void initializeSubFlows() throws InitialisationException {
		getSapEmployeeByEmailFlow = getSubFlow("getSAPEmployeeByEmail");
		getSapEmployeeByEmailFlow.initialise();

		getSapEmployeeByNameFlow = getSubFlow("getSAPEmployeeByName");
		getSapEmployeeByNameFlow.initialise();

		terminateSapEmployeeFlow = getSubFlow("terminateSAPEmployee");
		terminateSapEmployeeFlow.initialise();

		updateWorkersNameFlow = getSubFlow("updateWorkdayEmployeeName");
		updateWorkersNameFlow.initialise();

		updateWorkersEmailFlow = getSubFlow("updateWorkdayEmployeeEmail");
		updateWorkersEmailFlow.initialise();
	}
	
	private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}
}
