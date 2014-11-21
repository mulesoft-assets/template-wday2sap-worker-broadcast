/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.templates.utils.Employee;

import com.mulesoft.module.batch.BatchTestHelper;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {

	private static final long TIMEOUT_MILLIS = 180000;
	private static final long DELAY_MILLIS = 500;
	protected static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
	protected static final int TIMEOUT_SEC = 60;
	private static final String TEST_NAME = "wday";
	private BatchTestHelper helper;
	private Employee user;	
	private static String WORKDAY_ID;	
    
    @BeforeClass
    public static void beforeTestClass() {
        System.setProperty("poll.startDelayMillis", "8000");
        System.setProperty("poll.frequencyMillis", "30000");               
    }

    @Before
    public void setUp() throws Exception {
		final Properties props = new Properties();
    	try {
    		props.load(new FileInputStream(PATH_TO_TEST_PROPERTIES));
    	} catch (Exception e) {
    	   logger.error("Error occured while reading mule.test.properties", e);
    	} 
    	WORKDAY_ID = props.getProperty("wday.testuser.id");
    	helper = new BatchTestHelper(muleContext);
		stopFlowSchedulers(POLL_FLOW_NAME);
		registerListeners();
		
		createTestDataInSandBox(generateEmployee());
    }

    @After
    public void tearDown() throws MuleException, Exception {
    	deleteTestDataFromSandBox();
    }
    
    private void registerListeners() throws NotificationException {
		muleContext.registerListener(pipelineListener);
	}
    
    private void createTestDataInSandBox(Employee user) throws MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("updateWorkdayEmployee");
		flow.initialise();
		logger.info("updating a workday employee...");
		try {
			flow.process(getTestEvent(user, MessageExchangePattern.REQUEST_RESPONSE));						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
    private Employee generateEmployee(){    	
//    	user = new Employee(TEST_NAME + System.currentTimeMillis(), TEST_NAME + System.currentTimeMillis(), WORKDAY_ID);  
    	user = new Employee(TEST_NAME, TEST_NAME, "" + System.currentTimeMillis(), WORKDAY_ID);  
		return user;
	}
        
	@SuppressWarnings("unchecked")
	@Test
    public void testMainFlow() throws Exception {
		// the first one is testing insert branch
		basicTest();
		
		// the second one is testing update branch
		
//		basicTest();
    }

	private void basicTest() throws InterruptedException, Exception,
			InitialisationException, MuleException {
		Thread.sleep(10000);
		runSchedulersOnce(POLL_FLOW_NAME);
		waitForPollToRun();
		helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
		helper.assertJobWasSuccessful();	
				
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("getSAPEmployee");
		flow.initialise();
		Map<String, String> map = new HashMap<String, String>();
		map.put("firstName", user.getGivenName());
		map.put("lastName", user.getFamilyName());
		MuleEvent response = flow.process(getTestEvent(map, MessageExchangePattern.REQUEST_RESPONSE));
		Map<String, String> sapEmployee = (Map<String, String>) response.getMessage().getPayload();
		logger.info("sap employee: " + sapEmployee);
		assertNotNull("SAP Employee should have been synced", sapEmployee.get("id"));
		assertEquals("SAP Employee First Name should be synced", user.getGivenName(), sapEmployee.get("firstName"));
		assertEquals("SAP Employee Last Name should be synced", user.getFamilyName(), sapEmployee.get("lastName"));
	}    
    
    private void deleteTestDataFromSandBox() throws MuleException, Exception {
    	logger.info("deleting test data...");		    					

	}       
	
}
