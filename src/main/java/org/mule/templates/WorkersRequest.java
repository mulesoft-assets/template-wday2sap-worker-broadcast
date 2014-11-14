package org.mule.templates;

import java.text.ParseException;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.workday.hr.EffectiveAndUpdatedDateTimeDataType;
import com.workday.hr.GetWorkersRequestType;
import com.workday.hr.TransactionLogCriteriaType;
import com.workday.hr.WorkerRequestCriteriaType;

public class WorkersRequest {

	public static GetWorkersRequestType create(Date startDate) throws ParseException, DatatypeConfigurationException {

		/*
		 * Set data range for events
		 */

		EffectiveAndUpdatedDateTimeDataType dateRangeData = new EffectiveAndUpdatedDateTimeDataType();
		dateRangeData.setUpdatedFrom(xmlDate(startDate));
		dateRangeData.setUpdatedThrough(xmlDate(new Date()));

		/*
		 * Set event type criteria filter
		 */

		TransactionLogCriteriaType transactionLogCriteria = new TransactionLogCriteriaType();
		transactionLogCriteria.setTransactionDateRangeData(dateRangeData);

		WorkerRequestCriteriaType workerRequestCriteria = new WorkerRequestCriteriaType();
		workerRequestCriteria.getTransactionLogCriteriaData().add(transactionLogCriteria);
		GetWorkersRequestType getWorkersType = new GetWorkersRequestType();
		getWorkersType.setRequestCriteria(workerRequestCriteria);
		
		return getWorkersType;
	}

	private static XMLGregorianCalendar xmlDate(Date date) throws DatatypeConfigurationException {
		GregorianCalendar gregorianCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
		gregorianCalendar.setTime(date);
		return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
	}

}
