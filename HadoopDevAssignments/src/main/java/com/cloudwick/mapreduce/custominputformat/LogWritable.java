/**
 * 
 */
package com.cloudwick.mapreduce.custominputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Omkar
 *
 */
public class LogWritable implements Writable{

	private String ipAddr;
	private String clientIdentity;
	private String userId;
	private String timeStamp;
	private String requestType;
	private String requestPage;
	private String httpProt;
	private int responseCode;
	private int responseSize;
	private String referrer;
	private String userAgent;
	
	
	
	/**
	 * @param ipAddr
	 * @param clientIdentity
	 * @param userId
	 * @param timeStamp
	 * @param requestType
	 * @param requestPage
	 * @param httpProt
	 * @param responseCode
	 * @param responseSize
	 * @param referrer
	 * @param userAgent
	 */
	public LogWritable(String ipAddr, String clientIdentity, String userId,
			String timeStamp, String requestType, String requestPage,
			String httpProt, int responseCode, int responseSize,
			String referrer, String userAgent) {
		this.ipAddr = ipAddr;
		this.clientIdentity = clientIdentity;
		this.userId = userId;
		this.timeStamp = timeStamp;
		this.requestType = requestType;
		this.requestPage = requestPage;
		this.httpProt = httpProt;
		this.responseCode = responseCode;
		this.responseSize = responseSize;
		this.referrer = referrer;
		this.userAgent = userAgent;
	}

	public LogWritable() {
		// TODO Auto-generated constructor stub
		this.ipAddr = null;
		this.clientIdentity = null;
		this.userId = null;
		this.timeStamp = null;
		this.requestType = null;
		this.requestPage = null;
		this.httpProt = null;
		this.responseCode = 0;
		this.responseSize = 0;
		this.referrer = null;
		this.userAgent = null;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	public String getClientIdentity() {
		return clientIdentity;
	}

	public void setClientIdentity(String clientIdentity) {
		this.clientIdentity = clientIdentity;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getRequestType() {
		return requestType;
	}

	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}

	public String getRequestPage() {
		return requestPage;
	}

	public void setRequestPage(String requestPage) {
		this.requestPage = requestPage;
	}

	public String getHttpProt() {
		return httpProt;
	}

	public void setHttpProt(String httpProt) {
		this.httpProt = httpProt;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public int getRepsonseSize() {
		return responseSize;
	}

	public void setRepsonseSize(int repsonseSize) {
		this.responseSize = repsonseSize;
	}

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ipAddr = in.readUTF();
		clientIdentity = in.readUTF();
		userId = in.readUTF();
		timeStamp = in.readUTF();
		requestType = in.readUTF();
		requestPage = in.readUTF();
		httpProt = in.readUTF();;
		responseCode = in.readInt();
		responseSize = in.readInt();
		referrer = in.readUTF();
		userAgent = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(ipAddr.toString());
		out.writeUTF(clientIdentity.toString());
		out.writeUTF(userId.toString());
		out.writeUTF(timeStamp.toString());
		out.writeUTF(requestType.toString());
		out.writeUTF(requestPage.toString());
		out.writeUTF(httpProt.toString());
		out.writeInt(responseCode);
		out.writeInt(responseSize);
		out.writeUTF(referrer.toString());
		out.writeUTF(userAgent.toString());
	}

}
