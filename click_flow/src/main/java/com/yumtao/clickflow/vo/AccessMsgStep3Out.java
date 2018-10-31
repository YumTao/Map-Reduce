package com.yumtao.clickflow.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

import com.yumtao.clickflow.util.DateUtil;

/**
 * @author yumTao
 *
 */
public class AccessMsgStep3Out implements WritableComparable<AccessMsgStep3Out> {

	private String session;
	private String starttime;
	private String endtime;
	private String starturl;
	private String endurl;
	private int accessPageNum;
	private String ip;
	private String cookie;
	private String referal;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(session);
		out.writeUTF(starttime);
		out.writeUTF(endtime);
		out.writeUTF(starturl);
		out.writeUTF(endurl);
		out.writeInt(accessPageNum);
		out.writeUTF(ip);
		out.writeUTF(cookie);
		out.writeUTF(referal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.session = in.readUTF();
		this.starttime = in.readUTF();
		this.endtime = in.readUTF();
		this.starturl = in.readUTF();
		this.endurl = in.readUTF();
		this.accessPageNum = in.readInt();
		this.ip = in.readUTF();
		this.cookie = in.readUTF();
		this.referal = in.readUTF();
	}

	@Override
	public int compareTo(AccessMsgStep3Out o) {
		return 0;
	}

	public AccessMsgStep3Out() {

	}

	public AccessMsgStep3Out(String session, String starttime, String endtime, String starturl, String endurl,
			int accessPageNum, String ip, String cookie, String referal) {
		this.session = session;
		this.starttime = starttime;
		this.endtime = endtime;
		this.starturl = starturl;
		this.endurl = endurl;
		this.accessPageNum = accessPageNum;
		this.ip = ip;
		this.cookie = cookie;
		this.referal = referal;
	}

	@Override
	public String toString() {
		return session + "\t" + starttime + "\t" + endtime + "\t" + starturl + "\t" + endurl + "\t" + accessPageNum
				+ "\t" + ip + "\t" + cookie + "\t" + referal;
	}

	public Date getTime(String timestr) {
		try {
			return DateUtil.parse(timestr, DateUtil.destSdf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getStarttime() {
		return starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}

	public String getEndtime() {
		return endtime;
	}

	public void setEndtime(String endtime) {
		this.endtime = endtime;
	}

	public String getStarturl() {
		return starturl;
	}

	public void setStarturl(String starturl) {
		this.starturl = starturl;
	}

	public String getEndurl() {
		return endurl;
	}

	public void setEndurl(String endurl) {
		this.endurl = endurl;
	}

	public int getAccessPageNum() {
		return accessPageNum;
	}

	public void setAccessPageNum(int accessPageNum) {
		this.accessPageNum = accessPageNum;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public String getReferal() {
		return referal;
	}

	public void setReferal(String referal) {
		this.referal = referal;
	}

}
