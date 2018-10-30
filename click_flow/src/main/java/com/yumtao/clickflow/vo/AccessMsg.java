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
public class AccessMsg implements WritableComparable<AccessMsg> {

	private String timestamp;
	private String ip;
	private String cookie;
	private String session;
	private String url;
	private String referal;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(timestamp);
		out.writeUTF(ip);
		out.writeUTF(cookie);
		out.writeUTF(session);
		out.writeUTF(url);
		out.writeUTF(referal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readUTF();
		this.ip = in.readUTF();
		this.cookie = in.readUTF();
		this.session = in.readUTF();
		this.url = in.readUTF();
		this.referal = in.readUTF();
	}

	@Override
	public int compareTo(AccessMsg o) {
		try {
			Date right = o.getTime();
			Date left = this.getTime();
			return Long.valueOf(DateUtil.getMillSecBetween(right, left)).intValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public AccessMsg() {
	}

	public AccessMsg(String timestamp, String ip, String cookie, String session, String url, String referal) {
		this.timestamp = timestamp;
		this.ip = ip;
		this.cookie = cookie;
		this.session = session;
		this.url = url;
		this.referal = referal;
	}

	@Override
	public String toString() {
		return timestamp + "\t" + ip + "\t" + cookie + "\t" + session + "\t" + url + "\t" + referal;
	}

	public Date getTime() {
		try {
			return DateUtil.parse(this.timestamp, DateUtil.destSdf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
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

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getReferal() {
		return referal;
	}

	public void setReferal(String referal) {
		this.referal = referal;
	}

}
