package click_flow;

import java.util.Arrays;

public class Test {
	public static void main(String[] args) {
//		String msString = "194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] \"GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/4.0 (compatible;)\"";
		
		String msString = "124.42.13.230 - - [18/Sep/2013:06:57:48 +0000] \"GET /mongodb-replica-set/ HTTP/1.1\" 200 50131 \"http://www.baidu.com/s?tn=baiduhome_pg&ie=utf-8&bs=%E5%9C%A8linux%E5%90%AF%E5%8A%A8%E4%B8%8Bmongodb.conf&f=8&rsv_bp=1&wd=about+to+fork+child+process%2C+waiting+until+server+is+ready+for+connections.&rsv_n=2&rsv_sug3=1&rsv_sug1=1&rsv_sug4=187&inputT=906\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; BTRS101170; InfoPath.2; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727)\"";
		/*Arrays.asList(msString.split(" ")).stream().forEach(System.out::println);

		String ip = msString.split(" ")[0];
		String timestamp = msString.split(" ")[3];
		String referal = msString.split(" ")[10];

		System.out.println(String.format("ip: %s, timestamp: %s, referal: %s", ip, timestamp, referal));*/
		
		int indexOf = msString.indexOf("\"");
		int indexOf2 = msString.indexOf("\"", indexOf + 1);
		System.out.println(msString.substring(indexOf));
		System.out.println(msString.substring(indexOf2));
		System.out.println(msString.substring(indexOf + 1, indexOf2));
	}

}
