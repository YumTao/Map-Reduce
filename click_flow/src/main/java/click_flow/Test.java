package click_flow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.yumtao.clickflow.vo.AccessMsgByStep2;

public class Test {
	public static void main(String[] args) {

		AccessMsgByStep2 a1 = new AccessMsgByStep2("1111", "1111", "-", "1111", "-", "-");
		AccessMsgByStep2 a2 = new AccessMsgByStep2("2222", "2222", "-", "2222", "-", "-");
		AccessMsgByStep2 a3 = new AccessMsgByStep2("3333", "3333", "-", "3333", "-", "-");

		List<AccessMsgByStep2> agoList = new ArrayList<>();
		agoList.add(a1);
		agoList.add(a2);
		agoList.add(a3);

		List<AccessMsgByStep2> nowList = new ArrayList<>();
		for (AccessMsgByStep2 accessMsgByStep : agoList) {
			nowList.add(accessMsgByStep);
		}
		System.out.println(nowList);

		List<AccessMsgByStep2> nowList2 = new ArrayList<>();
		Iterator<AccessMsgByStep2> iterator = agoList.iterator();

		while (iterator.hasNext()) {
			AccessMsgByStep2 next = iterator.next();
			nowList2.add(next);
		}

		System.out.println(nowList2);
	}

}
