package capricorn.q.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * @Description : PhoenixUtil
 * @Author : Capricorn.QBB
 * @Date : 2019-04-04
 * @Version : 1.0
 */
public class PhoenixUtil {

	private static LinkedList<Connection> connectionQueue;
	private static final int QUEUE_LEN = 5;

	static {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public synchronized static Connection getConnection(String zkUrl) {
		try {
			if (connectionQueue == null) {
				connectionQueue = new LinkedList<>();
				for (int i = 0; i < QUEUE_LEN; i++) {
					Connection conn = DriverManager.getConnection("jdbc:phoenix:" + zkUrl);
					connectionQueue.push(conn);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectionQueue.poll();
	}

	public static void returnConnection(Connection conn) {
		connectionQueue.push(conn);
	}
}
