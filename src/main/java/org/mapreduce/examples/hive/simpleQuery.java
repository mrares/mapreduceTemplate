package org.mapreduce.examples.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class simpleQuery {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final Logger log = LoggerFactory.getLogger(simpleQuery.class);

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			log.info("Usage: hadoop jar <jar file> " + simpleQuery.class.getName() + " <hive server:port> [<query to run, eg: show tables>]");
			return;
		}
		
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String hiveServer = args[0];
		String query = (args.length == 2) ? args[1] : "show tables";
		
		//replace "hive" here with the name of the user the queries should run as
		Connection con = DriverManager.getConnection("jdbc:hive2://" + hiveServer + "/default;auth=noSasl", "javaMR", "");
		Statement stmt = con.createStatement();
		ResultSet res = stmt.executeQuery(query);
		
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		
	}
}
