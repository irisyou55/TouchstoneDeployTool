package run;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;

import DataType.*;
import Schema.Attribute;
import Schema.SchemaReader;
import Schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBStatisticsCollector {
	private static final Logger LOGGER = LoggerFactory.getLogger(DBStatisticsCollector.class);
	// target database (original database)
	private String ip = null;
	private String port = null;
	private String dbName = null;
	private String userName = null;
	private String passwd = null;

	private List<Table> tables = null;

	public DBStatisticsCollector(String ip, String port, String dbName, String userName, String passwd,
			List<Table> tables) {
		super();
		this.ip = ip;
		this.port = port;
		this.dbName = dbName;
		this.userName = userName;
		this.passwd = passwd;
		this.tables = tables;
	}

	@SuppressWarnings("resource")
	public void run() {
		Connection conn = DBConnector.getDBConnection(ip, port, dbName, userName, passwd);
		try {
			Statement stmt = conn.createStatement();
			for (int i = 0; i < tables.size(); i++) {
				Table table = tables.get(i);

				List<Attribute> attributes = table.getAttributes();
				for (int j = 0; j < attributes.size(); j++) {
					Attribute attribute = attributes.get(j);

					ResultSet rs = stmt.executeQuery("select count(*) from " + table.getTableName() + 
							" where " + attribute.getAttrName() + " is null");
					rs.next();
					float nullRatio = (float)rs.getLong(1) / table.getTableSize();

					rs = stmt.executeQuery("select count(distinct(" + attribute.getAttrName() + ")) from " + 
							table.getTableName() + " where " + attribute.getAttrName() + " is not null");
					rs.next();
					long cardinality = rs.getLong(1);

					switch (attribute.getDataType()) {
					case "integer":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						long minValue = rs.getLong(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						long maxValue = rs.getLong(1);

						attribute.setDataTypeInfo(new TSInteger(nullRatio, cardinality, minValue, maxValue));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ cardinality + ";" + minValue + ";" + maxValue + "]");
						break;
					case "real":
					case "decimal":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						double minValue2 = rs.getDouble(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						double maxValue2 = rs.getDouble(1);

						if (attribute.getDataType().equals("real")) {
							attribute.setDataTypeInfo(new TSReal(nullRatio, minValue2, maxValue2));
						} else {
							attribute.setDataTypeInfo(new TSDecimal(nullRatio, minValue2, maxValue2));
						}
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ minValue2 + ";" + maxValue2 + "]");
						break;
					case "date":
					case "datetime":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						Date minDate = rs.getDate(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						Date maxDate = rs.getDate(1);

						if (attribute.getDataType().equals("date")) {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							attribute.setDataTypeInfo(new TSDate(nullRatio, sdf.format(minDate), sdf.format(maxDate)));
							System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
									+ sdf.format(minDate) + ";" + sdf.format(maxDate) + "]");
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
							attribute.setDataTypeInfo(new TSDateTime(nullRatio, sdf.format(minDate), sdf.format(maxDate)));
							System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
									+ sdf.format(minDate) + ";" + sdf.format(maxDate) + "]");
						}
						break;
					case "varchar":
						rs = stmt.executeQuery("select avg(length(" + attribute.getAttrName() + ")) from " + table.getTableName());
						rs.next();
						float avgLength = rs.getFloat(1);
						rs = stmt.executeQuery("select max(length(" + attribute.getAttrName() + ")) from " + table.getTableName());
						rs.next();
						int maxLength = rs.getInt(1);

						attribute.setDataTypeInfo(new TSVarchar(nullRatio, avgLength, maxLength));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ avgLength + ";" + maxLength + "]");
						break;
					case "bool":
						rs = stmt.executeQuery("select count(*) from " + table.getTableName() + " where " + 
								attribute.getAttrName() + " is True");
						rs.next();
						float trueRatio = rs.getLong(1) / ((1 - nullRatio) * table.getTableSize());

						attribute.setDataTypeInfo(new TSBool(nullRatio, trueRatio));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ trueRatio + "]");
						break;
					}

					rs.close();
				} // for columns
			} // for tables
		} catch (SQLException e) {
			e.printStackTrace();
		}
		LOGGER.info("All table information after filling the data characteristics:" + tables);
	}

	public List<Table> getTables() {
		return tables;
	}

	public static void main(String[] args) {

		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read("src/test/input/tpch_schema_sf_1.txt");

		String ip = "127.0.0.1", port = "3306", dbName = "tpch", userName = "root", passwd = "youshuhong525";
		DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
		collector.run();
	}
}
