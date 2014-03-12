package com.levelsbeyond.postgres;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

public class AutoFKIndexDB {
	@SuppressWarnings("unused")
	private static final XMLOutputter xmlOut = new XMLOutputter(Format.getPrettyFormat());

	private static String constraintQuery =
			"SELECT tc.table_name, kcu.column_name, tc.constraint_name " +
					"FROM information_schema.table_constraints tc " +
					"LEFT JOIN information_schema.key_column_usage kcu " +
					"ON tc.constraint_catalog = kcu.constraint_catalog " +
					"AND tc.constraint_schema = kcu.constraint_schema " +
					"AND tc.constraint_name = kcu.constraint_name " +
					"WHERE lower(tc.constraint_type) = 'foreign key'";

	private static final String tuplesQuery = "SELECT reltuples::bigint FROM pg_constraint JOIN pg_class ON (conrelid = pg_class.oid) WHERE contype = 'f' AND conname = ?";

	private static final String hasIndexQuery = "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ? AND n.nspname = 'public'";

	private static final String createIndex = "CREATE INDEX %s ON public.%s (%s)";

	private static final String indexQuery = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexdef like 'CREATE INDEX%'";

	private static final Map<String, Pair<String, String>> indexMap = new HashMap<String, Pair<String, String>>();

	public static void main(String[] args) throws SQLException, IOException {
		String host = "localhost";
		String database = null;
		String user = null;
		String password = null;
		int minTuples = 1000;
		Boolean dropIndices = null;
		Boolean createIndices = null;
		boolean showHelp = false;

		for (String arg : args) {
			if (arg.startsWith("--host=")) {
				host = arg.substring(7);
			}
			else if (arg.startsWith("--database=")) {
				database = arg.substring(11);
			}
			else if (arg.startsWith("--user=")) {
				user = arg.substring(7);
			}
			else if (arg.startsWith("--password=")) {
				password = arg.substring(11);
			}
			else if (arg.startsWith("--min-tuples=")) {
				minTuples = Integer.valueOf(arg.substring(13));
			}
			else if (arg.startsWith("--drop-indices=")) {
				dropIndices = BooleanUtils.toBooleanObject(arg.substring(15));
			}
			else if (arg.startsWith("--create-indices=")) {
				createIndices = BooleanUtils.toBooleanObject(arg.substring(17));
			}
			else if (arg.startsWith("--help")) {
				showHelp = true;
			}
		}

		Properties props = new Properties();
		props.load(ClassLoader.getSystemResourceAsStream("default.index.properties"));
		try {
			props.load(new FileInputStream(getJarPath() + File.separator + "index.properties"));
		}
		catch (Exception e) {
			System.err.println("Couldn't locate index.properties -- ignoring.");
		}
		for (Entry<Object, Object> prop : props.entrySet()) {
			String indexName = prop.getKey().toString();
			String[] tableAndColumn = prop.getValue().toString().split("\\.");
			if (tableAndColumn.length == 2) {
				indexMap.put(indexName, new ImmutablePair<String, String>(tableAndColumn[0], tableAndColumn[1]));
			}
		}

		if (showHelp || database == null || user == null || password == null || (dropIndices == null && createIndices == null)) {
			System.out.println("Usage: java -jar AutoFKIndexDB.jar\nOptions:\n" +
					"\t--host=[" + host + "]\n" +
					"\t--database=[" + database + "]\n" +
					"\t--user=[" + user + "]\n" +
					"\t--password=[" + password + "]\n" +
					"\t--min-tuples=[" + minTuples + "]\n" +
					"\t--drop-indices=" + (dropIndices != null ? "[" + dropIndices + "]" : "true|false") + "\n" +
					"\t--create-indices=" + (createIndices != null ? "[" + createIndices + "]" : "true|false") + "\n" +
					"\t--help");
			System.exit(1);
		}

		Connection conn = null;
		try {
			String url = "jdbc:postgresql://" + host + ":5432/" + database;
			conn = DriverManager.getConnection(url, user, password);

			new AutoFKIndexDB().creasteFKIndices(conn, minTuples, BooleanUtils.toBoolean(dropIndices), BooleanUtils.toBoolean(createIndices));
		}
		catch (Exception e) {
			System.err.println("Got an exception: " + e.getMessage());
			e.printStackTrace();
		}
		finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	private static String jarPath;

	private static String getJarPath() {
		if (jarPath == null) {
			try {
				File jarFile = new File(AutoFKIndexDB.class.getProtectionDomain().getCodeSource().getLocation().toURI());
				jarPath = jarFile.getParentFile().getAbsolutePath();
			}
			catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		return jarPath;
	}

	private void creasteFKIndices(Connection conn, int minTuples, boolean dropIndices, boolean createIndices) throws SQLException {
		if (dropIndices) {
			deleteForeignKeyIndices(conn);
		}

		if (createIndices) {
			List<ForeignKeyConstraint> fkConstraints = new ArrayList<ForeignKeyConstraint>();
			PreparedStatement selectStmt = conn.prepareStatement(constraintQuery);
			ResultSet rs = selectStmt.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString(1);
				String columnName = rs.getString(2);
				String constraintName = rs.getString(3);
				fkConstraints.add(new ForeignKeyConstraint(tableName, columnName, constraintName));
			}

			for (ForeignKeyConstraint fk : fkConstraints) {
				selectStmt = conn.prepareStatement(tuplesQuery);
				selectStmt.setString(1, fk.getConstraintName());
				rs = selectStmt.executeQuery();
				if (rs.next()) {
					fk.setTuples(rs.getLong(1));
				}
			}

			Collections.sort(fkConstraints);

			for (ForeignKeyConstraint fk : fkConstraints) {
				String indexName = "fki_" + fk.getConstraintName();
				if (indexName.lastIndexOf("_fkey") > 0) {
					indexName = indexName.substring(0, indexName.lastIndexOf("_fkey"));
				}
				if (fk.getTuples() >= minTuples && !hasIndex(conn, indexName)) {
					System.out.printf("Creating index %s (tuples: %d)\n", (fk.getTableName() + "." + fk.getColumnName() + " --> " + indexName), fk.getTuples());

					PreparedStatement createStmt = conn.prepareStatement(String.format(createIndex, indexName, fk.getTableName(), fk.getColumnName()));
					createStmt.execute();
				}
			}

			for (Entry<String, Pair<String, String>> entry : indexMap.entrySet()) {
				String indexName = entry.getKey();
				String tableName = entry.getValue().getLeft();
				String columnName = entry.getValue().getRight();

				if (!hasIndex(conn, indexName)) {
					System.out.printf("Creating index %s\n", (tableName + "." + columnName + " --> " + indexName));

					try {
						PreparedStatement createStmt = conn.prepareStatement(String.format(createIndex, indexName, tableName, columnName));
						createStmt.execute();
					}
					catch (SQLException e) {
						System.err.println("Failed to create index " + indexName + ": " + e.getMessage());
					}
				}
			}
		}
	}

	private boolean hasIndex(Connection conn, String name) throws SQLException {
		PreparedStatement selectStmt = conn.prepareStatement(hasIndexQuery);
		selectStmt.setString(1, name);
		ResultSet rs = selectStmt.executeQuery();
		return rs.next();
	}

	private void deleteForeignKeyIndices(Connection conn) throws SQLException {
		List<String> indices = new ArrayList<String>();

		PreparedStatement selectStmt = conn.prepareStatement(indexQuery);
		ResultSet rs = selectStmt.executeQuery();
		while (rs.next()) {
			indices.add(rs.getString(1));
		}

		for (String indexName : indices) {
			PreparedStatement deleteStmt = conn.prepareStatement("DROP INDEX " + indexName);
			System.out.println("Deleting index " + indexName);
			deleteStmt.execute();
		}
	}

	public class ForeignKeyConstraint implements Comparable<ForeignKeyConstraint> {
		private String tableName;
		private String columnName;
		private String constraintName;
		private Long tuples;

		public ForeignKeyConstraint(String tableName, String columnName, String constraintName) {
			super();
			this.tableName = tableName;
			this.columnName = columnName;
			this.constraintName = constraintName;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getConstraintName() {
			return constraintName;
		}

		public void setConstraintName(String constraintName) {
			this.constraintName = constraintName;
		}

		public Long getTuples() {
			return tuples;
		}

		public void setTuples(Long tuples) {
			this.tuples = tuples;
		}

		@Override
		public int compareTo(ForeignKeyConstraint that) {
			int result = 0;
			if (this.tuples != null && that.tuples != null) {
				result = that.tuples.compareTo(this.tuples);
			}
			if (result == 0) {
				result = this.tableName.compareTo(that.tableName);
			}
			if (result == 0) {
				result = this.columnName.compareTo(that.columnName);
			}
			if (result == 0) {
				result = this.constraintName.compareTo(that.constraintName);
			}
			return result;
		}
	}
}
