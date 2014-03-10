package com.levelsbeyond.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.BooleanUtils;
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

	public static void main(String[] args) throws SQLException {
		String host = "localhost";
		String database = "ABC-Workflow";
		String user = "abcworkflow";
		String password = "FordBo$$302";
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

		if (showHelp || (dropIndices == null && createIndices == null)) {
			System.out.println("Usage: java -jar AutoFKIndexDB.jar\nOptions:\n" +
					"\t--host=[" + host + "]\n" +
					"\t--database=[" + database + "]\n" +
					"\t--user=[" + user + "]\n" +
					"\t--password=[" + password + "]\n" +
					"\t--min_tuples=[" + minTuples + "]\n" +
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
					System.out.printf("Tuples %10d: %s\n", fk.getTuples(), (fk.getTableName() + "." + fk.getColumnName() + " --> " + indexName));

					PreparedStatement createStmt = conn.prepareStatement(String.format(createIndex, indexName, fk.getTableName(), fk.getColumnName()));
					createStmt.execute();
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
