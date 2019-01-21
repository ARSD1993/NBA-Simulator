package database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public class DatabaseQuery {
	
	private final static String CREATE_TEAM_TABLE = "CREATE TABLE %s (PLAYER_NAME VARCHAR(64) NOT NULL," +
		"GAMES_PLAYED INT, MINUTES_PLAYED INT, FIELD_GOALS_ATTEMPTED DECIMAL(10,5)," +
		"FIELD_GOALS_MADE DECIMAL(10,5), FIELD_GOAL_PERCENT DECIMAL(6,5), THREE_POINTERS_ATTEMPTED DECIMAL(10,5)," +
		"THREE_POINTERS_MADE DECIMAL(10,5), THREE_POINTER_PERCENT DECIMAL(6,5), FREE_THROWS_ATTEMPTED DECIMAL(10,5)," +
		"FREE_THROWS_MADE DECIMAL(10,5), FREE_THROWS_PERCENT DECIMAL(6,5), REBOUNDS_PER_GAME DECIMAL(10,5)," +
		"ASSISTS_PER_GAME DECIMAL(10,5), BLOCKS_PER_GAME DECIMAL(10,5), STEALS_PER_GAME DECIMAL(10,5), PERSONAL_FOULS_PER_GAME DECIMAL(10,5)," +
		"TURNOVERS_PER_GAME DECIMAL(10,5), POINTS_PER_GAME DECIMAL(10,5), CAREER_GAMES_PLAYED INT, CAREER_MINUTES_PLAYED INT," +
		"CAREER_FIELD_GOALS_ATTEMPTED DECIMAL(10,5), CAREER_FIELD_GOALS_MADE DECIMAL(10,5), CAREER_FIELD_GOAL_PERCENT DECIMAL(6,5)," +
		"CAREER_THREE_POINTERS_ATTEMPTED DECIMAL(10,5), CAREER_THREE_POINTERS_MADE DECIMAL(10,5), CAREER_THREE_POINTER_PERCENT DECIMAL(6,5)," +
		"CAREER_FREE_THROWS_ATTEMPTED DECIMAL(10,5), CAREER_FREE_THROWS_MADE DECIMAL(10,5), CAREER_FREE_THROWS_PERCENT DECIMAL(6,5)," +
		"CAREER_REBOUNDS_PER_GAME DECIMAL(10,5), CAREER_ASSISTS_PER_GAME DECIMAL(10,5), CAREER_BLOCKS_PER_GAME DECIMAL(10,5)," +
		"CAREER_STEALS_PER_GAME DECIMAL(10,5), CAREER_PERSONAL_FOULS_PER_GAME DECIMAL(10,5), CAREER_TURNOVERS_PER_GAME DECIMAL(10,5)," +
		"CAREER_POINTS_PER_GAME DECIMAL(10,5), PRIMARY KEY (PLAYER_NAME))";
	
	private final static String CREATE_NBA_SCHEDULE_TABLE = "CREATE TABLE NBA_SCHEDULE (GAME_DATE DATE NOT NULL, TEAM_ONE VARCHAR(64) NOT NULL, TEAM_TWO VARCHAR(64) NOT NULL," +
			"HOME_TEAM  VARCHAR(64), MY_WINNER VARCHAR(64), ACTUAL_WINNER VARCHAR(64), TEAM_ONE_SCORE DECIMAL(10,5), TEAM_TWO_SCORE DECIMAL(10,5), PRIMARY KEY (GAME_DATE, " +
			"TEAM_ONE, TEAM_TWO))";
	
	private final static String INSERT_NBA_SCHEDULE = "INSERT INTO NBA_SCHEDULE (GAME_DATE, TEAM_ONE, TEAM_TWO, HOME_TEAM, MY_WINNER, ACTUAL_WINNER, TEAM_ONE_SCORE, TEAM_TWO_SCORE)" +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE TEAM_ONE_SCORE = ?, TEAM_TWO_SCORE = ?, ACTUAL_WINNER = ?";
	
	private final static String SELECT_PLAYER_PERFORMANCE = "select PLAYER_NAME, (POINTS_PER_GAME + ASSISTS_PER_GAME * 2 + REBOUNDS_PER_GAME + STEALS_PER_GAME * 2 + BLOCKS_PER_GAME * 2)" +
			" / MINUTES_PLAYED as PLAYER_PERFORMANCE from %s where MINUTES_PLAYED >= 20";
	
	private final static String INSERT_PLAYER_STATEMENT = "INSERT INTO %s (%s) values (";
	
	private final static String CREATE_LAST_UPDATED_TABLE = "CREATE TABLE LAST_UPDATED (LAST_UPDATED_TEAM DATE, LAST_UPDATED_SCHEDULE DATE, LAST_UPDATED_RESULT DATE)";
	
	private final static String INSERT_LAST_UPDATED = "INSERT INTO LAST_UPDATED (LAST_UPDATED_TEAM, LAST_UPDATED_SCHEDULE, LAST_UPDATED_RESULT) values (?, ?, ?)";
	
	private final static String TRUNCATE_LAST_UPDATED = "TRUNCATE TABLE LAST_UPDATED";
	
	private final static String TRUNCATE_TEAM_TABLE = "TRUNCATE TABLE %s";
	
    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
	
	public DatabaseQuery (String user, String password) {
		//login to the data
		login(user, password);
	}

	private void login(String user, String password) {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/NBA_SIM" +"?user="
                            + user + "&password=" + password);
		} 
		catch (ClassNotFoundException e) {
			System.out.println("Could not find JDBC driver");
			e.printStackTrace();
		} 
		catch (SQLException e) {
			System.out.println("Could not connect to specified database with provided credentials");
			e.printStackTrace();
		}
	}
	
	public boolean checkTable(String teamName) {
		String tableName = tableName(teamName);
		boolean ret = false;
		try {
			DatabaseMetaData dbm = connect.getMetaData();
			resultSet = dbm.getTables(null, null, tableName, null);
			if (resultSet.next()) {
				ret = true;
			}
		} 
		catch (SQLException e) {
			System.out.println("Error checking if team table exists");
			e.printStackTrace();
		}
		return ret;
	}
	
	public boolean shouldUpdateData() {
		boolean ret = false;
		try {
			DatabaseMetaData dbm = connect.getMetaData();
			resultSet = dbm.getTables(null, null, "LAST_UPDATED", null);
			if (resultSet.next()) {
				// check table if last date was a week ago
				statement = connect.createStatement();
				resultSet = statement.executeQuery("select * from LAST_UPDATED");
				resultSet.next();
				LocalDate dbDate = resultSet.getObject("LAST_UPDATED_TEAM" , LocalDate.class);
				LocalDate dbInstantWeekAfter = dbDate.plus(6, ChronoUnit.DAYS);
				LocalDate currentInstant = LocalDate.now();
				if (dbInstantWeekAfter.isBefore(currentInstant)) {
					ret = true;
					updateMetadataRecords();
				}
			}
			else {
				createTable(CREATE_LAST_UPDATED_TABLE, "LAST_UPDATED");
				insertMetadataRecords(LocalDate.now(), LocalDate.now(), LocalDate.now());
				ret = true;
			}
		}
		catch (SQLException e) {
			System.out.println("Error checking if metadata table exists");
			e.printStackTrace();
		}
		return ret;
	}
	
	private void truncateMetadataTable() {
		try {
			statement = connect.createStatement();
			statement.executeUpdate(TRUNCATE_LAST_UPDATED);
			statement.close();
		} catch (SQLException e) {
			System.out.println("Error creating LAST_UPDATED table");
			e.printStackTrace();
		}
	}

	private void insertMetadataRecords(LocalDate teamDataDate, LocalDate scheduleDataDate, LocalDate resultDataDate) {
		try {
			preparedStatement = connect.prepareStatement(INSERT_LAST_UPDATED);
			preparedStatement.setObject(1, teamDataDate);
			preparedStatement.setObject(2, scheduleDataDate);
			preparedStatement.setObject(3, resultDataDate);
			preparedStatement.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void updateMetadataRecords () {
		try {
			statement = connect.createStatement();
			resultSet = statement.executeQuery("select * from LAST_UPDATED");
			resultSet.next();
			LocalDate scheduleDate = resultSet.getObject("LAST_UPDATED_SCHEDULE" , LocalDate.class);
			LocalDate resultDate = resultSet.getObject("LAST_UPDATED_RESULT" , LocalDate.class);
			LocalDate currentDate = LocalDate.now();
			truncateMetadataTable();
			insertMetadataRecords(currentDate, scheduleDate, resultDate);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void createTable(String sqlCreateTable, String tableName) {
		try {
			statement = connect.createStatement();
			statement.executeUpdate(sqlCreateTable);
			statement.close();
		} catch (SQLException e) {
			System.out.println("Error creating table " + tableName);
			e.printStackTrace();
		}
		
	}

	public void createNBATable(String teamName) {
		String tableName = tableName(teamName);
		try {
			statement = connect.createStatement();
			statement.executeUpdate(String.format(CREATE_TEAM_TABLE, tableName));
			statement.close();
		} catch (SQLException e) {
			System.out.println("Error creating new table for " + teamName);
			e.printStackTrace();
		}
	}

	public void insert(Set<String> keySet, Collection<String> values, String teamName) {
		String databaseColumn = getDatabaseColumnString(keySet);
		String tableName = tableName(teamName);
		String sqlQuery = String.format(setInsert(INSERT_PLAYER_STATEMENT, keySet), tableName, databaseColumn);
		try {
			preparedStatement = connect.prepareStatement(sqlQuery);
			Object[] arr = values.toArray();
			for (int i = 0; i < arr.length; i++) {
				String value = arr[i].toString();
				if (value.matches("[A-Z]+.*")) {
					preparedStatement.setString(i + 1, value);
				}
				else {
					preparedStatement.setDouble(i + 1, Double.parseDouble(value));
				}
			}
			preparedStatement.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private String tableName (String teamName) {
		return teamName.replace("-", "_").toUpperCase();
	}

	private String setInsert(String insertStatement, Set<String> keySet) {
		StringBuilder insertBuilder = new StringBuilder(insertStatement);
		for (String column: keySet) {
			insertBuilder.append("?,");
		}
		insertBuilder.deleteCharAt(insertBuilder.length() - 1);
		insertBuilder.append(")");
		return insertBuilder.toString();
	}

	private String getDatabaseColumnString(Set<String> keySet) {
		StringBuilder databaseColumn = new StringBuilder("");
		for (String column: keySet) {
			databaseColumn.append(" " + column +",");
		}
		databaseColumn.deleteCharAt(databaseColumn.length() - 1);
		return databaseColumn.toString();
	}

	public void truncateTeamTable(String team) {
		try {
			statement = connect.createStatement();
			statement.executeUpdate(String.format(TRUNCATE_TEAM_TABLE, tableName(team)));
			statement.close();
		} catch (SQLException e) {
			System.out.println("Error creating LAST_UPDATED table");
			e.printStackTrace();
		}
	}

	public boolean shouldUpdateSchedule() {
		boolean ret = true;
		try {
			checkScheduleTable();
			LocalDate currentDate = LocalDate.now();
			statement = connect.createStatement();
		    resultSet = statement.executeQuery("select * from NBA_SCHEDULE where GAME_DATE = '" + currentDate + "'");
		    if (resultSet.next()) {
		    	ret = false;
		    }
		    else {
		    	// need to update table date for schedule
		    	statement = connect.createStatement();
			    resultSet = statement.executeQuery("select * from LAST_UPDATED");
				resultSet.next();
				LocalDate lastResultDate = resultSet.getObject("LAST_UPDATED_RESULT" , LocalDate.class);
				LocalDate lastTeamDate = resultSet.getObject("LAST_UPDATED_TEAM" , LocalDate.class);
				truncateMetadataTable();
				insertMetadataRecords(lastTeamDate, LocalDate.now(), lastResultDate);
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	private void checkScheduleTable() {
		try {
			DatabaseMetaData dbm = connect.getMetaData();
			resultSet = dbm.getTables(null, null, "NBA_SCHEDULE", null);
			if (!resultSet.next()) {
				createTable(CREATE_NBA_SCHEDULE_TABLE, "NBA_SCHEDULE");
			}
		}
		catch (SQLException e) {
			System.out.println("Error checking if metadata table exists");
			e.printStackTrace();
		}
	}

	public boolean shouldUpdateResult() {
		boolean ret = false;
		try {
			statement = connect.createStatement();
		    resultSet = statement.executeQuery("select * from LAST_UPDATED");
			resultSet.next();
			LocalDate lastResultDate = resultSet.getObject("LAST_UPDATED_RESULT" , LocalDate.class);
			LocalDate currentDate = LocalDate.now();
			if (lastResultDate.isBefore(currentDate)) {
				ret = true;
				LocalDate lastScheduleDate = resultSet.getObject("LAST_UPDATED_SCHEDULE" , LocalDate.class);
				LocalDate lastTeamDate = resultSet.getObject("LAST_UPDATED_TEAM" , LocalDate.class);
				truncateMetadataTable();
				insertMetadataRecords(lastTeamDate, lastScheduleDate, LocalDate.now());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	public void insertScheduleRecord(String teamOne, String teamTwo, LocalDate gameDate, String actualWinner) {
		try {
			double teamOneScore = getScore(teamOne.replace(" ", "_"), true);
			double teamTwoScore = getScore(teamTwo.replace(" ", "_"), false);
			String teamWinner;
			if (teamOneScore > teamTwoScore) {
				teamWinner = teamOne;
			}
			else {
				teamWinner = teamTwo;
			}
			preparedStatement = connect.prepareStatement(INSERT_NBA_SCHEDULE);
			preparedStatement.setObject(1, gameDate);
			preparedStatement.setString(2, teamOne);
			preparedStatement.setString(3, teamTwo);
			preparedStatement.setString(4, teamOne);
			preparedStatement.setString(5, teamWinner);
			preparedStatement.setString(6, actualWinner);
			preparedStatement.setDouble(7, teamOneScore);
			preparedStatement.setDouble(8, teamTwoScore);
			preparedStatement.setDouble(9, teamOneScore);
			preparedStatement.setDouble(10, teamTwoScore);
			preparedStatement.setString(11, actualWinner);
			preparedStatement.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private double getScore(String teamName, boolean isHome) {
		// TODO Auto-generated method stub
		double value = 0;
		
		try {
			statement = connect.createStatement();
	        resultSet = statement.executeQuery(String.format(SELECT_PLAYER_PERFORMANCE, teamName));
		    while (resultSet.next()) {
		    	value += resultSet.getDouble("PLAYER_PERFORMANCE");
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (isHome) {
			value = value * 1.1;
		}
		
		return value;
	}
}
