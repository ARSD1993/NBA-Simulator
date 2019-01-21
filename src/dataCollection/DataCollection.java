package dataCollection;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import database.DatabaseQuery;


public class DataCollection {
	
	private static String HOST_URL = "http://www.espn.com";
	
	private static String TEAMS_URL = "/nba/players";
	
	private static String SCHEDULE_URL = "/nba/schedule";
	
	private static String PREVIOUS_DAY_RESULT_URL = "/nba/schedule/_/date/";
	
	private static final List<String> HEADER_LIST = createList();
	
	private static final Map<String, String> TEAM_MAP = createTeamMap();
	
	public static void main(String[] args) throws IOException {
	    ArrayList<String> teamLinks = getTeamLinks();
	    Map<String, Set<String>> teamPlayerMap = getPlayersPerTeam(teamLinks);
	    insertPlayerDataToDB(teamPlayerMap);
	    getSchedule();
	}

	private static void getSchedule() {
		URL sceduleUrl;
		URL resultUrl;
		try {
			DatabaseQuery dbQuery = new DatabaseQuery("ahrshia", "password");
			if (dbQuery.shouldUpdateSchedule()) {
				sceduleUrl = new URL(HOST_URL + SCHEDULE_URL);
			    Document teamDoc = Jsoup.parse(sceduleUrl, 5000);
				Element tableDate = teamDoc.select("h2").first();
				Elements teamsPlaying = teamDoc.select("div.responsive-table-wrap").first().select("abbr[title]");
				String dateString = tableDate.text().replaceAll(".*,", String.valueOf(Calendar.getInstance().get(Calendar.YEAR)));
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy MMMM d");
				LocalDate gameDate = LocalDate.parse(dateString, formatter);
				for (int i = 0; i < teamsPlaying.size(); i = i + 2) {
					String teamOne = teamsPlaying.get(i).attr("title");
					String teamTwo = teamsPlaying.get(i + 1).attr("title");
					dbQuery.insertScheduleRecord(teamOne, teamTwo, gameDate, "TBD");
				}
			}
			if (dbQuery.shouldUpdateResult()) {
				// we should go to the previous days results and see how we did;
				LocalDate currentDate = LocalDate.now().minus(1, ChronoUnit.DAYS);
				String year = Integer.toString(currentDate.getYear());
				String month = Integer.toString(currentDate.getMonthValue());
				String day = Integer.toString(currentDate.getDayOfMonth());
				if (month.length() == 1) {
					month = "0" + month;
				}
				if (day.length() == 1) {
					day = "0" + day;
				}
				resultUrl = new URL(HOST_URL + PREVIOUS_DAY_RESULT_URL + year + month + day);
				Document teamDoc = Jsoup.parse(resultUrl, 5000);
				Element tableDate = teamDoc.select("h2").first();
				String dateString = tableDate.text().replaceAll(".*,", String.valueOf(Calendar.getInstance().get(Calendar.YEAR)));
				Elements teamsWinning = teamDoc.select("div.responsive-table-wrap").first().select("a[href*=gameId]");
				Elements teamsPlaying = teamDoc.select("div.responsive-table-wrap").first().select("abbr[title]");
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy MMMM d");
				LocalDate gameDate = LocalDate.parse(dateString, formatter);
				ArrayList<String> winningTeams = new ArrayList<String>();
				for (int i = 0; i < teamsWinning.size(); i++) {
					winningTeams.add(teamsWinning.get(i).text().substring(0, 3));
				}
				int index = 0;
				for (int i = 0; i < teamsPlaying.size(); i = i + 2) {
					String teamOne = teamsPlaying.get(i).attr("title");
					String teamTwo = teamsPlaying.get(i + 1).attr("title");
					dbQuery.insertScheduleRecord(teamOne, teamTwo, gameDate, TEAM_MAP.get(winningTeams.get(index).replace(" ", "")));
					index++;
				}
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void insertPlayerDataToDB(Map<String, Set<String>> teamPlayerMap) {
		// first check if table should be updated
	    DatabaseQuery dbQuery = new DatabaseQuery("ahrshia", "password");
		if (dbQuery.shouldUpdateData()) {
			for (Entry<String, Set<String>> s : teamPlayerMap.entrySet()) {
				// check if table exist. If not create a new table for the team;
				if (!dbQuery.checkTable(s.getKey())) {
					//create table
					dbQuery.createNBATable(s.getKey());
					System.out.println("Table Created for team " + s.getKey());
				}
				else {
					// if the tables need to be updated and the tables exist then we need to truncate the table before inserting
					dbQuery.truncateTeamTable(s.getKey());
				}
				// now get data for player
				for (String playLink: s.getValue()) {
					try {
						URL getPlayerUrl = new URL(playLink);
						Document teamDoc = Jsoup.parse(getPlayerUrl, 5000);
						Map<String, String> record = new HashMap<String, String>();
						record.clear();
						Elements playerElements = teamDoc.select("#content > div.bg-opaque > div.span-6.last > div.mod-container.mod-table.mod-no-footer:nth-child(2)");
						Elements playerNameElement = teamDoc.select("h1");
						for (Element playPage: playerElements) {
							if (!record.isEmpty()) {
								break;
							}
							record.put("PLAYER_NAME", playerNameElement.get(0).text());
							Elements statNums = playPage.select("td");
							int header = 0;
							for (int i = 0; i < statNums.size(); i++) {
								String text = statNums.get(i).text();
								if (text.matches("(?i).*Regular\\s*Season|.*Career.*")) {
									continue;
								}
								if (text.matches("(?i).*Postseason.*")) {
									// some players have post season data for some reason?
									break;
								}
								if (text.matches("[A-Z]+.*")) {
									break;
								}
								header = putIntoRecord(record, text, header);
							}
						}
						if (!record.isEmpty()) {
						storeRecord(record, dbQuery, s.getKey());
						}
				    }
					catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}
			}
			
		}
		
	}

	private static void storeRecord(Map<String, String> record, DatabaseQuery dbQuery, String teamName) {
		Set<String> keySet = record.keySet();
		Collection<String> value = record.values();
		
		dbQuery.insert(keySet, value, teamName);
		
		// TODO Auto-generated method stub
		
	}

	private static int putIntoRecord(Map<String, String> record, String text, int header) {
		int currentHeader = header;
		if (text.contains("-")) {
			String[] twoText = text.split("-");
			currentHeader = addtoRecord(currentHeader, twoText[0], record);
			currentHeader = addtoRecord(currentHeader, twoText[1], record);
		}
		else {
			currentHeader = addtoRecord(header, text, record);
		}
		return currentHeader;
	}
	
	private static int addtoRecord (int headerIndex, String stat, Map<String, String> record) {
		int retHeaderIndex = headerIndex;
		record.put(HEADER_LIST.get(retHeaderIndex), stat);
		return ++retHeaderIndex;
	}

	private static Map<String, Set<String>> getPlayersPerTeam(ArrayList<String> teamLinks) {
		// TODO Auto-generated method stub
		Map<String, Set<String>> playerTeamMap = new HashMap<String, Set<String>>();
		for (String link : teamLinks) {
			try {
				ArrayList<String> playList = new ArrayList<String>();
				Set<String> playSet = new HashSet<String>();
				link = link.replace("/_/", "/roster/_/");
				URL getTeamsUrl = new URL(link);
				URLConnection teamCon = getTeamsUrl.openConnection();
				String teamEncoding = teamCon.getContentEncoding();
				teamEncoding = teamEncoding == null ? "UTF-8" : teamEncoding;
				Document teamDoc = Jsoup.parse(getTeamsUrl, 5000);
				Elements teamElements = teamDoc.select("a[href*=/player/_]");
				String team = getTeam(link);
				for (Element currentTeamlink : teamElements) {
					String teamLink = currentTeamlink.getElementsByAttribute("href").attr("href");
					playList.add(teamLink);
					playSet.add(teamLink);
				}
				playerTeamMap.put(team, playSet);
			} 
			catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		return playerTeamMap;
	}

	private static String getTeam(String link) {
		StringBuilder builder = new StringBuilder(link);
		builder.reverse();
		int index = builder.indexOf("/");
		return link.substring(link.length() - index, link.length());
	}

	private static ArrayList<String> getTeamLinks() {
		URL getTeamsUrl;
		ArrayList<String> teamLinks = new ArrayList<String>();
		try {
			getTeamsUrl = new URL(HOST_URL + TEAMS_URL);
			URLConnection teamCon = getTeamsUrl.openConnection();
			String teamEncoding = teamCon.getContentEncoding();
			teamEncoding = teamEncoding == null ? "UTF-8" : teamEncoding;
			Document teamDoc = Jsoup.parse(getTeamsUrl, 5000);
			Elements teamElements = teamDoc.select("a[href*=/team/_]");
			for (Element currentTeamlink : teamElements) {
				String teamLink = currentTeamlink.getElementsByAttribute("href").attr("href");
				teamLinks.add(teamLink);
			}	
		} 
		catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return teamLinks;
	}
	
	private static List<String> createList() {
		List<String> headers = new ArrayList<String>();
		headers.add("GAMES_PLAYED");
		headers.add("MINUTES_PLAYED");
		headers.add("FIELD_GOALS_MADE");
		headers.add("FIELD_GOALS_ATTEMPTED");
		headers.add("FIELD_GOAL_PERCENT");
		headers.add("THREE_POINTERS_MADE");
		headers.add("THREE_POINTERS_ATTEMPTED");
		headers.add("THREE_POINTER_PERCENT");
		headers.add("FREE_THROWS_MADE");
		headers.add("FREE_THROWS_ATTEMPTED");
		headers.add("FREE_THROWS_PERCENT");
		headers.add("REBOUNDS_PER_GAME");
		headers.add("ASSISTS_PER_GAME");
		headers.add("BLOCKS_PER_GAME");
		headers.add("STEALS_PER_GAME");
		headers.add("PERSONAL_FOULS_PER_GAME");
		headers.add("TURNOVERS_PER_GAME");
		headers.add("POINTS_PER_GAME");
		headers.add("CAREER_GAMES_PLAYED");
		headers.add("CAREER_MINUTES_PLAYED");
		headers.add("CAREER_FIELD_GOALS_ATTEMPTED");
		headers.add("CAREER_FIELD_GOALS_MADE");
		headers.add("CAREER_FIELD_GOAL_PERCENT");
		headers.add("CAREER_THREE_POINTERS_ATTEMPTED");
		headers.add("CAREER_THREE_POINTERS_MADE");
		headers.add("CAREER_THREE_POINTER_PERCENT");
		headers.add("CAREER_FREE_THROWS_ATTEMPTED");
		headers.add("CAREER_FREE_THROWS_MADE");
		headers.add("CAREER_FREE_THROWS_PERCENT");
		headers.add("CAREER_REBOUNDS_PER_GAME");
		headers.add("CAREER_ASSISTS_PER_GAME");
		headers.add("CAREER_BLOCKS_PER_GAME");
		headers.add("CAREER_STEALS_PER_GAME");
		headers.add("CAREER_PERSONAL_FOULS_PER_GAME");
		headers.add("CAREER_TURNOVERS_PER_GAME");
		headers.add("CAREER_POINTS_PER_GAME");
		return headers;
	}
	
	private static Map<String, String> createTeamMap() {
	Map<String, String> teamMap = new HashMap<String, String>();
		teamMap.put("DAL", "Dallas Mavericks");
		teamMap.put("CHA", "Charlotte Hornets");
		teamMap.put("MIA", "Miami Heat");
		teamMap.put("CLE", "Cleveland Cavaliers");
		teamMap.put("WSH", "Washington Wizards");
		teamMap.put("ATL", "Atlanta Hawks");
		teamMap.put("BKN", "Brooklyn Nets");
		teamMap.put("NO", "New Orleans Pelicans");
		teamMap.put("BOS", "Boston Celtics");
		teamMap.put("MIN", "Minnesota Timberwolves");
		teamMap.put("ORL", "Orlando Magic");
		teamMap.put("CHI", "Chicago Bulls");
		teamMap.put("DET", "Detroit Pistons");
		teamMap.put("MEM", "Memphis Grizzlies");
		teamMap.put("PHI", "Philadelphia 76ers");
		teamMap.put("PHX", "Phoenix Suns");
		teamMap.put("OKC", "Oklahoma City Thunder");
		teamMap.put("LAL", "Los Angeles Lakers");
		teamMap.put("SA", "San Antonio Spurs");
		teamMap.put("TOR", "Toronto Raptors");
		teamMap.put("DEN", "Denver Nuggets");
		teamMap.put("SAC", "Sacramento Kings");
		teamMap.put("HOU", "Houston Rockets");
		teamMap.put("GS", "Golden State Warriors");
		teamMap.put("UTAH", "Utah Jazz");
		teamMap.put("MIL", "Milwaukee Bucks");
		teamMap.put("NY", "New York Knicks");
		teamMap.put("POR", "Portland Trail Blazers");
		teamMap.put("LAC", "LA Clippers");
		teamMap.put("IND", "Indiana Pacers");
		return teamMap;
	}

}
