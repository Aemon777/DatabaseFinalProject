package conversion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.sql.SQLException;
import java.sql.CallableStatement;

import bo.BattingStats;
import bo.CatchingStats;
import bo.FieldingStats;
import bo.PitchingStats;
import bo.Player;
import bo.PlayerSeason;
import bo.Team;
import bo.TeamSeason;
import dataaccesslayer.HibernateUtil;

public class Convert {

	static Connection conn;

	//static final String MYSQL_CONN_URL = "jdbc:mysql://163.11.235.163/mlb?"
	static final String MYSQL_CONN_URL = "jdbc:mysql://163.11.239.96/mlb?"
    + "verifyServerCertificate=false&useSSL=false&"
    + "useLegacyDatetimeCode=false&serverTimezone=America/New_York&"
    + "user=jdbc&password=jdbc";  

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			conn = DriverManager.getConnection(MYSQL_CONN_URL);
			
			convert();
			long endTime = System.currentTimeMillis();
			long elapsed = (endTime - startTime) / (1000*60);
			System.out.println("Elapsed time in mins: " + elapsed);

			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (!conn.isClosed()) conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
    HibernateUtil.stopConnectionProvider();
		HibernateUtil.getSessionFactory().close();
	}

	private static void convert() {
		try {
			HashMap<String,Player> players = getPlayers();
			System.out.println("Players Retrieved.");
			HashMap<String,Team> teams = getTeams();
			System.out.println("Teams Retrieved.");
			addTeamSeasons(teams);
			System.out.println("TeamSeasons Retrieved.");
			addPositions(players);
			System.out.println("Positions Retrieved.");
			addSeasons(players, teams);
			System.out.println("Seasons Retrieved.");
			
			//Debugging persist issues with count
			//int count = 0;
			for (Player p : players.values()) {
				//count ++;
				HibernateUtil.persistPlayer(p);
				//System.out.println("" + count);
			}
			System.out.println("Persisted Players.");
			
			// Persist the team objects.
			for (Team t : teams.values()) {
				HibernateUtil.persistTeam(t);
			}
			System.out.println("Persisted Teams.");
			
			HibernateUtil.flushObjects();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static HashMap<String, Team> getTeams() throws SQLException {
		HashMap<String, Team> teams = new HashMap<String, Team>();
		
		int count=0;
		String query = "{ CALL GetTeams }";
		ResultSet rs;
		CallableStatement stmt = conn.prepareCall(query);
			rs = stmt.executeQuery();
			while (rs.next()) {
				count++;
				// this just gives us some progress feedback
				String tid = rs.getString("teamID");
				String name = rs.getString("name");
				// this check is for data scrubbing
				// don't want to bring any team over that doesn't have a tid and name
				if (tid == null	|| tid.isEmpty() 
								|| name == null 
								|| name.isEmpty())
					continue;
				
				
				Team t = new Team();

				t.setName(rs.getString("name"));
				t.setLeague(rs.getString("lgID"));
				t.setYearFounded(rs.getInt("yearFounded"));
				t.setYearLast(rs.getInt("yearLast"));
				
				//Output checking
				/*
				System.out.println(String.format("%s",
						rs.getString("teamID") + " "
						+ rs.getString("name") + " "
						+ rs.getString("lgID") + " "
						+ rs.getInt("yearFounded") + " "
						+ rs.getInt("yearLast")));
				*/
				
				teams.put(tid, t);
			}

		// Get the teams. 
		// Only capture the most recent team name and league.
		System.out.println("num teams: " + count);
		return teams;
	}
	
	private static void addTeamSeasons(HashMap<String, Team> teams) throws SQLException {
		int count=0;
		String query = "{ CALL GetTeamSeasons }";
		ResultSet rs;
		CallableStatement stmt = conn.prepareCall(query);
			rs = stmt.executeQuery();
			while (rs.next()) {
				count++;
				// this just gives us some progress feedback
				
				String tid = rs.getString("teamID");
				String year = rs.getString("year");
				
				if (tid == null	|| tid.isEmpty() 
								|| year == null
								|| year.isEmpty())
					continue;
				
				TeamSeason ts = new TeamSeason(teams.get(tid), rs.getInt("year"));

				ts.setGamesPlayed(rs.getInt("gamesplayed"));
				ts.setWins(rs.getInt("wins"));
				ts.setLosses(rs.getInt("losses"));
				ts.setRank(rs.getInt("rank"));
				ts.setAttendance(rs.getInt("totalattendance"));
				
				
				if (teams.containsKey(tid)) {
					teams.get(tid).addSeason(ts);
				}

				//Output checking
				/*
				System.out.println(String.format("%s",
						rs.getString("teamID") + " "
						+ rs.getString("year") + " "
						+ rs.getString("gamesplayed") + " "
						+ rs.getInt("wins") + " "
						+ rs.getInt("losses") + " "
						+ rs.getInt("rank") + " "
						+ rs.getInt("totalattendance")
						));
				*/
			}
		System.out.println("num team seasons: " + count);
		// Get team season data.
		
	}
	
	public static HashMap<String, Player> getPlayers() throws SQLException {
		HashMap<String, Player> players = new HashMap<String, Player>();
		PreparedStatement ps = conn.prepareStatement("select " + 
				"playerID, " + 
				"nameFirst, " + 
				"nameLast, " + 
				"nameGiven, "+ 
				"birthDay, " + 
				"birthMonth, " + 
				"birthYear, " + 
				"deathDay, "+ 
				"deathMonth, " + 
				"deathYear, " + 
				"bats, " + 
				"throws, " + 
				"birthCity, " + 
				"birthState, " + 
				"birthCountry, " + 
				"debut, " + 
				"finalGame " +
				"from Master");
		// for debugging comment previous line, uncomment next line
		//"from Master where playerID = 'bondsba01' or playerID = 'youklke01';");
		ResultSet rs = ps.executeQuery();
		int count=0; // for progress feedback only
		while (rs.next()) {
			count++;
			// this just gives us some progress feedback
			//if (count % 1000 == 0)
			//	System.out.println("num players: " + count);

			String pid = rs.getString("playerID");
			String firstName = rs.getString("nameFirst");
			String lastName = rs.getString("nameLast");
			// this check is for data scrubbing
			// don't want to bring anybody over that doesn't have a pid, firstname and lastname
			if (pid == null	|| pid.isEmpty() 
							|| firstName == null 
							|| firstName.isEmpty() 
							|| lastName == null 
							|| lastName.isEmpty()) 
				continue;
			Player p = new Player();
			p.setName(firstName + " " + lastName);
			p.setGivenName(rs.getString("nameGiven"));

			java.util.Date birthDay = convertIntsToDate(rs.getInt("birthYear"), rs.getInt("birthMonth"), rs.getInt("birthDay"));
			if (birthDay!=null) 
				p.setBirthDay(birthDay);
			
			java.util.Date deathDay = convertIntsToDate(rs.getInt("deathYear"), rs.getInt("deathMonth"), rs.getInt("deathDay"));
			if (deathDay!=null)
				p.setDeathDay(deathDay);

			// need to do some data scrubbing for bats and throws columns
			String hand = rs.getString("bats");
			if (hand!=null){
				if (hand.equalsIgnoreCase("B")){
					hand = "S";
				}
				else if (hand.equalsIgnoreCase(""))
					hand = null;
			} 
			p.setBattingHand(hand);

			// Clean up throwing hand
			hand = rs.getString("throws");
			if (hand.equalsIgnoreCase("")){
				hand = null;
			} 
			p.setThrowingHand(hand);

			p.setBirthCity(rs.getString("birthCity"));
			p.setBirthState(rs.getString("birthState"));
			p.setBirthCountry(rs.getString("birthCountry"));

			// Clean up debut and final game data.
			try {
				java.util.Date firstGame = rs.getDate("debut");
				if (firstGame!=null) 
					p.setFirstGame(firstGame);
			}
			catch (SQLException e){
				// Ignore conversion error - remains null;
				//System.out.println(pid + ": debut invalid format");
			}
			try {
				java.util.Date lastGame = rs.getDate("finalGame");
				if (lastGame!=null)
					p.setLastGame(lastGame);
			}
			catch (SQLException e){
				// Ignore conversion error - remains null
				//System.out.println(pid + ": finalGame invalid format");
			}

			players.put(pid, p);
		}
		System.out.println("num players: " + count);
		rs.close();
		ps.close();
		return players;
	}
	
	private static java.util.Date convertIntsToDate(int year, int month, int day) {
		Calendar c = new GregorianCalendar();
		java.util.Date d=null;
		// if year is 0, then date wasn't populated in MySQL database
		if (year!=0) {
			c.set(year, month-1, day);
			d = c.getTime();
		}
		return d;
	}
	
	public static void addPositions(HashMap<String, Player> players) {
		try {
			PreparedStatement ps = conn.prepareStatement("select " +
					"distinct playerID, pos from Fielding");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				String pos = rs.getString("pos");
				if (players.containsKey(pid)) {
					players.get(pid).addPosition(pos);
				}
			}
			rs.close();
			ps.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addSeasons(HashMap<String, Player> players, HashMap<String, Team> teams) {
		try {
			PreparedStatement ps = conn.prepareStatement("select " + 
					"playerID, yearID, teamID, lgId, sum(G) as gamesPlayed " + 
					"from Batting " + 
					"group by playerID, yearID, teamID, lgID;");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				int yid = rs.getInt("yearID");
				String pid = rs.getString("playerID");
				Player p = players.get(pid);
				if (p != null) {
					PlayerSeason s = p.getPlayerSeason(yid);
					// it is possible to see more than one of these per player if he switched teams
					// set all of these attrs the first time we see this playerseason
					if (s == null) {
						s = new PlayerSeason(p,yid);
						p.addSeason(s);
						s.setGamesPlayed(rs.getInt("gamesPlayed"));
					}
					else {
						s.setGamesPlayed(rs.getInt("gamesPlayed") + s.getGamesPlayed());
					}
					
					// Associate player with a team season.
					TeamSeason ts = teams.get(rs.getString("teamID")).getTeamSeason(yid);
					ts.addPlayer(p);
				}
			}
			System.out.println("PlayerSeasons Retrieved.");
			addSalaries(players);
			System.out.println("Salaries Retrieved.");
			addBattingStats(players);		
			System.out.println("BattingStats Retrieved.");
			addFieldingStats(players);
			System.out.println("FieldingStats Retrieved.");
			addPitchingStats(players);
			System.out.println("PitchingStats Retrieved.");
			addCatchingStats(players);
			System.out.println("CatchingStats Retrieved.");
				
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static double addSalaries(HashMap<String, Player> players) {
		double salary = 0;
		try {
			PreparedStatement ps = conn.prepareStatement("select " + 
					"playerID, yearID, sum(salary) as salary " + 
					"from Salaries " +
					"group by playerID, yearID");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				int yid = rs.getInt("yearID");
				salary = rs.getDouble("salary");
				Player p = players.get(pid);
				if (p != null ) {
					PlayerSeason psi = p.getPlayerSeason(yid);
					if (psi != null) {
						psi.setSalary(salary);
					}
				}
			}
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return salary;
	}

	public static void addBattingStats(HashMap<String, Player> players) {
		try {
			PreparedStatement ps = conn.prepareStatement("select "	+
					"playerID, yearID, " +
					"sum(AB) as atBats, " + 
					"sum(H) as hits, " + 
					"sum(2B) as doubles, " + 
					"sum(3B) as triples, " + 
					"sum(HR) as homeRuns, " + 
					"sum(RBI) as runsBattedIn, " + 
					"sum(SO) as strikeouts, " + 
					"sum(BB) as walks, " + 
					"sum(HBP) as hitByPitch, " + 
					"sum(IBB) as intentionalWalks, " + 
					"sum(SB) as steals, " + 
					"sum(CS) as stealsAttempted " + 
					"from Batting " + 
					"group by playerID, yearID");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				int yid = rs.getInt("yearID");
				Player p = players.get(pid);
				if (p != null) {
					PlayerSeason psi = p.getPlayerSeason(yid);
					if (psi != null) {
						BattingStats s = new BattingStats();
						s.setId(psi);
						s.setAtBats(rs.getInt("atBats"));
						s.setHits(rs.getInt("hits"));
						s.setDoubles(rs.getInt("doubles"));
						s.setTriples(rs.getInt("triples"));
						s.setHomeRuns(rs.getInt("homeRuns"));
						s.setRunsBattedIn(rs.getInt("runsBattedIn"));
						s.setStrikeouts(rs.getInt("strikeouts"));
						s.setWalks(rs.getInt("walks"));
						s.setHitByPitch(rs.getInt("hitByPitch"));
						s.setIntentionalWalks(rs.getInt("intentionalWalks"));
						s.setSteals(rs.getInt("steals"));
						s.setStealsAttempted(rs.getInt("stealsAttempted"));
						psi.setBattingStats(s);
					}
				}	
			}
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void addFieldingStats(HashMap<String, Player> players) {
		try {
			PreparedStatement ps = conn.prepareStatement("select " +
					"playerID, yearID, " +
					"sum(E) as errors, " +
					"sum(PO) as putOuts " +
					"from Fielding " +
					"group by playerID, yearID");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				int yid = rs.getInt("yearID");
				Player p = players.get(pid);
				if (p != null) {
					PlayerSeason psi = p.getPlayerSeason(yid);
					if (psi != null) {
						FieldingStats s = new FieldingStats();
						s.setId(psi);
						s.setErrors(rs.getInt("errors"));
						s.setPutOuts(rs.getInt("putOuts"));
						psi.setFieldingStats(s);
					}
				}
			}
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void addPitchingStats(HashMap<String, Player> players) {
		try {
			PreparedStatement ps = conn.prepareStatement("select " +
					"playerID, yearID, " +
					"sum(IPOuts) as outsPitched, " + 
					"sum(ER) as earnedRunsAllowed, " +
					"sum(HR) as homeRunsAllowed, " + 
					"sum(SO) as strikeouts, " +
					"sum(BB) as walks, " + 
					"sum(W) as wins, " +
					"sum(L) as losses, " + 
					"sum(WP) as wildPitches, " +
					"sum(BFP) as battersFaced, " + 
					"sum(HBP) as hitBatters, " +
					"sum(SV) as saves " + 
					"from Pitching " +
					"group by playerID, yearID");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				int yid = rs.getInt("yearID");
				Player p = players.get(pid);
				if (p != null) {
					PlayerSeason psi = p.getPlayerSeason(yid);
					if (psi != null) {
						PitchingStats s = new PitchingStats();
						s.setId(psi);
						s.setOutsPitched(rs.getInt("outsPitched"));
						s.setEarnedRunsAllowed(rs.getInt("earnedRunsAllowed"));
						s.setHomeRunsAllowed(rs.getInt("homeRunsAllowed"));
						s.setStrikeouts(rs.getInt("strikeouts"));
						s.setWalks(rs.getInt("walks"));
						s.setWins(rs.getInt("wins"));
						s.setLosses(rs.getInt("losses"));
						s.setWildPitches(rs.getInt("wildPitches"));
						s.setBattersFaced(rs.getInt("battersFaced"));
						s.setHitBatters(rs.getInt("hitBatters"));
						s.setSaves(rs.getInt("saves"));
						psi.setPitchingStats(s);
					}
				}
			}
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void addCatchingStats(HashMap<String, Player> players) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("select " +
					"playerID, yearID, " +
					"sum(PB) as passedBalls, " +
					"sum(WP) as wildPitches, " +
					"sum(SB) as stealsAllowed, " +
					"sum(CS) as stealsCaught " +
					"from Fielding " +
					"group by playerID, yearID");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String pid = rs.getString("playerID");
				int yid = rs.getInt("yearID");
				Player p = players.get(pid);
				if (p != null) {
					PlayerSeason psi = p.getPlayerSeason(yid);
					if (psi != null) {
						CatchingStats s = new CatchingStats();
						s.setId(psi);
						s.setPassedBalls(rs.getInt("passedBalls"));
						s.setWildPitches(rs.getInt("wildPitches"));
						s.setStealsAllowed(rs.getInt("stealsAllowed"));
						s.setStealsCaught(rs.getInt("stealsCaught"));
						psi.setCatchingStats(s);
					}
				}
			}
			rs.close();
			ps.close();
		} catch (Exception e) {
			ps.toString();
			e.printStackTrace();
		}
	}
}