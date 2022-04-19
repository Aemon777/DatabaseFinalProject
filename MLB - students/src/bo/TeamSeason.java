package bo;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@SuppressWarnings("serial")
@Entity(name = "teamseason")
public class TeamSeason implements Serializable {
    @EmbeddedId
	TeamSeasonId id;

	@Embeddable
	static class TeamSeasonId implements Serializable {
		@ManyToOne
		@JoinColumn(name = "teamId", referencedColumnName = "teamId", insertable = false, updatable = false)
		Team team;
		@Column(name="year")
		Integer teamYear;
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof TeamSeasonId)){
				return false;
			}
			TeamSeasonId other = (TeamSeasonId)obj;
			// in order for two different object of this type to be equal,
			// they must be for the same year and for the same team
			return (this.team==other.team &&
					this.teamYear==other.teamYear);
		}
		 
		@Override
		public int hashCode() {
			Integer hash = 0;
			if (this.team != null) hash += this.team.hashCode();
			if (this.teamYear != null) hash += this.teamYear.hashCode();
			return hash;
		}
	}

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(name = "teamseasonplayer", 
    joinColumns={
        @JoinColumn(name="teamId", insertable = false, updatable = false), 
        @JoinColumn(name="year",  insertable = false, updatable = false)}, 
    inverseJoinColumns={
        @JoinColumn(name="playerId", insertable = false, updatable = false)})
    Set<Player> players = new HashSet<Player>();

    public TeamSeason() {}

    public TeamSeason(Team t, Integer year) {
        TeamSeasonId tsi = new TeamSeasonId();
        tsi.team = t;
        tsi.teamYear = year;
        this.id = tsi;
    }

    //derived stats
    public Integer getTies() {
        Integer ties = 0;
        if (this.getLosses() != null && 
            this.getWins() != null &&
            this.getGamesPlayed() != null) {
            ties = this.getGamesPlayed() - this.getWins() - this.getLosses(); 
        }
        return ties;
    }

    public void setPlayers(Set<Player> players) {
        this.players = players;
    }

    public Set<Player> getPlayers() {
        return this.players;
    }

    public void addPlayer(Player player) {
        this.players.add(player);
    }

    @Column
    Integer gamesPlayed;
    @Column
    Integer wins;
    @Column
    Integer losses;
    @Column
    Integer rank;
    @Column
    Integer attendance;

    public void setTeam(Team team) {
        this.id.team = team;
    }

    public Team getTeam() {
        return this.id.team;
    }

    public void setYear(Integer year) {
        this.id.teamYear = year;
    }

    public Integer getYear() {
        return this.id.teamYear;
    }

    public void setGamesPlayed(Integer gamesPlayed) {
        this.gamesPlayed = gamesPlayed;
    }

    public Integer getGamesPlayed() {
        return gamesPlayed;
    }

    public void setWins(Integer wins) {
        this.wins = wins;
    }

    public Integer getWins() {
        return wins;
    }

    public void setLosses(Integer losses) {
        this.losses = losses;
    }

    public Integer getLosses() {
        return losses;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public Integer getRank() {
        return rank;
    }

    public void setAttendance(Integer attendance) {
        this.attendance = attendance;
    }

    public Integer getAttendance() {
        return attendance;
    }

}