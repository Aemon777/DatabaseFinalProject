package bo;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Entity(name = "team")
public class Team {
    //Assuming team id can be dropped in favor of a more standard unique id
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Integer teamId;

    @OneToMany(fetch = FetchType.EAGER, cascade=CascadeType.ALL, mappedBy="id.team")
	@Fetch(FetchMode.JOIN)
    Set<TeamSeason> seasons = new HashSet<TeamSeason>();

    @Column
    String name;
    @Column
    String league;
    @Column
    Integer yearFounded;
    @Column
    Integer yearLast;

    public TeamSeason getTeamSeason(int year) {
		for (TeamSeason ts: seasons) {
			if (ts.getYear().equals(year)) return ts;
		}
		return null;
	}

    public void setSeasons(Set<TeamSeason> seasons) {
        this.seasons = seasons;
    }

    public void addSeason(TeamSeason s) {
        this.seasons.add(s);
    }

    public Set<TeamSeason> getSeasons() {
        return seasons;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setLeague(String league) {
        this.league = league;
    }

    public String getLeague() {
        return league;
    }

    public void setYearFounded(Integer yearFounded) {
        this.yearFounded = yearFounded;
    }

    public Integer getYearFounded() {
        return yearFounded;
    }

    public void setYearLast(Integer yearLast) {
        this.yearLast = yearLast;
    }

    public Integer getYearLast() {
        return yearLast;
    }

}