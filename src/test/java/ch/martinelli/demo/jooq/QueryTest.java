package ch.martinelli.demo.jooq;

import ch.martinelli.demo.jooq.database.tables.records.AthleteRecord;
import ch.martinelli.demo.jooq.database.tables.records.CompetitionRecord;
import ch.martinelli.demo.jooq.projection.AthleteDTO;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jooq.JooqTest;

import java.util.List;

import static ch.martinelli.demo.jooq.database.Tables.*;
import static ch.martinelli.demo.jooq.database.tables.Athlete.ATHLETE;
import static ch.martinelli.demo.jooq.database.tables.Club.CLUB;
import static ch.martinelli.demo.jooq.database.tables.Competition.COMPETITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.Records.mapping;
import static org.jooq.impl.DSL.multiset;
import static org.jooq.impl.DSL.select;

@JooqTest
public class QueryTest {

    @Autowired
    private DSLContext dsl;

    @Test
    void find_competitions() {
        Result<CompetitionRecord> competitions = dsl
                .selectFrom(COMPETITION)
                .fetch();

        assertThat(competitions).hasSize(1);
    }

    @Test
    void insert_athlete() {
        Long id = dsl.insertInto(ATHLETE)
                .columns(ATHLETE.FIRST_NAME, ATHLETE.LAST_NAME, ATHLETE.GENDER, ATHLETE.YEAR_OF_BIRTH, ATHLETE.CLUB_ID, ATHLETE.ORGANIZATION_ID)
                .values("Sanya", "Richards-Ross", "f", 1985, 1L, 1L)
                .returningResult(ATHLETE.ID)
                .fetchOneInto(Long.class);

        assertThat(id).isEqualTo(1);
    }

    @Test
    void updatable_record() {
        AthleteRecord athlete = dsl.newRecord(ATHLETE);
        athlete.setFirstName("Mujinga");
        athlete.setLastName("Kambundji");
        athlete.setGender("f");
        athlete.setYearOfBirth(1992);
        athlete.setClubId(1L);
        athlete.setOrganizationId(1L);

        athlete.store();

        assertThat(athlete.getId()).isNotNull();
    }

    @Test
    void projection() {
        List<AthleteDTO> athletes = dsl
                .select(ATHLETE.FIRST_NAME, ATHLETE.LAST_NAME, CLUB.NAME)
                .from(ATHLETE)
                .join(CLUB).on(CLUB.ID.eq(ATHLETE.CLUB_ID))
                .fetchInto(AthleteDTO.class);

        assertThat(athletes).hasSize(1);
        assertThat(athletes.get(0)).satisfies(athlete -> {
            assertThat(athlete.firstName()).isEqualTo("Armand");
            assertThat(athlete.lastName()).isEqualTo("Duplantis");
            assertThat(athlete.clubName()).isEqualTo("Louisiana State University");
        });
    }

    @Test
    void implicit_join() {
        List<AthleteDTO> athletes = dsl
                .select(ATHLETE.FIRST_NAME, ATHLETE.LAST_NAME, ATHLETE.club().NAME)
                .from(ATHLETE)
                .fetchInto(AthleteDTO.class);

        assertThat(athletes).hasSize(1);
        assertThat(athletes.get(0)).satisfies(athlete -> {
            assertThat(athlete.firstName()).isEqualTo("Armand");
            assertThat(athlete.lastName()).isEqualTo("Duplantis");
            assertThat(athlete.clubName()).isEqualTo("Louisiana State University");
        });
    }

    @Test
    void delete() {
        int deletedRows = dsl
                .deleteFrom(ATHLETE)
                .where(ATHLETE.ID.eq(1000L))
                .execute();

        assertThat(deletedRows).isEqualTo(1);
    }

    @Test
    void multisetSelect() {
        var competitionId = 1L;

        CompetitionRankingData competitionRankingData = dsl.select(
                        COMPETITION.NAME,
                        COMPETITION.COMPETITION_DATE,
                        COMPETITION.ALWAYS_FIRST_THREE_MEDALS,
                        COMPETITION.MEDAL_PERCENTAGE,
                        multiset(select(
                                        CATEGORY.ABBREVIATION,
                                        CATEGORY.NAME,
                                        CATEGORY.YEAR_FROM,
                                        CATEGORY.YEAR_TO,
                                        multiset(select(
                                                        CATEGORY_ATHLETE.athlete().FIRST_NAME,
                                                        CATEGORY_ATHLETE.athlete().LAST_NAME,
                                                        CATEGORY_ATHLETE.athlete().YEAR_OF_BIRTH,
                                                        CATEGORY_ATHLETE.athlete().club().NAME,
                                                        multiset(select(
                                                                        RESULT.event().ABBREVIATION,
                                                                        RESULT.RESULT_,
                                                                        RESULT.POINTS
                                                                )
                                                                        .from(RESULT)
                                                                        .where(RESULT.ATHLETE_ID.eq(CATEGORY_ATHLETE.athlete().ID))
                                                                        .and(RESULT.COMPETITION_ID.eq(COMPETITION.ID))
                                                                        .and(RESULT.CATEGORY_ID.eq(CATEGORY.ID))
                                                                        .orderBy(RESULT.POSITION)
                                                        ).convertFrom(r -> r.map(mapping(CompetitionRankingData.Category.Athlete.Result::new)))
                                                )
                                                        .from(CATEGORY_ATHLETE)
                                                        .where(CATEGORY_ATHLETE.CATEGORY_ID.eq(COMPETITION.SERIES_ID))
                                        ).convertFrom(r -> r.map(mapping(CompetitionRankingData.Category.Athlete::new)))
                                )
                                        .from(CATEGORY)
                                        .where(CATEGORY.SERIES_ID.eq(COMPETITION.SERIES_ID))
                                        .orderBy(CATEGORY.ABBREVIATION)
                        ).convertFrom(r -> r.map(mapping(CompetitionRankingData.Category::new)))
                )
                .from(COMPETITION)
                .where(COMPETITION.ID.eq(competitionId))
                .fetchOne(mapping(CompetitionRankingData::new));

        System.out.println(competitionRankingData);
    }
}
