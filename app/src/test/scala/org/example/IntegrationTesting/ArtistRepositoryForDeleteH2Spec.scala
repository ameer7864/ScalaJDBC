package org.example.IntegrationTesting

import config.TestConfigLoader
import mvc.repositories.ArtistRepositoryForDelete
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class ArtistRepositoryForDeleteH2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testConfigLoader = new TestConfigLoader()

  override def beforeEach(): Unit = {
    testConfigLoader.initializeDatabase()
  }

  override def afterEach(): Unit = {}

  "ArtistRepositoryForDelete with H2" should "delete records with user_id = 'user1' from ARTISTS table" in {
    // Given - Inject TestConfigLoader
    val repository = new ArtistRepositoryForDelete(testConfigLoader)

    // Verify initial data exists for user_id = 'user1'
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val initialCountResult = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user1'")
      initialCountResult.next()
      initialCountResult.getInt("count") shouldBe 3 // All 3 test records have user_id = 'user1'
      statement.close()
    }

    // When
    val result = repository.deleteFromSql("ARTISTS")

    // Then
    result shouldBe 3 // Should delete all 3 records

    // Verify records were actually deleted
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user1'")
      resultSet.next()
      resultSet.getInt("count") shouldBe 0
      statement.close()
    }
  }

  it should "return 0 when no records match the deletion criteria" in {
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      statement.execute("DELETE FROM ARTISTS WHERE user_id = 'user1'")
      statement.close()
    }

    val repository = new ArtistRepositoryForDelete(testConfigLoader)

    val result = repository.deleteFromSql("ARTISTS")

    result shouldBe 0

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user1'")
      resultSet.next()
      resultSet.getInt("count") shouldBe 0
      statement.close()
    }
  }

  it should "handle non-existent table gracefully" in {
    val repository = new ArtistRepositoryForDelete(testConfigLoader)

    val exception = intercept[org.h2.jdbc.JdbcSQLSyntaxErrorException] {
      repository.deleteFromSql("non_existent_table")
    }

    exception.getMessage should include("Table \"NON_EXISTENT_TABLE\" not found")
  }

  it should "work correctly when table has mixed user_id values" in {
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      statement.execute(
        """
          INSERT INTO ARTISTS (user_id, rank, artist_name, playcount, mbid) VALUES
          ('user2', '1', 'User2 Artist One', '150', 'user2-mbid1'),
          ('user2', '2', 'User2 Artist Two', '250', 'user2-mbid2'),
          ('user3', '1', 'User3 Artist One', '999', 'user3-mbid1')
        """
      )
      statement.close()
    }

    val repository = new ArtistRepositoryForDelete(testConfigLoader)

    val result = repository.deleteFromSql("ARTISTS")

    result shouldBe 3

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()

      val user1Result = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user1'")
      user1Result.next()
      user1Result.getInt("count") shouldBe 0

      val user2Result = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user2'")
      user2Result.next()
      user2Result.getInt("count") shouldBe 2

      val user3Result = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS WHERE user_id = 'user3'")
      user3Result.next()
      user3Result.getInt("count") shouldBe 1

      statement.close()
    }
  }

}