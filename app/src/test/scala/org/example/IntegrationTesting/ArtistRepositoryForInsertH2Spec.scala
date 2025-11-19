package org.example.IntegrationTesting

import config.TestConfigLoader
import mvc.models.Artists
import mvc.repositories.ArtistRepositoryForInsert
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class ArtistRepositoryForInsertH2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testConfigLoader = new TestConfigLoader()

  override def beforeEach(): Unit = {
    testConfigLoader.initializeDatabase()
  }

  override def afterEach(): Unit = {}

  "ArtistRepository with H2" should "insert artists in batches successfully" in {
    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val artists = List(
      Artists("user2", "1", "New Artist One", "50", "new-mbid1"),
      Artists("user2", "2", "New Artist Two", "75", "new-mbid2")
    )

    val result = repository.insertInBatches(artists)

    result shouldBe true

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM artists WHERE user_id = 'user2'")
      resultSet.next()
      resultSet.getInt("count") shouldBe 2

      val detailResultSet = statement.executeQuery(
        "SELECT user_id, rank, artist_name, playcount, mbid FROM artists WHERE user_id = 'user2' ORDER BY rank"
      )

      detailResultSet.next()
      detailResultSet.getString("user_id") shouldBe "user2"
      detailResultSet.getString("rank") shouldBe "1"
      detailResultSet.getString("artist_name") shouldBe "New Artist One"
      detailResultSet.getString("playcount") shouldBe "50"
      detailResultSet.getString("mbid") shouldBe "new-mbid1"

      // Second inserted record
      detailResultSet.next()
      detailResultSet.getString("user_id") shouldBe "user2"
      detailResultSet.getString("rank") shouldBe "2"
      detailResultSet.getString("artist_name") shouldBe "New Artist Two"
      detailResultSet.getString("playcount") shouldBe "75"
      detailResultSet.getString("mbid") shouldBe "new-mbid2"

      statement.close()
    }
  }

  it should "handle empty artists list" in {
    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val artists = List.empty[Artists]

    val result = repository.insertInBatches(artists)

    result shouldBe false

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM artists")
      resultSet.next()
      resultSet.getInt("count") shouldBe 3 // Only the initial 3 test records
      statement.close()
    }
  }

  it should "insert multiple batches correctly" in {
    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val firstBatch = List(
      Artists("user3", "1", "Batch One Artist", "100", "batch1-mbid")
    )
    val secondBatch = List(
      Artists("user4", "1", "Batch Two Artist", "200", "batch2-mbid"),
      Artists("user4", "2", "Batch Two Artist 2", "300", "batch2-mbid2")
    )

    val result1 = repository.insertInBatches(firstBatch)
    val result2 = repository.insertInBatches(secondBatch)

    result1 shouldBe true
    result2 shouldBe true

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()

      val resultSet1 = statement.executeQuery("SELECT COUNT(*) as count FROM artists WHERE user_id = 'user3'")
      resultSet1.next()
      resultSet1.getInt("count") shouldBe 1

      // Check second batch
      val resultSet2 = statement.executeQuery("SELECT COUNT(*) as count FROM artists WHERE user_id = 'user4'")
      resultSet2.next()
      resultSet2.getInt("count") shouldBe 2

      // Check total records (3 initial + 3 new)
      val totalResultSet = statement.executeQuery("SELECT COUNT(*) as count FROM artists")
      totalResultSet.next()
      totalResultSet.getInt("count") shouldBe 6

      statement.close()
    }
  }

  it should "handle duplicate data insertion" in {
    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val duplicateArtists = List(
      Artists("user1", "1", "Duplicate Artist", "100", "duplicate-mbid") // Same user_id and rank as existing
    )

    val result = repository.insertInBatches(duplicateArtists)

    result shouldBe true

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM artists WHERE user_id = 'user1' AND rank = '1'")
      resultSet.next()
      resultSet.getInt("count") shouldBe 2
      statement.close()
    }
  }

  it should "maintain data integrity after insertion" in {
    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val artists = List(
      Artists("user5", "1", "Integrity Test Artist", "999", "integrity-mbid")
    )

    val result = repository.insertInBatches(artists)

    result shouldBe true

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        "SELECT user_id, rank, artist_name, playcount, mbid FROM artists WHERE user_id = 'user5'"
      )

      resultSet.next()
      resultSet.getString("user_id") shouldBe "user5"
      resultSet.getString("rank") shouldBe "1"
      resultSet.getString("artist_name") shouldBe "Integrity Test Artist"
      resultSet.getString("playcount") shouldBe "999"
      resultSet.getString("mbid") shouldBe "integrity-mbid"

      statement.close()
    }
  }
}