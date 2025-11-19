package org.example.IntegrationTesting

import config.TestConfigLoader
import mvc.repositories.ArtistRepositoryForUpdate
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class ArtistRepositoryForUpdateH2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testConfigLoader = new TestConfigLoader()

  override def beforeEach(): Unit = {
    testConfigLoader.initializeDatabase()
  }

  override def afterEach(): Unit = {}

  "ArtistRepositoryForUpdate with H2" should "update all artists playcount to 15" in {
    val repository = new ArtistRepositoryForUpdate(testConfigLoader)

    val result = repository.updateInBatches()

    result shouldBe 3

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT playcount FROM artists")

      while (resultSet.next()) {
        resultSet.getString("playcount") shouldBe "15"
      }

      statement.close()
    }
  }

  it should "return 0 when no rows are updated on empty table" in {
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      statement.execute("DELETE FROM artists")
      statement.close()
    }

    val repository = new ArtistRepositoryForUpdate(testConfigLoader)

    val result = repository.updateInBatches()

    result shouldBe 0
  }

  it should "handle database operations correctly" in {
    val repository = new ArtistRepositoryForUpdate(testConfigLoader)

    val result1 = repository.updateInBatches()
    result1 shouldBe 3

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM artists WHERE playcount = '15'")
      resultSet.next()
      resultSet.getInt("count") shouldBe 3
      statement.close()
    }
  }
}