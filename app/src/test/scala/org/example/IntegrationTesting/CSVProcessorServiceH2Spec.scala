package org.example.IntegrationTesting

import config.TestConfigLoader
import mvc.repositories.ArtistRepositoryForInsert
import mvc.services.CSVProcessorService
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Using

class CSVProcessorServiceH2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testConfigLoader = new TestConfigLoader()
  private val testCsvDir = "src/test/resources/csv-test-data"

  override def beforeEach(): Unit = {
    testConfigLoader.initializeDatabase()
    Files.createDirectories(Paths.get(testCsvDir))
  }

  override def afterEach(): Unit = {
    val testDir = new File(testCsvDir)
    if (testDir.exists()) {
      testDir.listFiles().foreach(_.delete())
    }
  }

  private def clearArtistsTable(): Unit = {
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      statement.execute("DELETE FROM ARTISTS")
      statement.close()
    }
  }

  "CSVProcessorService with H2" should "successfully process a valid CSV file and insert records" in {
    clearArtistsTable()

    val csvContent =
      """user_id,rank,artist_name,playcount,mbid
        |user1,1,Artist One,100,mbid1
        |user1,2,Artist Two,200,mbid2
        |user1,3,Artist Three,300,mbid3
        |""".stripMargin

    val testFile = new File(s"$testCsvDir/valid_data.csv")
    Files.write(testFile.toPath, csvContent.getBytes)

    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val service = new CSVProcessorService(repository)

    val result = service.processInBatches(testFile.getAbsolutePath, batchSize = 2)

    result.successCount shouldBe 3
    result.errorCount shouldBe 0

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS")
      resultSet.next()
      resultSet.getInt("count") shouldBe 3
      statement.close()
    }
  }

  it should "handle CSV file with some invalid records and count errors" in {
    clearArtistsTable()

    val csvContent =
      """user_id,rank,artist_name,playcount,mbid
        |user1,1,Artist One,100,mbid1
        |,2,,200,mbid2
        |user1,3,Artist Three,300,mbid3
        |invalid_user,,Invalid Artist,,
        |""".stripMargin

    val testFile = new File(s"$testCsvDir/invalid_data.csv")
    Files.write(testFile.toPath, csvContent.getBytes)

    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val service = new CSVProcessorService(repository)

    val result = service.processInBatches(testFile.getAbsolutePath, batchSize = 2)

    result.successCount should be >= 0
    result.errorCount should be >= 0
    (result.successCount + result.errorCount) shouldBe 4 // Total records in CSV

    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) as count FROM ARTISTS")
      resultSet.next()
      resultSet.getInt("count") shouldBe result.successCount
      statement.close()
    }
  }

  it should "handle non-existent CSV file gracefully" in {
    clearArtistsTable()

    val repository = new ArtistRepositoryForInsert(testConfigLoader)
    val service = new CSVProcessorService(repository)

    val exception = intercept[java.io.FileNotFoundException] {
      service.processInBatches("non_existent_file.csv", batchSize = 2)
    }

    exception.getMessage should include("non_existent_file.csv")
  }
}