package org.example.IntegrationTesting

import config.TestConfigLoader
import mvc.repositories.ArtistRepositoryForReadToCSV
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class ArtistRepositoryForReadToCSVH2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val testConfigLoader = new TestConfigLoader()
  private val testOutputDir = "src/test/resources/csv-output"

  override def beforeEach(): Unit = {
    testConfigLoader.initializeDatabase()
    Files.createDirectories(Paths.get(testOutputDir))
  }

  override def afterEach(): Unit = {
    val outputDir = new File(testOutputDir)
    if (outputDir.exists()) {
      outputDir.listFiles().foreach(_.delete())
    }
  }

  "ArtistRepositoryForReadToCSV with H2" should "successfully export table data to CSV file" in {
    val repository = new ArtistRepositoryForReadToCSV(testConfigLoader)
    val outputFile = new File(s"$testOutputDir/export_success.csv")

    val result = repository.exportCSV("ARTISTS", outputFile.getAbsolutePath)

    result shouldBe true

    outputFile.exists() shouldBe true

    Using.resource(Source.fromFile(outputFile)) { source =>
      val lines = source.getLines().toList
      lines should have size 4 // Header + 3 data rows

      lines.head shouldBe "USER_ID,RANK,ARTIST_NAME,PLAYCOUNT,MBID"

      lines should contain allOf(
        "USER_ID,RANK,ARTIST_NAME,PLAYCOUNT,MBID",
        "user1,1,Artist One,100,mbid1",
        "user1,2,Artist Two,200,mbid2",
        "user1,3,Artist Three,300,mbid3"
      )
    }
  }

  it should "handle empty table by creating CSV with only headers" in {
    Using.resource(testConfigLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      statement.execute("CREATE TABLE EMPTY_TABLE (id VARCHAR(10), name VARCHAR(50))")
      statement.close()
    }

    val repository = new ArtistRepositoryForReadToCSV(testConfigLoader)
    val outputFile = new File(s"$testOutputDir/export_empty.csv")

    val result = repository.exportCSV("EMPTY_TABLE", outputFile.getAbsolutePath)

    result shouldBe true

    outputFile.exists() shouldBe true

    Using.resource(Source.fromFile(outputFile)) { source =>
      val lines = source.getLines().toList
      lines should have size 1
      lines.head shouldBe "ID,NAME"
    }
  }

  it should "throw exception when trying to export from non-existent table" in {
    val repository = new ArtistRepositoryForReadToCSV(testConfigLoader)
    val outputFile = new File(s"$testOutputDir/export_nonexistent.csv")

    val exception = intercept[org.h2.jdbc.JdbcSQLSyntaxErrorException] {
      repository.exportCSV("NON_EXISTENT_TABLE", outputFile.getAbsolutePath)
    }

    exception.getMessage should include("Table \"NON_EXISTENT_TABLE\" not found")
  }

}