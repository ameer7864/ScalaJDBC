package org.example.UnitTesting

import com.github.tototoshi.csv.CSVWriter
import config.ConfigLoader
import mvc.repositories.ArtistRepositoryForReadToCSV
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}

class ArtistReadToCSVSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "ArtistRepositoryForReadToCSV.exportCSV" should "successfully export data to CSV file (positive case)" in {
    val mockConfigLoader = mock[ConfigLoader]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]
    val mockResultSet = mock[ResultSet]
    val mockMetadata = mock[ResultSetMetaData]
    val mockCSVWriter = mock[CSVWriter]

    val repository = new ArtistRepositoryForReadToCSV(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeQuery("select * from artists")).thenReturn(mockResultSet)
    when(mockResultSet.getMetaData).thenReturn(mockMetadata)
    when(mockMetadata.getColumnCount).thenReturn(3)
    when(mockMetadata.getColumnName(1)).thenReturn("id")
    when(mockMetadata.getColumnName(2)).thenReturn("name")
    when(mockMetadata.getColumnName(3)).thenReturn("playcount")

    // Mock result set data
    when(mockResultSet.next()).thenReturn(true, true, false) // 2 rows
    when(mockResultSet.getString(1)).thenReturn("1", "2")
    when(mockResultSet.getString(2)).thenReturn("Artist1", "Artist2")
    when(mockResultSet.getString(3)).thenReturn("100", "200")

    val result = repository.exportCSV("artists", "output.csv")

    result shouldBe true

    verify(mockConfigLoader).getConnection
    verify(mockConnection).createStatement()
    verify(mockStatement).executeQuery("select * from artists")
    verify(mockResultSet, times(3)).next()
  }

  it should "return false when database has no columns (negative case)" in {
    val mockConfigLoader = mock[ConfigLoader]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]
    val mockResultSet = mock[ResultSet]
    val mockMetadata = mock[ResultSetMetaData]

    val repository = new ArtistRepositoryForReadToCSV(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeQuery("select * from empty_table")).thenReturn(mockResultSet)
    when(mockResultSet.getMetaData).thenReturn(mockMetadata)
    when(mockMetadata.getColumnCount).thenReturn(0) // No columns

    val result = repository.exportCSV("empty_table", "output.csv")

    result shouldBe false

    verify(mockMetadata).getColumnCount
    verify(mockResultSet, never()).next()
  }

  it should "handle SQL exceptions when they occur (exception case)" in {
    val mockConfigLoader = mock[ConfigLoader]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]


    val repository = new ArtistRepositoryForReadToCSV(mockConfigLoader)


    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeQuery("select * from invalid_table"))
      .thenThrow(new RuntimeException("Table does not exist"))

    val exception = intercept[RuntimeException] {
      repository.exportCSV("invalid_table", "output.csv")
    }

    exception.getMessage shouldBe "Table does not exist"

    verify(mockConfigLoader).getConnection
  }
}