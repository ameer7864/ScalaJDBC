package org.example.UnitTesting

import config.ConfigLoader
import mvc.repositories.ArtistRepositoryForInsert
import com.github.tototoshi.csv.CSVReader
import org.scalatestplus.mockito.MockitoSugar
import java.sql.{Connection, PreparedStatement}

trait TestMockUtils extends MockitoSugar {

  protected def createDatabaseMocks(): (ConfigLoader, Connection, PreparedStatement) = {
    val mockConfigLoader = mock[ConfigLoader]
    val mockConnection = mock[Connection]
    val mockPreparedStatement = mock[PreparedStatement]
    (mockConfigLoader, mockConnection, mockPreparedStatement)
  }

  protected def createCSVMocks(): (ArtistRepositoryForInsert, CSVReader) = {
    val mockRepository = mock[ArtistRepositoryForInsert]
    val mockCSVReader = mock[CSVReader]
    (mockRepository, mockCSVReader)
  }
}