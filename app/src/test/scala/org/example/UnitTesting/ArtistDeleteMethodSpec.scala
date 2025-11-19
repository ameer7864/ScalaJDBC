package org.example.UnitTesting

import mvc.repositories.ArtistRepositoryForDelete
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class ArtistDeleteMethodSpec extends AnyFlatSpec with Matchers with MockitoSugar with TestMockUtils {
  "ArtistRepositoryForDelete.deleteFromSql" should "return the number of deleted rows when deletion is successful" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForDelete(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement("delete from ARTISTS where user_id = '2'"))
      .thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(3)

    val result = repository.deleteFromSql("artists")

    result shouldBe 3
    verify(mockConfigLoader).getConnection
    verify(mockConnection).prepareStatement("delete from ARTISTS where user_id = '2'")
    verify(mockPreparedStatement).executeUpdate()
    verify(mockPreparedStatement).close()
    verify(mockConnection).close()
  }

  it should "return 0 when no rows are deleted" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForDelete(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement("delete from USERS where user_id = '2'"))
      .thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(0)

    val result = repository.deleteFromSql("users")

    result shouldBe 0
    verify(mockPreparedStatement).executeUpdate()
    verify(mockPreparedStatement).close()
    verify(mockConnection).close()
  }

  it should "use the correct table name in the SQL query" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForDelete(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement("delete from CUSTOM_TABLE where user_id = '2'"))
      .thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(1)

    val result = repository.deleteFromSql("custom_table")

    result shouldBe 1
    verify(mockConnection).prepareStatement("delete from CUSTOM_TABLE where user_id = '2'")
  }

  it should "handle different table names correctly" in {
    val testTables = List("users", "artists", "albums", "tracks")

    testTables.foreach { tableName =>
      val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
      val repository = new ArtistRepositoryForDelete(mockConfigLoader)

      when(mockConfigLoader.getConnection).thenReturn(mockConnection)
      when(mockConnection.prepareStatement(s"delete from ${tableName.toUpperCase} where user_id = '2'"))
        .thenReturn(mockPreparedStatement)
      when(mockPreparedStatement.executeUpdate()).thenReturn(1)

      val result = repository.deleteFromSql(tableName)

      result shouldBe 1
      verify(mockConnection).prepareStatement(s"delete from ${tableName.toUpperCase} where user_id = '2'")
    }
  }
}