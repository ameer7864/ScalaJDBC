package org.example.UnitTesting

import mvc.repositories.ArtistRepositoryForDelete
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.SQLException

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

  it should "throw exception when preparing statement fails" in {
    val (mockConfigLoader, mockConnection, _) = createDatabaseMocks()
    val repository = new ArtistRepositoryForDelete(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement("delete from ARTISTS where user_id = '2'"))
      .thenThrow(new SQLException("Invalid SQL syntax"))

    val exception = intercept[SQLException] {
      repository.deleteFromSql("artists")
    }

    exception.getMessage shouldBe "Invalid SQL syntax"
    verify(mockConnection).close() // Connection should still be closed
  }
}