package org.example.UnitTesting

import mvc.repositories.ArtistRepositoryForUpdate
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class ArtistUpdateMethodSpec extends AnyFlatSpec with Matchers with MockitoSugar with TestMockUtils{
  "ArtistRepositoryForUpdate.updateInBatches" should "return the number of updated rows when update is successful" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForUpdate(mockConfigLoader)
    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(5)

    val result = repository.updateInBatches()

    result shouldBe 5

    verify(mockConfigLoader).getConnection
    verify(mockConnection).prepareStatement(anyString())
    verify(mockPreparedStatement).executeUpdate()
    verify(mockPreparedStatement).close()
    verify(mockConnection).close()
  }

  it should "return 0 when no rows are updated" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForUpdate(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(0) // No rows updated

    val result = repository.updateInBatches()

    result shouldBe 0

    verify(mockPreparedStatement).executeUpdate()
    verify(mockPreparedStatement).close()
    verify(mockConnection).close()
  }

  it should "increment totalUpdated only when rows are updated" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForUpdate(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(3) // 3 rows updated

    val result = repository.updateInBatches()

    result shouldBe 3

    verify(mockPreparedStatement).executeUpdate()
    verify(mockPreparedStatement).close()
    verify(mockConnection).close()
  }

  it should "not increment totalUpdated when no rows are affected" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val repository = new ArtistRepositoryForUpdate(mockConfigLoader)

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(0) // No rows updated

    val result = repository.updateInBatches()

    result shouldBe 0

    verify(mockPreparedStatement).executeUpdate()
  }
}