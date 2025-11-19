package org.example.UnitTesting

import mvc.models.Artists
import mvc.repositories.ArtistRepositoryForInsert
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class ArtistInsertMethodSpec extends AnyFlatSpec with Matchers with MockitoSugar with TestMockUtils {
  "ArtistRepositoryForInsert.insertInBatches" should "successfully insert artists in batches" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val artistRepository = new ArtistRepositoryForInsert(mockConfigLoader)

    val artists = List(
      Artists("user1", "1", "Artist One", "100", "mbid1"),
      Artists("user1", "2", "Artist Two", "200", "mbid2")
    )

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeBatch()).thenReturn(Array(1, 1))

    val result = artistRepository.insertInBatches(artists)

    result shouldBe true

    verify(mockConnection).setAutoCommit(false)
    verify(mockPreparedStatement, times(2)).setString(1, "user1")
    verify(mockPreparedStatement, times(1)).setString(2, "1")
    verify(mockPreparedStatement, times(1)).setString(2, "2")
    verify(mockPreparedStatement, times(1)).setString(3, "Artist One")
    verify(mockPreparedStatement, times(1)).setString(3, "Artist Two")
    verify(mockPreparedStatement, times(1)).setString(4, "100")
    verify(mockPreparedStatement, times(1)).setString(4, "200")
    verify(mockPreparedStatement, times(1)).setString(5, "mbid1")
    verify(mockPreparedStatement, times(1)).setString(5, "mbid2")
    verify(mockPreparedStatement, times(2)).addBatch()
    verify(mockPreparedStatement).executeBatch()
    verify(mockConnection).commit()
    verify(mockConnection).close()
  }

  it should "return false and rollback when batch execution fails or Exception occurs" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val artistRepository = new ArtistRepositoryForInsert(mockConfigLoader)

    val artists = List(Artists("user1", "1", "Artist One", "100", "mbid1"))

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeBatch()).thenThrow(new RuntimeException("Batch failed"))

    val result = artistRepository.insertInBatches(artists)

    result shouldBe false

    verify(mockConnection).setAutoCommit(false)
    verify(mockPreparedStatement).addBatch()
    verify(mockPreparedStatement).executeBatch()
    verify(mockConnection).rollback()
    verify(mockConnection).close()
    verify(mockPreparedStatement).close()
  }

  it should "handle empty artists list" in {
    val (mockConfigLoader, mockConnection, mockPreparedStatement) = createDatabaseMocks()
    val artistRepository = new ArtistRepositoryForInsert(mockConfigLoader)

    val artists = List.empty[Artists]

    when(mockConfigLoader.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeBatch()).thenReturn(Array.empty[Int])

    val result = artistRepository.insertInBatches(artists)

    result shouldBe false

    verify(mockConnection).setAutoCommit(false)
    verify(mockPreparedStatement, never()).addBatch()
    verify(mockConnection).close()
  }
}