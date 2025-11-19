package org.example.UnitTesting

import mvc.models.Artists
import mvc.services.CSVProcessorService
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class CSVProcessorServiceSpec extends AnyFlatSpec with Matchers with MockitoSugar with TestMockUtils {
  private val sampleRow = Map(
    "user_id" -> "user123",
    "rank" -> "1",
    "artist_name" -> "Test Artist",
    "playcount" -> "100",
    "mbid" -> "test-mbid"
  )

  "CSVProcessorService.processInBatches" should "process full batches successfully" in {
    val (mockRepository, mockCSVReader) = createCSVMocks()
    val service = new CSVProcessorService(mockRepository)

    val mockIterator = mock[Iterator[Map[String, String]]]
    when(mockIterator.hasNext).thenReturn(true, true, true, true, false)
    when(mockIterator.next()).thenReturn(sampleRow, sampleRow, sampleRow, sampleRow)
    when(mockCSVReader.iteratorWithHeaders).thenReturn(mockIterator)

    val result = service.processReader(mockCSVReader, 2)

    result.successCount shouldBe 4
    result.errorCount shouldBe 0

    verify(mockRepository, times(2)).insertInBatches(any[List[Artists]])
  }

  it should "create Artists with correct field mapping" in {
    val (mockRepository, mockCSVReader) = createCSVMocks()
    val service = new CSVProcessorService(mockRepository)

    val testRow = Map(
      "user_id" -> "test_user",
      "rank" -> "5",
      "artist_name" -> "Test Band",
      "playcount" -> "250",
      "mbid" -> "test-mbid-123"
    )

    val mockIterator = mock[Iterator[Map[String, String]]]
    when(mockIterator.hasNext).thenReturn(true, false)
    when(mockIterator.next()).thenReturn(testRow)
    when(mockCSVReader.iteratorWithHeaders).thenReturn(mockIterator)

    val result = service.processReader(mockCSVReader, 10)

    result.successCount shouldBe 1
    result.errorCount shouldBe 0

    verify(mockRepository).insertInBatches(argThat { batch: List[Artists] =>
      batch.size == 1 && batch.head == Artists(
        userId = "test_user",
        rank = "5",
        artistName = "Test Band",
        playCount = "250",
        mbid = "test-mbid-123"
      )
    })
  }

  it should "handle missing optional fields by using empty strings" in {
    val (mockRepository, mockCSVReader) = createCSVMocks()
    val service = new CSVProcessorService(mockRepository)

    val incompleteRow = Map(
      "user_id" -> "test_user",
      "rank" -> "5"
    )

    val mockIterator = mock[Iterator[Map[String, String]]]
    when(mockIterator.hasNext).thenReturn(true, false)
    when(mockIterator.next()).thenReturn(incompleteRow)
    when(mockCSVReader.iteratorWithHeaders).thenReturn(mockIterator)

    val result = service.processReader(mockCSVReader, 10)

    result.successCount shouldBe 1
    result.errorCount shouldBe 0

    verify(mockRepository).insertInBatches(argThat { batch: List[Artists] =>
      batch.size == 1 && batch.head == Artists(
        userId = "test_user",
        rank = "5",
        artistName = "",
        playCount = "",
        mbid = ""
      )
    })
  }
}