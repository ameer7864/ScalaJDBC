package mvc.services

import mvc.models.{Artists, ProcessingResult}
import mvc.repositories.ArtistRepositoryForInsert
import com.github.tototoshi.csv.CSVReader
import config.ConfigLoader
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.Using

class CSVProcessorService(repository: ArtistRepositoryForInsert = new ArtistRepositoryForInsert(new ConfigLoader)) {
  private val logger = LoggerFactory.getLogger(getClass)

  def processInBatches(filepath: String, batchSize: Int): ProcessingResult = {
    logger.info(s"Processing CSV file: $filepath with batch size: $batchSize")
    Using.resource(CSVReader.open(new File(filepath))) { reader =>
      processReader(reader, batchSize)
    }
  }

  def processReader(reader: CSVReader, batchSize: Int): ProcessingResult = {
    val iterator = reader.iteratorWithHeaders
    var successCount = 0
    var errorCount = 0
    var currentBatch = List.empty[Artists]
    while (iterator.hasNext) {
      try {
        val row = iterator.next()

        val artist = Artists(
          userId = row.getOrElse("user_id", ""),
          rank = row.getOrElse("rank", ""),
          artistName = row.getOrElse("artist_name", ""),
          playCount = row.getOrElse("playcount", ""),
          mbid = row.getOrElse("mbid", "")
        )

        currentBatch = artist :: currentBatch
        successCount += 1

        if (successCount % batchSize == 0) {
          repository.insertInBatches(currentBatch.reverse)
          currentBatch = List.empty[Artists]
          logger.info(s"Processed $successCount records so far")
        }
      } catch {
        case e: Exception =>
          errorCount += 1
          if (errorCount <= 10) {
            logger.error(s"Error processing record #${successCount + errorCount}", e)
          } else if (errorCount == 11) {
            logger.warn("Suppressing further error logs after 10 errors")
          }
      }
    }

    if (currentBatch.nonEmpty) {
      repository.insertInBatches(currentBatch.reverse)
      logger.info(s"Processed final ${currentBatch.size} records")
    }

    ProcessingResult(successCount, errorCount)
  }
}
