package mvc.main

import mvc.services.CSVProcessorService
import org.slf4j.LoggerFactory

object ArtistsToSQLMain {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val CSVFilePath = "C:\\Users\\Ameeruddin\\Downloads\\artists.csv"
    logger.info("Starting artists data processing")

    val result = new CSVProcessorService().processInBatches(CSVFilePath, batchSize = 20000)

    logger.info(s"Processing completed. Success: ${result.successCount}, Errors: ${result.errorCount}")
  }
}