package mvc.main

import mvc.repositories.ArtistRepositoryForReadToCSV
import org.slf4j.LoggerFactory

object ArtistsSQLToCSVMain extends App {
  val logger = LoggerFactory.getLogger(getClass)
  val outputFilePath = "C:\\Users\\Ameeruddin\\Downloads\\artists_exp1.csv"
  logger.info("Starting exporting from DB to CSV")

  private val isExportComplete = new ArtistRepositoryForReadToCSV().exportCSV("artists", outputFilePath)

  if(isExportComplete)  logger.info("Export completed")
  else logger.error("Export not completed")
}
