package mvc.main

import mvc.repositories.ArtistRepositoryForUpdate
import org.slf4j.LoggerFactory

object ArtistsUpdateMain extends App {
  val logger = LoggerFactory.getLogger(getClass)
  logger.info("Starting bulk update...")

  private val totalUpdatedRows = new ArtistRepositoryForUpdate().updateInBatches()
  logger.info(s"Update completed! , $totalUpdatedRows records are updated")
}
