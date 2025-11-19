package mvc.main

import mvc.repositories.ArtistRepositoryForDelete
import org.slf4j.LoggerFactory

object ArtistsDeleteMain extends App {
  val logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Started deleting the Data From SQL DB")

  val rowCount = new ArtistRepositoryForDelete().deleteFromSql("artists")
  logger.info(s"Delete Complete !! , $rowCount rows are deleted")
}
