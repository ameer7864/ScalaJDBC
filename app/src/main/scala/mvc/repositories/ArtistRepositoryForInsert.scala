package mvc.repositories

import config.ConfigLoader
import mvc.models.Artists
import org.slf4j.LoggerFactory

import scala.util.Using

class ArtistRepositoryForInsert(configLoader: ConfigLoader) {
  private val logger = LoggerFactory.getLogger(getClass)

  def insertInBatches(artists: List[Artists]): Boolean = {
    Using.resource(configLoader.getConnection) { connection =>
      connection.setAutoCommit(false)

      val insertQuery =
        """insert into artists(
           user_id, rank, artist_name, playcount, mbid
        ) values (?, ?, ?, ?, ?)
        """

      if(artists.nonEmpty) {
        Using.resource(connection.prepareStatement(insertQuery)) { preparedStatement =>
          artists.foreach { artist =>
            preparedStatement.setString(1, artist.userId)
            preparedStatement.setString(2, artist.rank)
            preparedStatement.setString(3, artist.artistName)
            preparedStatement.setString(4, artist.playCount)
            preparedStatement.setString(5, artist.mbid)
            preparedStatement.addBatch()
          }

          try {
            preparedStatement.executeBatch()
            connection.commit()
            true
          } catch {
            case e: Exception =>
              connection.rollback()
              logger.error(s"Failed to execute batch: ${e.getMessage}", e)
              false
          }
        }
      }else {
        logger.warn("List is empty to insert")
        false
      }
    }
  }
}


object ArtistRepositoryForInsert {
  private val defaultConfigLoader = new ConfigLoader()
  private val defaultInstance = new ArtistRepositoryForInsert(defaultConfigLoader)

  def insertInBatches(artists: List[Artists]): Boolean = defaultInstance.insertInBatches(artists)
}
