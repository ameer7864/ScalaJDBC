package mvc.repositories

import config.ConfigLoader
import scala.util.Using

class ArtistRepositoryForUpdate(val configLoader: ConfigLoader = new ConfigLoader()) {

  def updateInBatches(): Int = {
    Using.resource(configLoader.getConnection) { connection =>

      val updateSQL =
        """
            UPDATE artists
            SET playcount = '15'
        """

      val statement = connection.prepareStatement(updateSQL)
      val rowsUpdated = statement.executeUpdate()
      statement.close()

      rowsUpdated
    }
  }
}