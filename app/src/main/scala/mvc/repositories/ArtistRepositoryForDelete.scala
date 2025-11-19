package mvc.repositories

import config.ConfigLoader
import scala.util.Using

class ArtistRepositoryForDelete(configLoader: ConfigLoader = new ConfigLoader()) {

  def deleteFromSql(tableName: String): Int = {
    val deleteQuery = s"delete from ${tableName} where user_id = 'user1'"
//    val deleteQuery = s"delete from ${tableName} where user_id = '2'"
    Using.resource(configLoader.getConnection) { connection =>
      Using.resource(connection.prepareStatement(deleteQuery)) { preparedStatement =>
        preparedStatement.executeUpdate()
      }
    }
  }
}
