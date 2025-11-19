package mvc.repositories

import config.ConfigLoader
import com.github.tototoshi.csv.CSVWriter
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.sql.ResultSet
import scala.util.Using

class ArtistRepositoryForReadToCSV(configLoader: ConfigLoader = new ConfigLoader()) {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def exportCSV(tableName: String, filepath: String): Boolean = {
    Using.resource(configLoader.getConnection) { connection =>
      val statement = connection.createStatement()
      val resultSet: ResultSet = statement.executeQuery(s"select * from $tableName")

      val metadata = resultSet.getMetaData
      val columnCount = metadata.getColumnCount
      if (columnCount != 0) {
        val headers = (1 to columnCount).map(i => metadata.getColumnName(i)).toList
        var rowCount = 0
        Using.resource(CSVWriter.open(new File(filepath))) { writer =>
          writer.writeRow(headers)

          while (resultSet.next()) {
            val row = (1 to columnCount).map(i => resultSet.getString(i)).toList
            writer.writeRow(row)
            rowCount += 1
            if (rowCount % 10000 == 0) {
              logger.info(s"Exported $rowCount records")
            }
          }
        }
        true
      }
      else {
        logger.error("Database has no columns")
        false
      }
    }
  }
}
