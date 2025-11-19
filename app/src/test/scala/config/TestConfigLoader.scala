package config

import com.typesafe.config.ConfigFactory

import java.sql.{Connection, DriverManager}
import scala.util.Using

class TestConfigLoader extends ConfigLoader {

  private val config = ConfigFactory.load()
  private val h2Config = config.getConfig("h2")

  override def getUrl: String = h2Config.getString("testUrl")
  override def getUsername: String = h2Config.getString("testUsername")
  override def getPassword: String = h2Config.getString("testPassword")

  override def getConnection: Connection = {
    DriverManager.getConnection(getUrl, getUsername, getPassword)
  }

  def initializeDatabase(): Unit = {
    Using.resource(getConnection) { connection =>
      val statement = connection.createStatement()

      // Drop table if exists and create fresh
      statement.execute("DROP TABLE IF EXISTS ARTISTS")

      // Create artists table with proper SQL syntax
      statement.execute(
        """
          CREATE TABLE ARTISTS (
            user_id VARCHAR(100),
            rank VARCHAR(100),
            artist_name VARCHAR(500),
            playcount VARCHAR(100),
            mbid VARCHAR(100)
          )
        """
      )

      // Insert test data
      statement.execute(
        """
          INSERT INTO ARTISTS (user_id, rank, artist_name, playcount, mbid) VALUES
          ('user1', '1', 'Artist One', '100', 'mbid1'),
          ('user1', '2', 'Artist Two', '200', 'mbid2'),
          ('user1', '3', 'Artist Three', '300', 'mbid3')
        """
      )

      statement.close()
    }
  }
}