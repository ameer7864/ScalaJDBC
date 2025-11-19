package config

import com.typesafe.config.{Config, ConfigFactory}
import java.sql.{Connection, DriverManager}

class ConfigLoader extends AbstractConfig {
  private val config: Config = ConfigFactory.load()
  private val databaseConfig = config.getConfig("database")

  override def getUrl: String = databaseConfig.getString("url")
  override def getUsername: String = databaseConfig.getString("username")
  override def getPassword: String = databaseConfig.getString("password")

  override def getConnection: Connection = {
    DriverManager.getConnection(getUrl, getUsername, getPassword)
  }
}