package config

import java.sql.Connection

trait AbstractConfig {
  def getUrl : String
  def getUsername : String
  def getPassword : String
  def getConnection : Connection
}
