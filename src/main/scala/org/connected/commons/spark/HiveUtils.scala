/*
 * Copyright  2019 Gopal Tiwari
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.connected.commons.spark

import org.apache.spark.sql.SparkSession

/**
  * Contains various Hive database related utilities.
  */
object HiveUtils {

  /**
    * Used to drop a table from Hive.
    *
    * @param sparkSession to connect to Hive metastore.
    * @param databaseName database name where to look for table.
    * @param tableName    table name to be dropped.
    * @return true if tables is dropped, false if table is not present in Database or if task failed.
    */
  def dropTable(sparkSession: SparkSession, databaseName: String, tableName: String): Boolean = {
    if (!sparkSession.sql(s"show tables in ${databaseName.trim.toLowerCase()}").where(s"tableName = '${tableName.trim.toLowerCase()}'").collect().isEmpty) {
      sparkSession.sql(s"""alter table ${databaseName.trim.toLowerCase()}.${tableName.trim.toLowerCase()} set TBLPROPERTIES("auto.purge" = "true")""")
      sparkSession.sql(s"drop table if exists  ${databaseName.trim.toLowerCase()}.${tableName.trim.toLowerCase()} purge")
      true
    } else false
  }

  /**
    * Used to check if a database is present.
    *
    * @param sparkSession to connect to Hive metastore.
    * @param databaseName database name to check.
    * @return true is database exists else false.
    */
  def isDatabaseExists(sparkSession: SparkSession, databaseName: String): Boolean = {
    if (!sparkSession.sql(s"SHOW databases ").where(s"database_name = '${databaseName.trim.toLowerCase}'").collect().isEmpty)
      true
    else
      false
  }

  /**
    * This method will be used to return if a table exists in db or not
    * @param sparkSession  to connect to Hive metastore.
    * @param tableName    string containing table name to be checked.
    * @param databaseName where to check for the table.
    * @return true if table exists else false.
    */
  def isTableExists(sparkSession: SparkSession, tableName: String, databaseName: String): Boolean = {
    if (tableName.trim.length == 0 || databaseName.trim.length == 0)
      return false
    if (!sparkSession.sql(s"show tables in ${databaseName.trim.toLowerCase} ").where(s"tableName = '${tableName.trim.toLowerCase}'").collect().isEmpty)
      true
    else
      false
  }

  /**
    * To check if a table exists or not.
    *
    * @param sparkSession         to connect to Hive metastore.
    * @param tableNameWithDBName string containing table name with database name seperated by a '.' e.g. DBNAME.TABLENAME
    * @return true if table exists else false.
    */
  def isTableExists(sparkSession: SparkSession, tableNameWithDBName: String): Boolean = {
    if (tableNameWithDBName.trim.toLowerCase().split('.').length != 2)
      return false
    val dbName = tableNameWithDBName.trim.toLowerCase().split('.')(0)
    val tableName = tableNameWithDBName.trim.toLowerCase().split('.')(1)
    isTableExists(sparkSession, tableName, dbName)
  }
}
