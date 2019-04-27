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
}
