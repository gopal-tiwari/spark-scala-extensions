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

package org.connected.commons.hdfs

import java.sql.Timestamp
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, FileUtil, ParentNotDirectoryException, Path}
import org.apache.spark.sql.SparkSession

object HDFSUtils {

  /**
    * Used to move files in HDFS from one directory to another.
    * @param sparkSession to be used to connect to HDFS.
    * @param fromFilePath is the path URI of source file.
    * @param toFilePath is the path URI of target directory.
    * @param deleteSourceFile will be set to true to delete source file else will be false.
    * @return a boolean representing the status of task completion.
    */
  def moveFileWithinHDFS(sparkSession:SparkSession,fromFilePath: String, toFilePath: String, deleteSourceFile: Boolean): Boolean = {

    val conf = sparkSession.sparkContext.hadoopConfiguration
    val srcPath = new Path(fromFilePath)
    val srcFs = srcPath.getFileSystem(conf)
    val dstPath = new Path(toFilePath)
    val dstFs = dstPath.getFileSystem(conf)
    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, deleteSourceFile, conf)
  }

  /**
    * Delete a file in HDFS.
    * @param sparkSession to be used to connect to HDFS.
    * @param filePath the path to delete.
    * @return a boolean representing the status of task completion.
    */
  def deleteFileInHDFS(sparkSession: SparkSession,filePath: String): Boolean = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val deletePath = new Path(filePath)
    val deleteFs = deletePath.getFileSystem(conf)
    deleteFs.delete(deletePath, false)
  }

  /**
    * Check a file exists in HDFS or not.
    * @param sparkSession to be used to connect to HDFS.
    * @param filePath file URI to be checked.
    * @return true if file exists else false.
    */
  def isHDFSFileExists(sparkSession: SparkSession,filePath: String): Boolean = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = new Path(filePath)
    fs.isFile(path)
  }


  /**
    * Delete all files present in a directory in HDFS based on modification days elapsed.
    * @param sparkSession to be used to connect to HDFS.
    * @param directoryPath to be cleaned up.
    * @param olderThanDays no of days used to compare modification days passed and older files.
    * @param referenceDate Timestamp to be used as latest date to compare. In most of the cases it will be current timestamp.
    * @return a boolean representing the status of task completion.
    */
  def deleteOldFiles(sparkSession: SparkSession, directoryPath: String, olderThanDays:Int, referenceDate:Timestamp): Boolean = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val dirPath = new Path(directoryPath)

    if(fs.isDirectory(dirPath)) {
      val cal = Calendar.getInstance
      cal.setTime(referenceDate)
      cal.add(Calendar.DAY_OF_WEEK, -olderThanDays)
      val backDate = new Timestamp(cal.getTime.getTime)
      println("Removing files older than :" + backDate.toString)
      val status = fs.listStatus(dirPath)
      status.map(x => {
        if (x.isFile && backDate.after(new Timestamp(fs.getFileStatus(x.getPath).getModificationTime))) {
          try {
            fs.delete(x.getPath, false)
          }
          catch
            {
              case e: Exception => e.printStackTrace()
            }
        }
      })
      true
    }
    else
      throw new ParentNotDirectoryException("parent is not a dir")
  }
}

