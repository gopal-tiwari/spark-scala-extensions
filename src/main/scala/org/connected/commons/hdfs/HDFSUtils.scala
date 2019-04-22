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

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession

object HDFSUtils {

  def moveFileWithinHDFS(sparkSession:SparkSession,fromFilePath: String, toFilePath: String, deleteSourceFile: Boolean): Boolean = {

    val conf = sparkSession.sparkContext.hadoopConfiguration
    val srcPath = new Path(fromFilePath);
    val srcFs = srcPath.getFileSystem(conf);
    val dstPath = new Path(toFilePath);
    val dstFs = dstPath.getFileSystem(conf);
    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, deleteSourceFile, conf);

  }

}

