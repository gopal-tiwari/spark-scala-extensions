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

package org.connected.commons.scala

import org.joda.time.format.DateTimeFormat

/**
  * Contains re usable extensions / implicits to be used in other modules.
  */
object ScalaUtils {

  implicit class CheckInt(val x: String) extends AnyVal {
    /**
      * Used to check if a string is a valid number grater than 0.
      *
      * @return true if string is a valid number else false.
      */
    def isPositiveNumber: Boolean = checkPositiveNumber(x)
  }

  def checkPositiveNumber(x: String): Boolean = if ((x.trim.length > 0) && (x.trim forall Character.isDigit) && (x.trim.toLong > 0)) true else false

  implicit class CheckIntArr(val x: Array[String]) extends AnyVal {
    /**
      * Used to check if all elements in a string array are representing a valid number grater than 0.
      *
      * @return true if string is a valid number else false.
      */
    def isPositiveNumber: Boolean = x forall checkPositiveNumber
  }

  implicit class CheckDate(val dateStr: String) {
    /**
      * To check whether a given string is a valid date with given pattern
      *
      * @param datePattern date pattern to be matched with.
      * @return Either as Left with Exception or Right with DateTime object.
      */
    def parseDate(datePattern: String) = {
      tryParseDate(dateStr, datePattern)
    }
  }

  private def tryParseDate(inputDateStr: String, datePattern: String) = try {
    val fmt = DateTimeFormat forPattern datePattern
    Right(fmt parseDateTime inputDateStr)
  } catch {
    case e: org.joda.time.IllegalFieldValueException => Left(e)
    case e: IllegalArgumentException => Left(e)
  }
}
