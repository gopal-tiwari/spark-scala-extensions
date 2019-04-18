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
    def isPositiveNumber:Boolean = x forall checkPositiveNumber
  }
}
