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

package org.connected.commons.strings

object StringHelpers
{
  /**
    * If str is surrounded by quotes it return the content between the quotes
    *
    * @param str String needs to be unquoted.
    * @return it return the content between the quotes.
    */
  def unquote(str: String): String =
  {
    if (str != null && str.length >= 2 && str.charAt(0) == '\"' && str.charAt(str.length - 1) == '\"')
      str.substring(1, str.length - 1)
    else
      str
  }

  /**
    * Turn a string of format "FooBar" into snake case "foo_bar"
    *
    * Note: snakify is not reversible, ie. in general the following will _not_ be true:
    *
    * s == camelify(snakify(s))
    *
    * @return the underscored string
    */
  def snakify(name: String) = name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase

  /**
    * Turns a string of format "foo_bar" into camel case "FooBar"
    *
    * Functional code courtesy of Jamie Webb (j@jmawebb.cjb.net) 2006/11/28
    * @param name the String to CamelCase
    *
    * @return the CamelCased string
    */
  def camelify(name : String): String = {
    def loop(x : List[Char]): List[Char] = (x: @unchecked) match {
      case '_' :: '_' :: rest => loop('_' :: rest)
      case '_' :: c :: rest => Character.toUpperCase(c) :: loop(rest)
      case '_' :: Nil => Nil
      case c :: rest => c :: loop(rest)
      case Nil => Nil
    }
    if (name == null)
      ""
    else
      loop('_' :: name.toList).mkString
  }

  /**
    * Capitalize every "word" in the string. A word is either separated by spaces or underscores.
    * @param in string to capify
    * @return the capified string
    */
  def capify(in: String): String = {
    val tmp = ((in match {
      case null => ""
      case s => s
    }).trim match {
      case "" => "n/a"
      case s => s
    }).toLowerCase
    val sb = new StringBuilder
    capify(tmp, 0, 250, false, false, sb)
    sb.toString
  }

  /**
    * Replaces the groups found in the msg string with a replacement according to the value found in the subst Map
    * @param msg string where replacements should be done
    * @param subst map of [regular expression with groups, replacement]
    */
  private def capify(in: String, pos: Int, max: Int, lastLetter: Boolean, lastSymbol: Boolean, out: StringBuilder ): Unit = {
    if (pos >= max || pos >= in.length) return
    else {
      in.charAt(pos) match {
        case c if Character.isDigit(c) => out.append(c); capify(in, pos + 1, max, false, false, out)
        case c if Character.isLetter(c) => out.append(if (lastLetter) c else Character.toUpperCase(c)) ; capify(in, pos + 1, max, true, false, out)
        case c if (c == ' ' || c == '_') && !lastSymbol => out.append(c) ; capify(in, pos + 1, max, false, true, out)
        case _ => capify(in, pos + 1, max, false, true, out)
      }
    }
  }

}
