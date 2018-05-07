package org.virtuslab.scoozie
package dsl

import scala.util.matching.Regex

sealed trait Predicate

object Predicates {
    case object AlwaysTrue extends Predicate

    case class BooleanProperty(property: String) extends Predicate {
        val BooleanPropertyRegex: Regex = """\$\{(.*)\}"""r

        lazy val formattedProperty: String = property match {
            case BooleanPropertyRegex(_) => property
            case _                       => """${%s}""" format property
        }
    }
}
