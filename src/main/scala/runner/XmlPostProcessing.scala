package org.virtuslab.scoozie
package runner

case class XmlPostProcessing(
    substitutions: Map[String, String])

object XmlPostProcessing {
    val Default = XmlPostProcessing(
        substitutions = Map(
            "&quot;" -> "\""))
}

