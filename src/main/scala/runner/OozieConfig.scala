package org.virtuslab.scoozie
package runner

case class OozieConfig(oozieUrl: String, properties: Map[String, String])