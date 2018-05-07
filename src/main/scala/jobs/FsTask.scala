package org.virtuslab.scoozie
package jobs

sealed trait FsTask
case class MkDir(path: String) extends FsTask
case class Mv(from: String, to: String) extends FsTask
case class Rm(path: String) extends FsTask
case class Touchz(path: String) extends FsTask
case class ChMod(recursive: Boolean, path: String, permissions: String, dirFiles: String) extends FsTask
case class ChGrp(recursive: Boolean, path: String, group: String, dirFiles: String) extends FsTask
