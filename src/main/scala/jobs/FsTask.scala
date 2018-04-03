package com.klout.scoozie
package jobs

sealed trait FsTask
case class MkDir(path: String) extends FsTask
case class Mv(from: String, to: String) extends FsTask
case class Rm(path: String) extends FsTask
case class Touchz(path: String) extends FsTask
case class ChMod(path: String, permissions: String, dirFiles: String) extends FsTask
