package com.virtuslab.scoozie
package verification

import conversion.GraphNode

case class VerificationNode(graphNode: GraphNode, parentThreads: Seq[ForkThread])