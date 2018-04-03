package com.klout.scoozie
package verification

import com.klout.scoozie.conversion.GraphNode

case class VerificationNode(graphNode: GraphNode, parentThreads: Seq[ForkThread])