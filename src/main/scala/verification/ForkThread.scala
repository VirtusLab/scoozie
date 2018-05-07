package org.virtuslab.scoozie
package verification

import org.virtuslab.scoozie.conversion.GraphNode

//path is the child of the fork that this thread lies on
case class ForkThread(fork: GraphNode, path: GraphNode)
