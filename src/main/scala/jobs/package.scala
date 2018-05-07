package com.virtuslab.scoozie

package object jobs {
    type ArgList = List[(String, String)]

    def verifySuccessPaths(paths: List[String]): List[String] = {
        val checkedPaths = paths map (currString => {
            val headString = {
                if (!currString.startsWith("${nameNode}")) {
                    val newStr = {
                        if (currString.startsWith("/"))
                            "${nameNode}"
                        else
                            "${nameNode}/"
                    }
                    newStr
                } else ""
            }
            val tailString = {
                if (!currString.endsWith("_SUCCESS")) {
                    val newStr = {
                        if (currString.last != '/')
                            "/_SUCCESS"
                        else
                            "_SUCCESS"
                    }
                    newStr
                } else ""
            }
            headString + currString + tailString
        })
        checkedPaths
    }
}
