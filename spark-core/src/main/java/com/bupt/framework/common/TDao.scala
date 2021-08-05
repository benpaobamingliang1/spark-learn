package com.bupt.framework.common

import com.bupt.framework.utils.EnvUtil


trait TDao {

    def readFile(path: String) = {
        EnvUtil.take().textFile(path)
    }
}
