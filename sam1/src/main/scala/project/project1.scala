package project

import java.util.logging.Logger

object project1 extends  Serializable {

  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

}
