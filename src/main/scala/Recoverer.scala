package org.apache.spark

object Recoverer {
  import scala.reflect.ClassTag
  import org.apache.spark.rdd.RDD
  def recover[T: ClassTag](sc: SparkContext, path: String): RDD[T] = {
    sc.checkpointFile[T](path)
  }
}
