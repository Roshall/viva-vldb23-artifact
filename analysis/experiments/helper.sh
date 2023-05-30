clean_spark() {
  find /tmp/spark* -mindepth 1 ! -regex '^/tmp/spark-events\(/.*\)?' -delete
}