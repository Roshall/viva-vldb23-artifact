clean_spark() {
  find /tmp/spark* -mindepth 1 ! -regex '^/tmp/spark-events\(/.*\)?' -delete
}

new_conf() {
  # 1 proxy_thresh, 2 script_dir, 3 query_name, 4 GPU num
  if [ ! -f "conf.yml.orig" ]; then
    echo "Copying conf.yml to conf.yml.orig"
    mv conf.yml conf.yml.orig
  fi
  sed "0,/<PROXY_THRESH>/s||${1}|" "${2}"/conf.yml.templ |\
  sed "0,/<INPUT_VIDEO>/s||dataset/${3}/data/|" |\
  sed "0,/<OUTPUT_VIDEO>/s||output${4}/|" |\
  sed "0,/<TMP>/s||tmp${4}/|" > conf.yml
}