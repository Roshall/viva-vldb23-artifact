clean_spark() {
  find /tmp/spark* -mindepth 1 ! -regex '^/tmp/spark-events\(/.*\)?' -delete
}

new_conf() {
  # 1 proxy_thresh, 2 script_dir, 3 query_name, 4 GPU num
  if [ ! -f "conf.yml.orig" ]; then
    echo "Copying conf.yml to conf.yml.orig"
    mv conf.yml conf.yml.orig
  fi
  sed "s|<PROXY_THRESH>|${1}|g" "${2}"/conf.yml.templ |\
  sed "s|input:.*|input: 'dataset/${3}/data/'|" |\
  sed "s|output:.*|output: 'output${4}/'|" |\
  sed "s|tmp:.*|tmp: 'tmp${4}/'|" > conf.yml
}