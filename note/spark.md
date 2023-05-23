# spark配置
from pyspark.conf import SparkConf 导入spark官方的配置工具<br>
在/viva/utils/config.py/viva_setup()中
```python
def viva_setup():
    config = ConfigManager()  # 从/conf.yml中读取键值对的函数
    spark_conf = SparkConf()
    pyspark_config = config.get_value('spark', 'property')
    for key, value in pyspark_config.items():  # 配置pyspark 
        spark_conf.set(key, value)
```
## 意外发现：
```python
    if use_gpu:
        # TODO(JAH): this changes config for GPU to a single executor. the
        # proper way we should implement is to set the right spark configs to
        # do this.
        spark_conf.set('spark.master', 'local[1]')
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
```
似乎是说这里存在spark配置的缺陷，亟待优化