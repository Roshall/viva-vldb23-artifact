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
似乎是说这里存在spark配置的缺陷，亟待优化<br> <br>
# 从/data/中获取视频文件创建spark row
在/viva/core/utils.py/ingest(custom_path = None)中
```python
    else:
        videos = [config.get_value('storage', 'input')]
        data = WalkRows(videos, ['mp4']).custom_op(None)
```
videos为列表，也就是说可以指定多个文件夹。此处默认只有/data/<br>
WalkRows定义于viva/nodes/data_nodes.py中，是一个生成spark row的工具类<br>
其唯一的方法custom_op(self, df)<br>
将/data/下所有mp4文件的绝对路径从1开始编号放在Row中构成列表返回给data变量
``` python
# can only be called in createdataframe
return [
           Row(str(os.path.abspath(os.path.join(pth, name))), next(idx))
           for path in paths
           for pth, subdirs, files in os.walk(path)
           for name in sorted(files) if name.endswith(tuple(extensions))
       ]
```
执行初始化的时候给它打印到日志输出给用户：
`logging.warn(f'Ingest->{data}')`<br> <br>
# ffmpeg配置
作者使用@pandas_udf装饰器定义了用户自定义函数给视频分块，以免不同视频帧率不一致<br>在viva/udfs/ingest.py中配置ffmpeg的输出参数时：
```python
def chunk(uri: pd.Series, segment_time_s: pd.Series, outdir: pd.Series) -> pd.DataFrame:
"""
    splits an input uri into equally sized videos of segment_time length.
    no encoding or additional processing happens so this is fast
    """

    #TODO if we want chunks larger than 60s segment_time arg will change
    outuris = []
    for idx, (u, s, o) in enumerate(zip(uri, segment_time_s, outdir)):
        if not os.path.exists(os.path.abspath(o)):  # 每个视频的分块都放在新的一个文件夹下
            os.makedirs(os.path.abspath(o))

        output_args = {
                'map': '0',
                'c': 'copy',
                'f': 'segment',
                'segment_time': f'00:00:{s:02}',
                'reset_timestamps': '1' #TODO not sure what this does 
                # 👆默认值为'0'，应该不影响性能，好像是用来强制视频分块从第0秒开始
            }

        all_outuris = glob(os.path.abspath(os.path.join(o, outname_base + '*mp4')))  # string列表包含1个视频的所有分块文件的绝对路径
```
发现作者的方法不支持大于60s的分块；这部分或可并行化。

## important
jdk的版本不能太高（如17），否则java报错，目前java11运行正常