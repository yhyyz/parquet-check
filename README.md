##  parquet-check

### params
```
ParquetCCTools 1.0
Usage: spark parquet ParquetCCTools [options]

-e, --env <value>        env: dev or prod  # dev local模式执行
-i, --inventry_s3_csv_path <value>
inventry_s3_csv_path: inventry file  # s3 的inventry文件路径，需要csv格式，至少需要三个字段，bucket,key,size
-s, --smaple_data_output_path <value>
smaple_data_output_path: save sample data dir # 会将需要计算的采样数据生产，存储到目录中
-t, --tmp_data_dir <value>
tmp_data_dir: tmp data dir  # 计算过程需要的临时目录
-r, --result_dir <value>
result_dir: save result  # 最终的结果数据
-n, --thead_num <value>  thead_num: default 50  # 执行任务的线程数，默认50
-m, --sample_file_min <value>  sample_file_min: sample_file_min: default 100000000 bytes  # 采样时文件的最小值，会根据最小值和最大值筛选文件，并且选择最小值和最大值之间的最大文件作为采样文件
-n, --sample_file_max <value>  sample_file_max: sample_file_max: default 150000000 bytes  # 采样时文件的最大值，，会根据最小值和最大值筛选文件，并且选择最小值和最大值之间的最大文件作为采样文件
```

#### job example
```
spark-submit  --master yarn \
--deploy-mode client \
--driver-memory 4g \
--class poc.ParquetCCTools /home/hadoop/parquet-check-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e prod \
-i s3a://***/tmp/inv_test.csv \
-s s3a://***/tmp/sample_data_output/ \
-t s3a://***/tmp/sample_tmp_data/ \
-r s3a://***/tmp/result/ \
-n 100000000 \
-m 150000000 \
```
